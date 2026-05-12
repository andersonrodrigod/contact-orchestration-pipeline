from core.logger import PipelineLogger
from pathlib import Path
from core.error_codes import (
    ERRO_INGESTAO,
    ERRO_QUALIDADE_DATA,
    ERRO_VALIDACAO_COLUNAS,
)
from src.config.governanca_config import (
    resolver_janela_corte_alias_resposta,
    resolver_limiar_nat_data,
    resolver_modo_estrito_alias_resposta,
)
from src.services.normalizacao_services import (
    criar_coluna_dt_envio_por_data_agendamento,
    formatar_coluna_data_br,
    limpar_texto_colunas_alvo,
    normalizar_tipos_dataframe,
)
from src.services.padronizacao_service import (
    padronizar_colunas_status,
    padronizar_colunas_status_resposta,
)
from src.services.schema_resposta_service import garantir_contrato_resposta_canonica
from src.services.validacao_service import (
    validar_colunas_origem_para_padronizacao,
    validar_padronizacao_colunas_data,
)
from src.utils.arquivos import ler_arquivo_csv, salvar_dataframe

def _caminho_xlsx_pareado(caminho_arquivo):
    caminho = Path(caminho_arquivo)
    if caminho.suffix.lower() in ['.xlsx', '.xls']:
        return str(caminho)
    return str(caminho.with_suffix('.xlsx'))


def _preferir_xlsx_se_existir(caminho_arquivo):
    caminho_xlsx = _caminho_xlsx_pareado(caminho_arquivo)
    if Path(caminho_xlsx).exists():
        return caminho_xlsx, True
    return caminho_arquivo, False


def _obter_saida_status_xlsx(saida_status):
    return str(Path(saida_status).with_suffix('.xlsx'))


def _obter_saida_status_resposta_xlsx(saida_status_resposta):
    return str(Path(saida_status_resposta).with_suffix('.xlsx'))


def _mensagem_alerta_nat(coluna, percentual, quantidade, total):
    return (
        f'Alerta de qualidade: coluna {coluna} com NaT em {percentual:.2f}% '
        f'({quantidade}/{total}).'
    )


COLUNAS_TEXTO_ALVO_STATUS = ['HSM', 'Status', 'Respondido', 'RESPOSTA']
COLUNAS_TEXTO_ALVO_STATUS_RESPOSTA = ['HSM', 'Status', 'Respondido', 'resposta']


def _inicializar_estado_normalizacao():
    return {
        'alertas_data': [],
        'erros_qualidade_data': [],
        'nat_status': 0,
        'pct_nat_status': 0.0,
        'nat_resposta': 0,
        'pct_nat_resposta': 0.0,
    }


def _ler_arquivos_status(logger, arquivo_status, arquivo_status_resposta):
    logger.info('LEITURA', 'Lendo arquivo status')
    df_status = ler_arquivo_csv(arquivo_status)
    logger.info('LEITURA', f'df_status: linhas={len(df_status)} colunas={len(df_status.columns)}')

    logger.info('LEITURA', 'Lendo arquivo status_resposta')
    df_status_resposta = ler_arquivo_csv(arquivo_status_resposta)
    logger.info(
        'LEITURA',
        f'df_status_resposta: linhas={len(df_status_resposta)} colunas={len(df_status_resposta.columns)}',
    )
    return df_status, df_status_resposta


def _validar_colunas_origem_normalizacao(
    logger,
    df_status,
    df_status_resposta,
    modo_estrito_alias_resposta,
    janela_corte_alias_resposta_ciclos,
):
    resultado_colunas_origem = validar_colunas_origem_para_padronizacao(
        df_status,
        df_status_resposta,
        modo_estrito_alias_resposta=modo_estrito_alias_resposta,
        janela_corte_alias_resposta_ciclos=janela_corte_alias_resposta_ciclos,
    )
    logger.info(
        'VALIDACAO_ORIGEM',
        f"ok={resultado_colunas_origem['ok']} mensagens={resultado_colunas_origem['mensagens']}",
    )
    return resultado_colunas_origem


def _aplicar_padronizacao_status_e_resposta(df_status, df_status_resposta):
    df_status = padronizar_colunas_status(df_status)
    df_status_resposta = padronizar_colunas_status_resposta(df_status_resposta)
    garantir_contrato_resposta_canonica(
        df_status_resposta,
        contexto='ingestao.status_resposta_pos_padronizacao',
    )
    return df_status, df_status_resposta


def _normalizar_tipos_e_coletar_qualidade_data(
    logger,
    df_status,
    df_status_resposta,
    limiar_nat_data,
    estado,
):
    logger.info('NORMALIZACAO', 'Convertendo tipos de colunas')
    df_status = normalizar_tipos_dataframe(df_status, colunas_data=['Data agendamento'])
    df_status_resposta = normalizar_tipos_dataframe(
        df_status_resposta, colunas_data=['DT_ATENDIMENTO']
    )
    logger.info(
        'NORMALIZACAO',
        f"Data agendamento dtype={df_status['Data agendamento'].dtype if 'Data agendamento' in df_status.columns else 'NA'}",
    )
    logger.info(
        'NORMALIZACAO',
        f"DT_ATENDIMENTO dtype={df_status_resposta['DT_ATENDIMENTO'].dtype if 'DT_ATENDIMENTO' in df_status_resposta.columns else 'NA'}",
    )
    if 'Data agendamento' in df_status.columns and len(df_status) > 0:
        estado['nat_status'] = int(df_status['Data agendamento'].isna().sum())
        estado['pct_nat_status'] = (estado['nat_status'] / len(df_status)) * 100
        logger.info(
            'NORMALIZACAO',
            f"Data agendamento NaT={estado['pct_nat_status']:.2f}% ({estado['nat_status']}/{len(df_status)})",
        )
        if estado['pct_nat_status'] >= limiar_nat_data:
            mensagem_alerta = _mensagem_alerta_nat(
                'Data agendamento', estado['pct_nat_status'], estado['nat_status'], len(df_status)
            )
            logger.error('VALIDACAO_DATA', mensagem_alerta)
            estado['erros_qualidade_data'].append(mensagem_alerta)
    if 'DT_ATENDIMENTO' in df_status_resposta.columns and len(df_status_resposta) > 0:
        estado['nat_resposta'] = int(df_status_resposta['DT_ATENDIMENTO'].isna().sum())
        estado['pct_nat_resposta'] = (estado['nat_resposta'] / len(df_status_resposta)) * 100
        logger.info(
            'NORMALIZACAO',
            f"DT_ATENDIMENTO NaT={estado['pct_nat_resposta']:.2f}% ({estado['nat_resposta']}/{len(df_status_resposta)})",
        )
        if estado['pct_nat_resposta'] >= limiar_nat_data:
            mensagem_alerta = _mensagem_alerta_nat(
                'DT_ATENDIMENTO', estado['pct_nat_resposta'], estado['nat_resposta'], len(df_status_resposta)
            )
            logger.error('VALIDACAO_DATA', mensagem_alerta)
            estado['erros_qualidade_data'].append(mensagem_alerta)
    return df_status, df_status_resposta


def _montar_resultado_normalizacao(
    mensagens_iniciais,
    resultado_colunas_origem,
    resultado_validacao,
    estado,
    limiar_nat_data,
    modo_estrito_alias_resposta,
    janela_corte_alias_resposta_ciclos,
):
    return {
        'ok': (
            resultado_colunas_origem['ok']
            and resultado_validacao['ok']
            and len(estado['erros_qualidade_data']) == 0
        ),
        'mensagens': (
            mensagens_iniciais
            + resultado_colunas_origem['mensagens']
            + resultado_validacao['mensagens']
            + estado['alertas_data']
            + estado['erros_qualidade_data']
        ),
        'nat_data_agendamento': estado['nat_status'],
        'pct_nat_data_agendamento': round(estado['pct_nat_status'], 2),
        'nat_dt_atendimento': estado['nat_resposta'],
        'pct_nat_dt_atendimento': round(estado['pct_nat_resposta'], 2),
        'limiar_nat_data_em_uso': limiar_nat_data,
        'warnings_alias_resposta_legado': resultado_colunas_origem.get(
            'warnings_alias_resposta_legado', 0
        ),
        'modo_estrito_alias_resposta': bool(modo_estrito_alias_resposta),
        'janela_corte_alias_resposta_ciclos': int(janela_corte_alias_resposta_ciclos),
        'qualidade_data': {
            'data_agendamento': {
                'nat': estado['nat_status'],
                'pct_nat': round(estado['pct_nat_status'], 2),
                'limiar': limiar_nat_data,
            },
            'dt_atendimento': {
                'nat': estado['nat_resposta'],
                'pct_nat': round(estado['pct_nat_resposta'], 2),
                'limiar': limiar_nat_data,
            },
        },
        'metricas_por_etapa': {
            'normalizacao_padronizacao': {
                'nat_data_agendamento': estado['nat_status'],
                'pct_nat_data_agendamento': round(estado['pct_nat_status'], 2),
                'nat_dt_atendimento': estado['nat_resposta'],
                'pct_nat_dt_atendimento': round(estado['pct_nat_resposta'], 2),
                'limiar_nat_data_em_uso': limiar_nat_data,
                'warnings_alias_resposta_legado': resultado_colunas_origem.get(
                    'warnings_alias_resposta_legado', 0
                ),
                'modo_estrito_alias_resposta': bool(modo_estrito_alias_resposta),
                'janela_corte_alias_resposta_ciclos': int(
                    janela_corte_alias_resposta_ciclos
                ),
            }
        },
    }


def executar_normalizacao_padronizacao(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta='src/data/status_resposta_complicacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    limiar_nat_data=None,
    contexto=None,
    permitir_override_limiar=True,
    modo_estrito_alias_resposta=None,
    janela_corte_alias_resposta_ciclos=None,
    mensagens_iniciais=None,
    logger=None,
    finalizar_logger=True,
):
    if mensagens_iniciais is None:
        mensagens_iniciais = []
    if logger is None:
        logger = PipelineLogger()
    limiar_nat_data, origem_limiar = resolver_limiar_nat_data(
        limiar_nat_data,
        contexto=contexto,
        permitir_override_limiar=permitir_override_limiar,
    )
    modo_estrito_alias_resposta, origem_modo_estrito = resolver_modo_estrito_alias_resposta(
        modo_estrito_alias_resposta
    )
    janela_corte_alias_resposta_ciclos, origem_janela_corte = resolver_janela_corte_alias_resposta(
        janela_corte_alias_resposta_ciclos
    )
    logger.info('INICIO', f'arquivo_status={arquivo_status}')
    logger.info('INICIO', f'arquivo_status_resposta={arquivo_status_resposta}')
    logger.info('INICIO', f'saida_status={saida_status}')
    logger.info('INICIO', f'saida_status_resposta={saida_status_resposta}')
    logger.info('INICIO', f'limiar_nat_data_em_uso={limiar_nat_data}')
    logger.info('INICIO', f'limiar_nat_data_origem={origem_limiar}')
    logger.info('INICIO', f'modo_estrito_alias_resposta={modo_estrito_alias_resposta}')
    logger.info('INICIO', f'modo_estrito_alias_resposta_origem={origem_modo_estrito}')
    logger.info(
        'INICIO',
        f'janela_corte_alias_resposta_ciclos={janela_corte_alias_resposta_ciclos}',
    )
    logger.info('INICIO', f'janela_corte_alias_resposta_origem={origem_janela_corte}')

    etapa_atual = 'INICIO'
    try:
        estado = _inicializar_estado_normalizacao()
        etapa_atual = 'LEITURA_STATUS'
        df_status, df_status_resposta = _ler_arquivos_status(
            logger, arquivo_status, arquivo_status_resposta
        )

        resultado_colunas_origem = _validar_colunas_origem_normalizacao(
            logger,
            df_status,
            df_status_resposta,
            modo_estrito_alias_resposta,
            janela_corte_alias_resposta_ciclos,
        )
        if not resultado_colunas_origem['ok']:
            resultado_final = {
                'ok': False,
                'mensagens': mensagens_iniciais + resultado_colunas_origem['mensagens'],
                'codigo_erro': ERRO_VALIDACAO_COLUNAS,
            }
            logger.warning('VALIDACAO_ORIGEM', 'Falhou validacao de colunas de origem')
            if finalizar_logger:
                logger.finalizar('FALHA_VALIDACAO_ORIGEM')
            return resultado_final

        etapa_atual = 'PADRONIZACAO'
        logger.info('PADRONIZACAO', 'Padronizando nomes de colunas')
        df_status, df_status_resposta = _aplicar_padronizacao_status_e_resposta(
            df_status, df_status_resposta
        )

        etapa_atual = 'NORMALIZACAO_TIPOS'
        df_status, df_status_resposta = _normalizar_tipos_e_coletar_qualidade_data(
            logger,
            df_status,
            df_status_resposta,
            limiar_nat_data,
            estado,
        )

        etapa_atual = 'LIMPEZA_TEXTO'
        logger.info(
            'NORMALIZACAO',
            f'Limpando texto apenas nas colunas alvo status={COLUNAS_TEXTO_ALVO_STATUS} '
            f'status_resposta={COLUNAS_TEXTO_ALVO_STATUS_RESPOSTA}',
        )
        df_status = limpar_texto_colunas_alvo(
            df_status,
            colunas_alvo=COLUNAS_TEXTO_ALVO_STATUS,
        )
        df_status_resposta = limpar_texto_colunas_alvo(
            df_status_resposta,
            colunas_alvo=COLUNAS_TEXTO_ALVO_STATUS_RESPOSTA,
        )

        etapa_atual = 'CRIAR_DT_ENVIO'
        logger.info('FORMATACAO', 'Criando coluna DT ENVIO a partir de Data agendamento (sem hora)')
        criar_coluna_dt_envio_por_data_agendamento(df_status)
        logger.info(
            'FORMATACAO',
            f"Exemplo DT ENVIO={df_status['DT ENVIO'].head(1).tolist() if 'DT ENVIO' in df_status.columns else []}",
        )

        etapa_atual = 'VALIDACAO_DATA'
        resultado_validacao = validar_padronizacao_colunas_data(df_status, df_status_resposta)
        logger.info(
            'VALIDACAO_DATA',
            f"ok={resultado_validacao['ok']} mensagens={resultado_validacao['mensagens']}",
        )
        resultado_final = _montar_resultado_normalizacao(
            mensagens_iniciais=mensagens_iniciais,
            resultado_colunas_origem=resultado_colunas_origem,
            resultado_validacao=resultado_validacao,
            estado=estado,
            limiar_nat_data=limiar_nat_data,
            modo_estrito_alias_resposta=modo_estrito_alias_resposta,
            janela_corte_alias_resposta_ciclos=janela_corte_alias_resposta_ciclos,
        )
        if not resultado_final['ok']:
            if len(estado['erros_qualidade_data']) > 0:
                resultado_final['codigo_erro'] = ERRO_QUALIDADE_DATA
                status_final = 'FALHA_QUALIDADE_DATA'
            else:
                resultado_final['codigo_erro'] = ERRO_VALIDACAO_COLUNAS
                status_final = 'FALHA_VALIDACAO_DATA'

            motivo_principal = (
                resultado_final['mensagens'][-1] if len(resultado_final['mensagens']) > 0 else 'Motivo nao informado.'
            )
            mensagem_bloqueio = (
                f"Saida bloqueada na etapa={etapa_atual}: arquivos nao foram salvos. "
                f"codigo_erro={resultado_final['codigo_erro']}. Motivo: {motivo_principal}"
            )
            logger.warning('BLOQUEIO_SAIDA', mensagem_bloqueio)
            resultado_final['mensagens'] = resultado_final['mensagens'] + [mensagem_bloqueio]
            if finalizar_logger:
                logger.finalizar(status_final)
            return resultado_final

        etapa_atual = 'FORMATAR_DT_ATENDIMENTO'
        logger.info('FORMATACAO', 'Formatando DT_ATENDIMENTO para BR')
        formatar_coluna_data_br(df_status_resposta, 'DT_ATENDIMENTO')
        logger.info(
            'FORMATACAO',
            f"Exemplo DT_ATENDIMENTO={df_status_resposta['DT_ATENDIMENTO'].head(1).tolist() if 'DT_ATENDIMENTO' in df_status_resposta.columns else []}",
        )

        etapa_atual = 'SALVAR_ARQUIVOS'
        logger.info('SAIDA', f'Salvando status em {saida_status}')
        salvar_dataframe(df_status, saida_status)
        logger.info('SAIDA', f'Salvando status_resposta em {saida_status_resposta}')
        salvar_dataframe(df_status_resposta, saida_status_resposta)

        if finalizar_logger:
            logger.finalizar('SUCESSO')
        return resultado_final
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        resultado_erro = {
            'ok': False,
            'mensagens': [f'Erro inesperado na execucao (etapa={etapa_atual}): {type(erro).__name__}: {erro}'],
            'codigo_erro': ERRO_INGESTAO,
        }
        if finalizar_logger:
            logger.finalizar('ERRO')
        return resultado_erro


def executar_ingestao_complicacao(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_complicacao='src/data/status_resposta_complicacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    limiar_nat_data=None,
    permitir_override_limiar=True,
    modo_estrito_alias_resposta=None,
    janela_corte_alias_resposta_ciclos=None,
    executar_xlsx_adicional=False,
    logger=None,
):
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(nome_pipeline='ingestao_complicacao')
    logger.info('MODO', 'Modo complicacao iniciado')
    resultado = executar_normalizacao_padronizacao(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        limiar_nat_data=limiar_nat_data,
        contexto='complicacao',
        permitir_override_limiar=permitir_override_limiar,
        modo_estrito_alias_resposta=modo_estrito_alias_resposta,
        janela_corte_alias_resposta_ciclos=janela_corte_alias_resposta_ciclos,
        mensagens_iniciais=['Modo complicacao selecionado.'],
        logger=logger,
        finalizar_logger=not logger_externo,
    )
    if not resultado.get('ok'):
        return resultado

    if not executar_xlsx_adicional:
        logger.info('MODO_XLSX', 'Execucao adicional XLSX desabilitada para este modo.')
        return resultado

    arquivo_status_xlsx, tem_status_xlsx = _preferir_xlsx_se_existir(arquivo_status)
    arquivo_resposta_xlsx, tem_resposta_xlsx = _preferir_xlsx_se_existir(
        arquivo_status_resposta_complicacao
    )
    if not (tem_status_xlsx or tem_resposta_xlsx):
        logger.info('MODO_XLSX', 'Nenhum XLSX de entrada encontrado para execucao adicional.')
        return resultado

    saida_status_xlsx = _obter_saida_status_xlsx(saida_status)
    saida_resposta_xlsx = _obter_saida_status_resposta_xlsx(saida_status_resposta)
    logger.info('MODO_XLSX', 'Execucao adicional XLSX iniciada para limpeza de status.')
    logger.info('MODO_XLSX', f'arquivo_status={arquivo_status_xlsx}')
    logger.info('MODO_XLSX', f'arquivo_status_resposta={arquivo_resposta_xlsx}')
    logger.info('MODO_XLSX', f'saida_status={saida_status_xlsx}')
    logger.info('MODO_XLSX', f'saida_status_resposta={saida_resposta_xlsx}')

    resultado_xlsx = executar_normalizacao_padronizacao(
        arquivo_status=arquivo_status_xlsx,
        arquivo_status_resposta=arquivo_resposta_xlsx,
        saida_status=saida_status_xlsx,
        saida_status_resposta=saida_resposta_xlsx,
        limiar_nat_data=limiar_nat_data,
        contexto='complicacao',
        permitir_override_limiar=permitir_override_limiar,
        modo_estrito_alias_resposta=modo_estrito_alias_resposta,
        janela_corte_alias_resposta_ciclos=janela_corte_alias_resposta_ciclos,
        mensagens_iniciais=['Execucao adicional XLSX (status + status_resposta).'],
        logger=logger,
        finalizar_logger=False,
    )
    if resultado_xlsx.get('ok'):
        logger.info('MODO_XLSX', 'Execucao adicional XLSX finalizada com sucesso.')
        resultado['mensagens'] = resultado.get('mensagens', []) + [
            f'Saida XLSX gerada: {saida_status_xlsx}',
            f'Saida XLSX gerada: {saida_resposta_xlsx}',
        ]
    else:
        logger.warning('MODO_XLSX', 'Execucao adicional XLSX falhou; fluxo CSV foi mantido.')
        resultado['mensagens'] = resultado.get('mensagens', []) + [
            'Aviso: falha na execucao adicional XLSX de limpeza de status.',
        ]
    return resultado


def executar_ingestao_somente_status(
    arquivo_status='src/data/status.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    nome_logger='ingestao_somente_status',
    limiar_nat_data=None,
    contexto='complicacao',
    permitir_override_limiar=True,
    logger=None,
):
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(nome_pipeline=nome_logger)
    limiar_nat_data, origem_limiar = resolver_limiar_nat_data(
        limiar_nat_data,
        contexto=contexto,
        permitir_override_limiar=permitir_override_limiar,
    )
    logger.info('INICIO', f'arquivo_status={arquivo_status}')
    logger.info('INICIO', f'saida_status={saida_status}')
    logger.info('INICIO', f'limiar_nat_data_em_uso={limiar_nat_data}')
    logger.info('INICIO', f'limiar_nat_data_origem={origem_limiar}')
    etapa_atual = 'INICIO'
    try:
        erros_qualidade_data = []
        nat_status = 0
        pct_nat_status = 0.0
        etapa_atual = 'LEITURA_STATUS'
        df_status = ler_arquivo_csv(arquivo_status)
        logger.info('LEITURA', f'df_status: linhas={len(df_status)} colunas={len(df_status.columns)}')

        etapa_atual = 'PADRONIZACAO'
        df_status = padronizar_colunas_status(df_status)
        etapa_atual = 'NORMALIZACAO_TIPOS'
        df_status = normalizar_tipos_dataframe(df_status, colunas_data=['Data agendamento'])
        if 'Data agendamento' in df_status.columns and len(df_status) > 0:
            nat_status = int(df_status['Data agendamento'].isna().sum())
            pct_nat_status = (nat_status / len(df_status)) * 100
            logger.info(
                'NORMALIZACAO',
                f'Data agendamento NaT={pct_nat_status:.2f}% ({nat_status}/{len(df_status)})',
            )
            if pct_nat_status >= limiar_nat_data:
                mensagem_alerta = _mensagem_alerta_nat(
                    'Data agendamento', pct_nat_status, nat_status, len(df_status)
                )
                logger.error('VALIDACAO_DATA', mensagem_alerta)
                erros_qualidade_data.append(mensagem_alerta)
        etapa_atual = 'LIMPEZA_TEXTO'
        df_status = limpar_texto_colunas_alvo(
            df_status,
            colunas_alvo=COLUNAS_TEXTO_ALVO_STATUS,
        )
        etapa_atual = 'CRIAR_DT_ENVIO'
        criar_coluna_dt_envio_por_data_agendamento(df_status)

        if len(erros_qualidade_data) > 0:
            mensagem_bloqueio = (
                f"Saida bloqueada na etapa={etapa_atual}: arquivo nao foi salvo. "
                f"codigo_erro={ERRO_QUALIDADE_DATA}. Motivo: {erros_qualidade_data[-1]}"
            )
            logger.warning('BLOQUEIO_SAIDA', mensagem_bloqueio)
            if not logger_externo:
                logger.finalizar('FALHA_QUALIDADE_DATA')
            return {
                'ok': False,
                'mensagens': erros_qualidade_data + [mensagem_bloqueio],
                'codigo_erro': ERRO_QUALIDADE_DATA,
                'nat_data_agendamento': nat_status,
                'pct_nat_data_agendamento': round(pct_nat_status, 2),
                'nat_dt_atendimento': 0,
                'pct_nat_dt_atendimento': 0.0,
                'limiar_nat_data_em_uso': limiar_nat_data,
                'qualidade_data': {
                    'data_agendamento': {
                        'nat': nat_status,
                        'pct_nat': round(pct_nat_status, 2),
                        'limiar': limiar_nat_data,
                    }
                },
            }

        etapa_atual = 'SALVAR_ARQUIVO'
        salvar_dataframe(df_status, saida_status)
        if not logger_externo:
            logger.finalizar('SUCESSO')
        return {
            'ok': True,
            'arquivo_saida': saida_status,
            'mensagens': ['Ingestao somente status executada com sucesso.'],
            'nat_data_agendamento': nat_status,
            'pct_nat_data_agendamento': round(pct_nat_status, 2),
            'nat_dt_atendimento': 0,
            'pct_nat_dt_atendimento': 0.0,
            'limiar_nat_data_em_uso': limiar_nat_data,
            'qualidade_data': {
                'data_agendamento': {
                    'nat': nat_status,
                    'pct_nat': round(pct_nat_status, 2),
                    'limiar': limiar_nat_data,
                }
            },
            'metricas_por_etapa': {
                'normalizacao_padronizacao': {
                    'nat_data_agendamento': nat_status,
                    'pct_nat_data_agendamento': round(pct_nat_status, 2),
                    'nat_dt_atendimento': 0,
                    'pct_nat_dt_atendimento': 0.0,
                    'limiar_nat_data_em_uso': limiar_nat_data,
                }
            },
        }
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        if not logger_externo:
            logger.finalizar('ERRO')
        return {
            'ok': False,
            'mensagens': [f'Erro na ingestao somente status (etapa={etapa_atual}): {type(erro).__name__}: {erro}'],
            'codigo_erro': ERRO_INGESTAO,
        }
