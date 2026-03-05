from core.logger import PipelineLogger
from pathlib import Path
from src.pipelines.concatenar_status_respostas_pipeline import run_unificar_status_respostas_pipeline
from src.services.normalizacao_services import (
    criar_coluna_dt_envio_por_data_agendamento,
    formatar_coluna_data_br,
    limpar_texto_exceto_colunas,
    normalizar_tipos_dataframe,
)
from src.services.padronizacao_service import (
    padronizar_colunas_status,
    padronizar_colunas_status_resposta,
)
from src.services.validacao_service import (
    validar_colunas_origem_para_padronizacao,
    validar_padronizacao_colunas_data,
)
from src.utils.arquivos import ler_arquivo_csv, salvar_dataframe

LIMIAR_AVISO_PERCENTUAL_NAT_DATA = 30.0


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


def executar_normalizacao_padronizacao(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta='src/data/status_resposta_complicacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    limiar_nat_data=LIMIAR_AVISO_PERCENTUAL_NAT_DATA,
    mensagens_iniciais=None,
    logger=None,
    finalizar_logger=True,
):
    if mensagens_iniciais is None:
        mensagens_iniciais = []
    if logger is None:
        logger = PipelineLogger()
    logger.info('INICIO', f'arquivo_status={arquivo_status}')
    logger.info('INICIO', f'arquivo_status_resposta={arquivo_status_resposta}')
    logger.info('INICIO', f'saida_status={saida_status}')
    logger.info('INICIO', f'saida_status_resposta={saida_status_resposta}')
    logger.info('INICIO', f'limiar_nat_data_em_uso={limiar_nat_data}')

    etapa_atual = 'INICIO'
    try:
        alertas_data = []
        erros_qualidade_data = []
        etapa_atual = 'LEITURA_STATUS'
        logger.info('LEITURA', 'Lendo arquivo status')
        df_status = ler_arquivo_csv(arquivo_status)
        logger.info('LEITURA', f'df_status: linhas={len(df_status)} colunas={len(df_status.columns)}')

        etapa_atual = 'LEITURA_STATUS_RESPOSTA'
        logger.info('LEITURA', 'Lendo arquivo status_resposta')
        df_status_resposta = ler_arquivo_csv(arquivo_status_resposta)
        logger.info(
            'LEITURA',
            f'df_status_resposta: linhas={len(df_status_resposta)} colunas={len(df_status_resposta.columns)}',
        )

        resultado_colunas_origem = validar_colunas_origem_para_padronizacao(
            df_status, df_status_resposta
        )
        logger.info(
            'VALIDACAO_ORIGEM',
            f"ok={resultado_colunas_origem['ok']} mensagens={resultado_colunas_origem['mensagens']}",
        )
        if not resultado_colunas_origem['ok']:
            resultado_final = {
                'ok': False,
                'mensagens': mensagens_iniciais + resultado_colunas_origem['mensagens'],
            }
            logger.warning('VALIDACAO_ORIGEM', 'Falhou validacao de colunas de origem')
            if finalizar_logger:
                logger.finalizar('FALHA_VALIDACAO_ORIGEM')
            return resultado_final

        etapa_atual = 'PADRONIZACAO'
        logger.info('PADRONIZACAO', 'Padronizando nomes de colunas')
        df_status = padronizar_colunas_status(df_status)
        df_status_resposta = padronizar_colunas_status_resposta(df_status_resposta)

        etapa_atual = 'NORMALIZACAO_TIPOS'
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
            qtd_nat_status = int(df_status['Data agendamento'].isna().sum())
            pct_nat_status = (qtd_nat_status / len(df_status)) * 100
            logger.info(
                'NORMALIZACAO',
                f'Data agendamento NaT={pct_nat_status:.2f}% ({qtd_nat_status}/{len(df_status)})',
            )
            if pct_nat_status >= limiar_nat_data:
                mensagem_alerta = _mensagem_alerta_nat(
                    'Data agendamento', pct_nat_status, qtd_nat_status, len(df_status)
                )
                logger.error('VALIDACAO_DATA', mensagem_alerta)
                erros_qualidade_data.append(mensagem_alerta)
        if 'DT_ATENDIMENTO' in df_status_resposta.columns and len(df_status_resposta) > 0:
            qtd_nat_resposta = int(df_status_resposta['DT_ATENDIMENTO'].isna().sum())
            pct_nat_resposta = (qtd_nat_resposta / len(df_status_resposta)) * 100
            logger.info(
                'NORMALIZACAO',
                f'DT_ATENDIMENTO NaT={pct_nat_resposta:.2f}% ({qtd_nat_resposta}/{len(df_status_resposta)})',
            )
            if pct_nat_resposta >= limiar_nat_data:
                mensagem_alerta = _mensagem_alerta_nat(
                    'DT_ATENDIMENTO', pct_nat_resposta, qtd_nat_resposta, len(df_status_resposta)
                )
                logger.error('VALIDACAO_DATA', mensagem_alerta)
                erros_qualidade_data.append(mensagem_alerta)

        etapa_atual = 'LIMPEZA_TEXTO'
        logger.info('NORMALIZACAO', 'Limpando texto nas colunas nao-data')
        df_status = limpar_texto_exceto_colunas(df_status, colunas_ignorar=['Data agendamento'])
        df_status_resposta = limpar_texto_exceto_colunas(
            df_status_resposta, colunas_ignorar=['DT_ATENDIMENTO']
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
        resultado_final = {
            'ok': (
                resultado_colunas_origem['ok']
                and resultado_validacao['ok']
                and len(erros_qualidade_data) == 0
            ),
            'mensagens': (
                mensagens_iniciais
                + resultado_colunas_origem['mensagens']
                + resultado_validacao['mensagens']
                + alertas_data
                + erros_qualidade_data
            ),
        }

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

        status_final = 'SUCESSO' if resultado_final['ok'] else 'FALHA_VALIDACAO_DATA'
        if len(erros_qualidade_data) > 0:
            status_final = 'FALHA_QUALIDADE_DATA'
        if finalizar_logger:
            logger.finalizar(status_final)
        return resultado_final
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        resultado_erro = {
            'ok': False,
            'mensagens': [f'Erro inesperado na execucao (etapa={etapa_atual}): {type(erro).__name__}: {erro}'],
        }
        if finalizar_logger:
            logger.finalizar('ERRO')
        return resultado_erro


def executar_ingestao_complicacao(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_complicacao='src/data/status_resposta_complicacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    limiar_nat_data=LIMIAR_AVISO_PERCENTUAL_NAT_DATA,
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
        mensagens_iniciais=['Modo complicacao selecionado.'],
        logger=logger,
        finalizar_logger=not logger_externo,
    )
    if not resultado.get('ok'):
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
    limiar_nat_data=LIMIAR_AVISO_PERCENTUAL_NAT_DATA,
    logger=None,
):
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(nome_pipeline=nome_logger)
    logger.info('INICIO', f'arquivo_status={arquivo_status}')
    logger.info('INICIO', f'saida_status={saida_status}')
    logger.info('INICIO', f'limiar_nat_data_em_uso={limiar_nat_data}')
    etapa_atual = 'INICIO'
    try:
        alertas_data = []
        etapa_atual = 'LEITURA_STATUS'
        df_status = ler_arquivo_csv(arquivo_status)
        logger.info('LEITURA', f'df_status: linhas={len(df_status)} colunas={len(df_status.columns)}')

        etapa_atual = 'PADRONIZACAO'
        df_status = padronizar_colunas_status(df_status)
        etapa_atual = 'NORMALIZACAO_TIPOS'
        df_status = normalizar_tipos_dataframe(df_status, colunas_data=['Data agendamento'])
        if 'Data agendamento' in df_status.columns and len(df_status) > 0:
            qtd_nat_status = int(df_status['Data agendamento'].isna().sum())
            pct_nat_status = (qtd_nat_status / len(df_status)) * 100
            logger.info(
                'NORMALIZACAO',
                f'Data agendamento NaT={pct_nat_status:.2f}% ({qtd_nat_status}/{len(df_status)})',
            )
            if pct_nat_status >= limiar_nat_data:
                mensagem_alerta = _mensagem_alerta_nat(
                    'Data agendamento', pct_nat_status, qtd_nat_status, len(df_status)
                )
                logger.warning('VALIDACAO_DATA', mensagem_alerta)
                alertas_data.append(mensagem_alerta)
        etapa_atual = 'LIMPEZA_TEXTO'
        df_status = limpar_texto_exceto_colunas(df_status, colunas_ignorar=['Data agendamento'])
        etapa_atual = 'CRIAR_DT_ENVIO'
        criar_coluna_dt_envio_por_data_agendamento(df_status)

        etapa_atual = 'SALVAR_ARQUIVO'
        salvar_dataframe(df_status, saida_status)
        if not logger_externo:
            logger.finalizar('SUCESSO')
        return {
            'ok': True,
            'arquivo_saida': saida_status,
            'mensagens': ['Ingestao somente status executada com sucesso.'] + alertas_data,
        }
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        if not logger_externo:
            logger.finalizar('ERRO')
        return {
            'ok': False,
            'mensagens': [f'Erro na ingestao somente status (etapa={etapa_atual}): {type(erro).__name__}: {erro}'],
        }


def executar_ingestao_unificar(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_eletivo='src/data/status_resposta_eletivo.csv',
    arquivo_status_resposta_internacao='src/data/status_resposta_internacao.csv',
    arquivo_status_resposta_unificado='src/data/status_resposta_eletivo_internacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_eletivo_internacao_limpo.csv',
    limiar_nat_data=LIMIAR_AVISO_PERCENTUAL_NAT_DATA,
    logger=None,
):
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(nome_pipeline='ingestao_unificar')
    logger.info('MODO', 'Modo unificar iniciado')
    logger.info('MODO', f'arquivo_eletivo={arquivo_status_resposta_eletivo}')
    logger.info('MODO', f'arquivo_internacao={arquivo_status_resposta_internacao}')
    logger.info('MODO', f'arquivo_unificado={arquivo_status_resposta_unificado}')
    logger.info('MODO', f'limiar_nat_data_em_uso={limiar_nat_data}')

    resultado_concat = run_unificar_status_respostas_pipeline(
        arquivo_eletivo=arquivo_status_resposta_eletivo,
        arquivo_internacao=arquivo_status_resposta_internacao,
        arquivo_saida=arquivo_status_resposta_unificado,
        logger=logger,
    )
    logger.info(
        'CONCATENACAO',
        f"ok={resultado_concat['ok']} mensagens={resultado_concat['mensagens']}",
    )
    if 'total_eletivo' in resultado_concat:
        logger.info('CONCATENACAO', f"total_eletivo={resultado_concat['total_eletivo']}")
    if 'total_internacao' in resultado_concat:
        logger.info('CONCATENACAO', f"total_internacao={resultado_concat['total_internacao']}")
    if 'total_concatenado' in resultado_concat:
        logger.info('CONCATENACAO', f"total_concatenado={resultado_concat['total_concatenado']}")

    if not resultado_concat['ok']:
        logger.warning('CONCATENACAO', 'Concatenacao nao executada por validacao')
        if not logger_externo:
            logger.finalizar('FALHA_CONCATENACAO')
        return resultado_concat

    resultado_csv = executar_normalizacao_padronizacao(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta_unificado,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        limiar_nat_data=limiar_nat_data,
        mensagens_iniciais=resultado_concat['mensagens'],
        logger=logger,
        finalizar_logger=not logger_externo,
    )
    if not resultado_csv.get('ok'):
        return resultado_csv

    arquivo_status_xlsx, tem_status_xlsx = _preferir_xlsx_se_existir(arquivo_status)
    arquivo_eletivo_xlsx, tem_eletivo_xlsx = _preferir_xlsx_se_existir(arquivo_status_resposta_eletivo)
    arquivo_internacao_xlsx, tem_internacao_xlsx = _preferir_xlsx_se_existir(
        arquivo_status_resposta_internacao
    )
    if not (tem_status_xlsx or tem_eletivo_xlsx or tem_internacao_xlsx):
        logger.info('MODO_XLSX', 'Nenhum XLSX de entrada encontrado para execucao adicional.')
        return resultado_csv

    arquivo_unificado_xlsx = _caminho_xlsx_pareado(arquivo_status_resposta_unificado)
    saida_status_xlsx = _obter_saida_status_xlsx(saida_status)
    saida_resposta_xlsx = _obter_saida_status_resposta_xlsx(saida_status_resposta)
    logger.info('MODO_XLSX', 'Execucao adicional XLSX iniciada (unificacao + limpeza).')
    logger.info('MODO_XLSX', f'arquivo_status={arquivo_status_xlsx}')
    logger.info('MODO_XLSX', f'arquivo_eletivo={arquivo_eletivo_xlsx}')
    logger.info('MODO_XLSX', f'arquivo_internacao={arquivo_internacao_xlsx}')
    logger.info('MODO_XLSX', f'arquivo_unificado={arquivo_unificado_xlsx}')

    resultado_concat_xlsx = run_unificar_status_respostas_pipeline(
        arquivo_eletivo=arquivo_eletivo_xlsx,
        arquivo_internacao=arquivo_internacao_xlsx,
        arquivo_saida=arquivo_unificado_xlsx,
        logger=logger,
    )
    if not resultado_concat_xlsx.get('ok'):
        logger.warning('MODO_XLSX', 'Falha na unificacao XLSX; fluxo CSV foi mantido.')
        resultado_csv['mensagens'] = resultado_csv.get('mensagens', []) + [
            'Aviso: falha na execucao adicional XLSX durante unificacao.',
        ]
        return resultado_csv

    resultado_xlsx = executar_normalizacao_padronizacao(
        arquivo_status=arquivo_status_xlsx,
        arquivo_status_resposta=arquivo_unificado_xlsx,
        saida_status=saida_status_xlsx,
        saida_status_resposta=saida_resposta_xlsx,
        limiar_nat_data=limiar_nat_data,
        mensagens_iniciais=['Execucao adicional XLSX (unificacao + limpeza de status).'],
        logger=logger,
        finalizar_logger=False,
    )
    if resultado_xlsx.get('ok'):
        logger.info('MODO_XLSX', 'Execucao adicional XLSX finalizada com sucesso.')
        resultado_csv['mensagens'] = resultado_csv.get('mensagens', []) + [
            f'Saida XLSX gerada: {saida_status_xlsx}',
            f'Saida XLSX gerada: {saida_resposta_xlsx}',
        ]
    else:
        logger.warning('MODO_XLSX', 'Falha na limpeza XLSX; fluxo CSV foi mantido.')
        resultado_csv['mensagens'] = resultado_csv.get('mensagens', []) + [
            'Aviso: falha na execucao adicional XLSX durante limpeza de status.',
        ]
    return resultado_csv
