from core.logger import PipelineLogger
from src.pipelines.concatenar_status_respostas_pipeline import run_unificar_status_respostas_pipeline
from src.services.normalizacao_services import (
    criar_coluna_dt_envio_por_data_agendamento,
    formatar_coluna_data_br,
    limpar_texto_exceto_colunas,
    normalizar_tipos_dataframe,
)
from src.services.schema_service import (
    padronizar_colunas_status,
    padronizar_colunas_status_resposta,
)
from src.services.validacao_service import (
    validar_colunas_origem_para_padronizacao,
    validar_padronizacao_colunas_data,
)
from src.utils.arquivos import ler_arquivo_csv


LIMITE_PERCENTUAL_NAT_DATA = 30.0


def executar_normalizacao_padronizacao(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta='src/data/status_resposta_complicacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    mensagens_iniciais=None,
    logger=None,
):
    if mensagens_iniciais is None:
        mensagens_iniciais = []
    if logger is None:
        logger = PipelineLogger()
    logger.info('INICIO', f'arquivo_status={arquivo_status}')
    logger.info('INICIO', f'arquivo_status_resposta={arquivo_status_resposta}')
    logger.info('INICIO', f'saida_status={saida_status}')
    logger.info('INICIO', f'saida_status_resposta={saida_status_resposta}')

    etapa_atual = 'INICIO'
    try:
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
            if pct_nat_status > LIMITE_PERCENTUAL_NAT_DATA:
                mensagem_nat = (
                    f'Abortado: Data agendamento com NaT acima de {LIMITE_PERCENTUAL_NAT_DATA:.0f}% '
                    f'({pct_nat_status:.2f}%).'
                )
                logger.error('VALIDACAO_DATA', mensagem_nat)
                logger.finalizar('FALHA_QUALIDADE_DATA')
                return {
                    'ok': False,
                    'mensagens': mensagens_iniciais + [mensagem_nat],
                }
        if 'DT_ATENDIMENTO' in df_status_resposta.columns and len(df_status_resposta) > 0:
            qtd_nat_resposta = int(df_status_resposta['DT_ATENDIMENTO'].isna().sum())
            pct_nat_resposta = (qtd_nat_resposta / len(df_status_resposta)) * 100
            logger.info(
                'NORMALIZACAO',
                f'DT_ATENDIMENTO NaT={pct_nat_resposta:.2f}% ({qtd_nat_resposta}/{len(df_status_resposta)})',
            )
            if pct_nat_resposta > LIMITE_PERCENTUAL_NAT_DATA:
                mensagem_nat = (
                    f'Abortado: DT_ATENDIMENTO com NaT acima de {LIMITE_PERCENTUAL_NAT_DATA:.0f}% '
                    f'({pct_nat_resposta:.2f}%).'
                )
                logger.error('VALIDACAO_DATA', mensagem_nat)
                logger.finalizar('FALHA_QUALIDADE_DATA')
                return {
                    'ok': False,
                    'mensagens': mensagens_iniciais + [mensagem_nat],
                }

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
            ),
            'mensagens': (
                mensagens_iniciais
                + resultado_colunas_origem['mensagens']
                + resultado_validacao['mensagens']
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
        df_status.to_csv(saida_status, sep=';', index=False, encoding='utf-8-sig')
        logger.info('SAIDA', f'Salvando status_resposta em {saida_status_resposta}')
        df_status_resposta.to_csv(saida_status_resposta, sep=';', index=False, encoding='utf-8-sig')

        status_final = 'SUCESSO' if resultado_final['ok'] else 'FALHA_VALIDACAO_DATA'
        logger.finalizar(status_final)
        return resultado_final
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        resultado_erro = {
            'ok': False,
            'mensagens': [f'Erro inesperado na execucao (etapa={etapa_atual}): {type(erro).__name__}: {erro}'],
        }
        logger.finalizar('ERRO')
        return resultado_erro


def executar_ingestao_complicacao(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_complicacao='src/data/status_resposta_complicacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
):
    logger = PipelineLogger(nome_pipeline='ingestao_complicacao')
    logger.info('MODO', 'Modo complicacao iniciado')
    return executar_normalizacao_padronizacao(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        mensagens_iniciais=['Modo complicacao selecionado.'],
        logger=logger,
    )


def executar_ingestao_somente_status(
    arquivo_status='src/data/status.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    nome_logger='ingestao_somente_status',
):
    logger = PipelineLogger(nome_pipeline=nome_logger)
    logger.info('INICIO', f'arquivo_status={arquivo_status}')
    logger.info('INICIO', f'saida_status={saida_status}')
    etapa_atual = 'INICIO'
    try:
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
            if pct_nat_status > LIMITE_PERCENTUAL_NAT_DATA:
                mensagem_nat = (
                    f'Abortado: Data agendamento com NaT acima de {LIMITE_PERCENTUAL_NAT_DATA:.0f}% '
                    f'({pct_nat_status:.2f}%).'
                )
                logger.error('VALIDACAO_DATA', mensagem_nat)
                logger.finalizar('FALHA_QUALIDADE_DATA')
                return {
                    'ok': False,
                    'mensagens': [mensagem_nat],
                }
        etapa_atual = 'LIMPEZA_TEXTO'
        df_status = limpar_texto_exceto_colunas(df_status, colunas_ignorar=['Data agendamento'])
        etapa_atual = 'CRIAR_DT_ENVIO'
        criar_coluna_dt_envio_por_data_agendamento(df_status)

        etapa_atual = 'SALVAR_ARQUIVO'
        df_status.to_csv(saida_status, sep=';', index=False, encoding='utf-8-sig')
        logger.finalizar('SUCESSO')
        return {
            'ok': True,
            'arquivo_saida': saida_status,
            'mensagens': ['Ingestao somente status executada com sucesso.'],
        }
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        logger.finalizar('ERRO')
        return {
            'ok': False,
            'mensagens': [f'Erro na ingestao somente status (etapa={etapa_atual}): {type(erro).__name__}: {erro}'],
        }


def executar_ingestao_unificar(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_eletivo='src/data/status_respostas_eletivo.csv',
    arquivo_status_resposta_internacao='src/data/status_resposta_internacao.csv',
    arquivo_status_resposta_unificado='src/data/status_resposta_eletivo_internacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_eletivo_internacao_limpo.csv',
):
    logger = PipelineLogger(nome_pipeline='ingestao_unificar')
    logger.info('MODO', 'Modo unificar iniciado')
    logger.info('MODO', f'arquivo_eletivo={arquivo_status_resposta_eletivo}')
    logger.info('MODO', f'arquivo_internacao={arquivo_status_resposta_internacao}')
    logger.info('MODO', f'arquivo_unificado={arquivo_status_resposta_unificado}')

    resultado_concat = run_unificar_status_respostas_pipeline(
        arquivo_eletivo=arquivo_status_resposta_eletivo,
        arquivo_internacao=arquivo_status_resposta_internacao,
        arquivo_saida=arquivo_status_resposta_unificado,
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
        logger.finalizar('FALHA_CONCATENACAO')
        return resultado_concat

    return executar_normalizacao_padronizacao(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta_unificado,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        mensagens_iniciais=resultado_concat['mensagens'],
        logger=logger,
    )
