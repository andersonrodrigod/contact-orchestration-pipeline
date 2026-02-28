from src.utils.arquivos import ler_arquivo_csv, salvar_output_validacao
from core.logger import PipelineLogger
from src.services.normalizacao_services import (
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
from src.services.dataset_service import concatenar_status_resposta_eletivo_internacao


def _executar_normalizacao_padronizacao(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta='src/data/status_resposta_complicacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    output_validacao='src/data/arquivo_limpo/output_validacao_datas.txt',
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
    logger.info('INICIO', f'output_validacao={output_validacao}')

    try:
        logger.info('LEITURA', 'Lendo arquivo status')
        df_status = ler_arquivo_csv(arquivo_status)
        logger.log_dataframe('LEITURA', 'df_status', df_status)

        logger.info('LEITURA', 'Lendo arquivo status_resposta')
        df_status_resposta = ler_arquivo_csv(arquivo_status_resposta)
        logger.log_dataframe('LEITURA', 'df_status_resposta', df_status_resposta)

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
            salvar_output_validacao(resultado_final, output_validacao)
            logger.warning('VALIDACAO_ORIGEM', 'Falhou validacao de colunas de origem')
            logger.finalizar('FALHA_VALIDACAO_ORIGEM')
            return resultado_final

        logger.info('PADRONIZACAO', 'Padronizando nomes de colunas')
        df_status = padronizar_colunas_status(df_status)
        df_status_resposta = padronizar_colunas_status_resposta(df_status_resposta)
        logger.log_dataframe('PADRONIZACAO', 'df_status', df_status)
        logger.log_dataframe('PADRONIZACAO', 'df_status_resposta', df_status_resposta)

        logger.info('NORMALIZACAO', 'Convertendo tipos de colunas')
        df_status = normalizar_tipos_dataframe(df_status, colunas_data=['DT_ENVIO'])
        df_status_resposta = normalizar_tipos_dataframe(
            df_status_resposta, colunas_data=['DT_ATENDIMENTO']
        )
        logger.info('NORMALIZACAO', f"DT_ENVIO dtype={df_status['DT_ENVIO'].dtype if 'DT_ENVIO' in df_status.columns else 'NA'}")
        logger.info(
            'NORMALIZACAO',
            f"DT_ATENDIMENTO dtype={df_status_resposta['DT_ATENDIMENTO'].dtype if 'DT_ATENDIMENTO' in df_status_resposta.columns else 'NA'}",
        )

        logger.info('NORMALIZACAO', 'Limpando texto nas colunas nao-data')
        df_status = limpar_texto_exceto_colunas(df_status, colunas_ignorar=['DT_ENVIO'])
        df_status_resposta = limpar_texto_exceto_colunas(
            df_status_resposta, colunas_ignorar=['DT_ATENDIMENTO']
        )

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
        salvar_output_validacao(resultado_final, output_validacao)
        logger.info('OUTPUT', f'Arquivo validacao salvo em {output_validacao}')

        logger.info('FORMATACAO', 'Formatando colunas de data para BR')
        formatar_coluna_data_br(df_status, 'DT_ENVIO')
        formatar_coluna_data_br(df_status_resposta, 'DT_ATENDIMENTO')
        logger.info(
            'FORMATACAO',
            f"Exemplo DT_ENVIO={df_status['DT_ENVIO'].head(1).tolist() if 'DT_ENVIO' in df_status.columns else []}",
        )
        logger.info(
            'FORMATACAO',
            f"Exemplo DT_ATENDIMENTO={df_status_resposta['DT_ATENDIMENTO'].head(1).tolist() if 'DT_ATENDIMENTO' in df_status_resposta.columns else []}",
        )

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
            'mensagens': [f'Erro inesperado na execucao: {erro}'],
        }
        salvar_output_validacao(resultado_erro, output_validacao)
        logger.info('OUTPUT', f'Arquivo validacao salvo em {output_validacao}')
        logger.finalizar('ERRO')
        return resultado_erro


def run_ingestao_complicacao(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_complicacao='src/data/status_resposta_complicacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    output_validacao='src/data/arquivo_limpo/output_validacao_datas.txt',
):
    logger = PipelineLogger(nome_pipeline='ingestao_complicacao')
    logger.info('MODO', 'Modo complicacao iniciado')
    return _executar_normalizacao_padronizacao(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        output_validacao=output_validacao,
        mensagens_iniciais=['Modo complicacao selecionado.'],
        logger=logger,
    )


def run_ingestao_unificar(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_eletivo='src/data/status_respostas_eletivo.csv',
    arquivo_status_resposta_internacao='src/data/status_resposta_internacao.csv',
    arquivo_status_resposta_unificado='src/data/status_resposta_eletivo_internacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    output_validacao='src/data/arquivo_limpo/output_validacao_datas.txt',
):
    logger = PipelineLogger(nome_pipeline='ingestao_unificar')
    logger.info('MODO', 'Modo unificar iniciado')
    logger.info('MODO', f'arquivo_eletivo={arquivo_status_resposta_eletivo}')
    logger.info('MODO', f'arquivo_internacao={arquivo_status_resposta_internacao}')
    logger.info('MODO', f'arquivo_unificado={arquivo_status_resposta_unificado}')

    resultado_concat = concatenar_status_resposta_eletivo_internacao(
        arquivo_eletivo=arquivo_status_resposta_eletivo,
        arquivo_internacao=arquivo_status_resposta_internacao,
        arquivo_saida=arquivo_status_resposta_unificado,
    )
    logger.info(
        'CONCATENACAO',
        f"ok={resultado_concat['ok']} mensagens={resultado_concat['mensagens']}",
    )

    if not resultado_concat['ok']:
        salvar_output_validacao(resultado_concat, output_validacao)
        logger.warning('CONCATENACAO', 'Concatenacao nao executada por validacao')
        logger.info('OUTPUT', f'Arquivo validacao salvo em {output_validacao}')
        logger.finalizar('FALHA_CONCATENACAO')
        return resultado_concat

    return _executar_normalizacao_padronizacao(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta_unificado,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        output_validacao=output_validacao,
        mensagens_iniciais=resultado_concat['mensagens'],
        logger=logger,
    )
