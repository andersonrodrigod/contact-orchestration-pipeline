from core.logger import PipelineLogger
from core.pipeline_result import error_result
from src.services.dataset_service import criar_dataset_complicacao
from src.services.validacao_service import validar_colunas_origem_dataset_complicacao
from src.utils.arquivos import ler_arquivo_csv


def run_criacao_dataset_pipeline(
    arquivo_origem_dataset,
    arquivo_saida_dataset,
    nome_logger='criacao_dataset',
    contexto='dataset',
):
    logger = PipelineLogger(nome_pipeline=nome_logger)
    logger.info('INICIO', f'arquivo_origem_dataset={arquivo_origem_dataset}')
    logger.info('INICIO', f'arquivo_saida_dataset={arquivo_saida_dataset}')

    try:
        df_origem = ler_arquivo_csv(arquivo_origem_dataset)
        colunas_arquivo = [str(col).strip() for col in df_origem.columns]
        logger.info('VALIDACAO_COLUNAS', f'colunas_arquivo={colunas_arquivo}')

        resultado_validacao = validar_colunas_origem_dataset_complicacao(colunas_arquivo, contexto=contexto)
        logger.info('VALIDACAO_COLUNAS', f"ok={resultado_validacao['ok']}")
        for mensagem in resultado_validacao['mensagens']:
            logger.info('VALIDACAO_COLUNAS', mensagem)
        if not resultado_validacao['ok']:
            logger.finalizar('FALHA_VALIDACAO_COLUNAS')
            return error_result(mensagens=resultado_validacao['mensagens'])

        resultado = criar_dataset_complicacao(
            arquivo_complicacao=arquivo_origem_dataset,
            arquivo_saida_dataset=arquivo_saida_dataset,
            contexto=contexto,
        )
        if not resultado.get('ok'):
            for mensagem in resultado.get('mensagens', []):
                logger.warning('VALIDACAO_COLUNAS', mensagem)
            logger.finalizar('FALHA_VALIDACAO_COLUNAS')
            return resultado
        logger.info('SAIDA', f"arquivo_saida={resultado.get('arquivo_saida', '')}")
        logger.info('SAIDA', f"total_linhas={resultado.get('total_linhas', 0)}")
        logger.finalizar('SUCESSO')
        return resultado
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        logger.finalizar('ERRO')
        return error_result(mensagens=[f'Erro na criacao do dataset: {erro}'])
