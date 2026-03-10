from core.error_codes import (
    ERRO_CRIACAO_DATASET,
    ERRO_VALIDACAO_ARQUIVOS,
    ERRO_VALIDACAO_COLUNAS,
)
from core.logger import PipelineLogger
from core.pipeline_result import error_result
from pathlib import Path
from src.services.validacao_service import validar_colunas_origem_dataset_complicacao
from src.utils.arquivos import ler_arquivo_csv, validar_arquivos_existem


def caminho_xlsx_pareado(caminho_arquivo):
    caminho = Path(caminho_arquivo)
    return str(caminho.with_suffix('.xlsx'))


def run_criacao_dataset_status_base(
    arquivo_origem_dataset,
    arquivo_status_integrado,
    arquivo_saida_dataset,
    criar_dataset_fn,
    nome_logger,
    contexto,
    logger=None,
    finalizar_logger=True,
):
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(nome_pipeline=nome_logger)
    logger.info('INICIO', f'arquivo_origem_dataset={arquivo_origem_dataset}')
    logger.info('INICIO', f'arquivo_status_integrado={arquivo_status_integrado}')
    logger.info('INICIO', f'arquivo_saida_dataset={arquivo_saida_dataset}')

    try:
        validacao_arquivos = validar_arquivos_existem(
            {
                'arquivo_origem_dataset': arquivo_origem_dataset,
                'arquivo_status_integrado': arquivo_status_integrado,
            }
        )
        if not validacao_arquivos['ok']:
            for mensagem in validacao_arquivos['mensagens']:
                logger.error('VALIDACAO_ARQUIVOS', mensagem)
            if not logger_externo and finalizar_logger:
                logger.finalizar('FALHA_VALIDACAO_ARQUIVOS')
            return error_result(
                mensagens=validacao_arquivos['mensagens'],
                codigo_erro=ERRO_VALIDACAO_ARQUIVOS,
            )

        df_origem = ler_arquivo_csv(arquivo_origem_dataset)
        colunas_arquivo = [str(col).strip() for col in df_origem.columns]
        resultado_validacao = validar_colunas_origem_dataset_complicacao(colunas_arquivo, contexto=contexto)
        logger.info('VALIDACAO_COLUNAS', f"ok={resultado_validacao['ok']}")
        for mensagem in resultado_validacao.get('mensagens', []):
            logger.info('VALIDACAO_COLUNAS', mensagem)
        if not resultado_validacao['ok']:
            if not logger_externo and finalizar_logger:
                logger.finalizar('FALHA_VALIDACAO_COLUNAS')
            return error_result(
                mensagens=resultado_validacao['mensagens'],
                codigo_erro=ERRO_VALIDACAO_COLUNAS,
            )

        resultado = criar_dataset_fn(
            arquivo_complicacao=arquivo_origem_dataset,
            arquivo_saida_dataset=arquivo_saida_dataset,
            arquivo_status_integrado=arquivo_status_integrado,
            contexto=contexto,
        )
        if not resultado.get('ok'):
            if not resultado.get('codigo_erro'):
                resultado['codigo_erro'] = ERRO_CRIACAO_DATASET
            for mensagem in resultado.get('mensagens', []):
                logger.warning('CRIACAO_DATASET', mensagem)
            if not logger_externo and finalizar_logger:
                logger.finalizar('FALHA_CRIACAO_DATASET')
            return resultado

        logger.info('SAIDA', f"arquivo_saida={resultado.get('arquivo_saida', '')}")
        logger.info('SAIDA', f"total_linhas={resultado.get('total_linhas', 0)}")
        if not logger_externo and finalizar_logger:
            logger.finalizar('SUCESSO')
        return resultado
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        if not logger_externo and finalizar_logger:
            logger.finalizar('ERRO')
        return error_result(
            mensagens=[f'Erro na criacao do dataset status: {erro}'],
            codigo_erro=ERRO_CRIACAO_DATASET,
        )
