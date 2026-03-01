from core.logger import PipelineLogger
from core.pipeline_result import error_result
from core.pipeline_result import ok_result
from src.pipelines.join_status_resposta_pipeline import (
    run_status_somente_complicacao_pipeline,
    run_unificar_status_resposta_complicacao_pipeline,
)
from src.services.dataset_service import criar_dataset_complicacao
from src.services.ingestao_service import executar_ingestao_complicacao, executar_ingestao_somente_status
from src.services.validacao_service import validar_colunas_origem_dataset_complicacao
from src.utils.arquivos import ler_arquivo_csv, validar_arquivos_existem


def _run_criacao_dataset_status(
    arquivo_origem_dataset,
    arquivo_status_integrado,
    arquivo_saida_dataset,
    nome_logger='criacao_dataset_status_complicacao',
    contexto='complicacao',
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
            return error_result(mensagens=validacao_arquivos['mensagens'])

        df_origem = ler_arquivo_csv(arquivo_origem_dataset)
        colunas_arquivo = [str(col).strip() for col in df_origem.columns]
        resultado_validacao = validar_colunas_origem_dataset_complicacao(colunas_arquivo, contexto=contexto)
        logger.info('VALIDACAO_COLUNAS', f"ok={resultado_validacao['ok']}")
        for mensagem in resultado_validacao.get('mensagens', []):
            logger.info('VALIDACAO_COLUNAS', mensagem)
        if not resultado_validacao['ok']:
            if not logger_externo and finalizar_logger:
                logger.finalizar('FALHA_VALIDACAO_COLUNAS')
            return error_result(mensagens=resultado_validacao['mensagens'])

        resultado = criar_dataset_complicacao(
            arquivo_complicacao=arquivo_origem_dataset,
            arquivo_saida_dataset=arquivo_saida_dataset,
            arquivo_status_integrado=arquivo_status_integrado,
            contexto=contexto,
        )
        if not resultado.get('ok'):
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
        return error_result(mensagens=[f'Erro na criacao do dataset status: {erro}'])


def run_complicacao_pipeline_enviar_status_com_resposta(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_complicacao='src/data/status_resposta_complicacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    saida_status_integrado='src/data/arquivo_limpo/status_complicacao.csv',
    logger=None,
):
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(nome_pipeline='status_complicacao_pipeline')
    resultado_ingestao = executar_ingestao_complicacao(
        arquivo_status=arquivo_status,
        arquivo_status_resposta_complicacao=arquivo_status_resposta_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        logger=logger,
    )
    if not resultado_ingestao.get('ok'):
        if not logger_externo:
            logger.finalizar('FALHA_INGESTAO')
        return resultado_ingestao

    resultado_integracao = run_unificar_status_resposta_complicacao_pipeline(
        arquivo_status=saida_status,
        arquivo_status_resposta=saida_status_resposta,
        arquivo_saida=saida_status_integrado,
        logger=logger,
    )
    if not resultado_integracao.get('ok'):
        if not logger_externo:
            logger.finalizar('FALHA_INTEGRACAO')
        return resultado_integracao

    resultado = ok_result(
        mensagens=resultado_integracao.get('mensagens', []),
        metricas={
            'total_status': resultado_integracao.get('total_status', 0),
            'com_match': resultado_integracao.get('com_match', 0),
            'sem_match': resultado_integracao.get('sem_match', 0),
        },
        arquivos={'arquivo_status_integrado': resultado_integracao.get('arquivo_saida')},
    )
    if not logger_externo:
        logger.finalizar('SUCESSO')
    return resultado


def run_complicacao_pipeline_enviar_status_somente_status(
    arquivo_status='src/data/status.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_integrado='src/data/arquivo_limpo/status_complicacao.csv',
    logger=None,
):
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(nome_pipeline='status_complicacao_somente_status_pipeline')
    resultado_ingestao = executar_ingestao_somente_status(
        arquivo_status=arquivo_status,
        saida_status=saida_status,
        nome_logger='ingestao_complicacao_somente_status',
        logger=logger,
    )
    if not resultado_ingestao.get('ok'):
        if not logger_externo:
            logger.finalizar('FALHA_INGESTAO')
        return resultado_ingestao

    resultado_status = run_status_somente_complicacao_pipeline(
        arquivo_status=saida_status,
        arquivo_saida=saida_status_integrado,
        logger=logger,
    )
    if not resultado_status.get('ok'):
        if not logger_externo:
            logger.finalizar('FALHA_INTEGRACAO')
        return resultado_status

    resultado = ok_result(
        mensagens=resultado_status.get('mensagens', []),
        metricas={
            'total_status': resultado_status.get('total_status', 0),
            'com_match': resultado_status.get('com_match', 0),
            'sem_match': resultado_status.get('sem_match', 0),
        },
        arquivos={'arquivo_status_integrado': resultado_status.get('arquivo_saida')},
    )
    if not logger_externo:
        logger.finalizar('SUCESSO')
    return resultado


def run_complicacao_pipeline_gerar_status_dataset(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_complicacao='src/data/status_resposta_complicacao.csv',
    arquivo_dataset_origem_complicacao='src/data/complicacao.xlsx',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    saida_status_integrado='src/data/arquivo_limpo/status_complicacao.csv',
    saida_dataset_status='src/data/arquivo_limpo/complicacao_status.xlsx',
):
    logger = PipelineLogger(nome_pipeline='status_complicacao_pipeline')
    resultado_status = run_complicacao_pipeline_enviar_status_com_resposta(
        arquivo_status=arquivo_status,
        arquivo_status_resposta_complicacao=arquivo_status_resposta_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        saida_status_integrado=saida_status_integrado,
        logger=logger,
    )
    if not resultado_status.get('ok'):
        logger.finalizar('FALHA')
        return resultado_status

    resultado_dataset = run_complicacao_pipeline_criar_dataset_status(
        arquivo_origem_dataset=arquivo_dataset_origem_complicacao,
        arquivo_status_integrado=saida_status_integrado,
        arquivo_saida_dataset=saida_dataset_status,
        nome_logger='criacao_dataset_complicacao',
        contexto='complicacao',
        logger=logger,
        finalizar_logger=False,
    )
    if not resultado_dataset.get('ok'):
        logger.finalizar('FALHA')
        return resultado_dataset

    resultado = ok_result(
        mensagens=(
            resultado_status.get('mensagens', [])
            + resultado_dataset.get('mensagens', [])
        ),
        metricas={
            'total_status': resultado_status.get('total_status', 0),
            'com_match': resultado_status.get('com_match', 0),
            'sem_match': resultado_status.get('sem_match', 0),
            'total_linhas': resultado_dataset.get('total_linhas', 0),
        },
        arquivos={'arquivo_status_dataset': resultado_dataset.get('arquivo_saida')},
    )
    logger.finalizar('SUCESSO')
    return resultado


def run_complicacao_pipeline_gerar_status_dataset_somente_status(
    arquivo_status='src/data/status.csv',
    arquivo_dataset_origem_complicacao='src/data/complicacao.xlsx',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_integrado='src/data/arquivo_limpo/status_complicacao.csv',
    saida_dataset_status='src/data/arquivo_limpo/complicacao_status.xlsx',
):
    logger = PipelineLogger(nome_pipeline='status_complicacao_somente_status_pipeline')
    resultado_status = run_complicacao_pipeline_enviar_status_somente_status(
        arquivo_status=arquivo_status,
        saida_status=saida_status,
        saida_status_integrado=saida_status_integrado,
        logger=logger,
    )
    if not resultado_status.get('ok'):
        logger.finalizar('FALHA')
        return resultado_status

    resultado_dataset = run_complicacao_pipeline_criar_dataset_status(
        arquivo_origem_dataset=arquivo_dataset_origem_complicacao,
        arquivo_status_integrado=saida_status_integrado,
        arquivo_saida_dataset=saida_dataset_status,
        nome_logger='criacao_dataset_complicacao_somente_status',
        contexto='complicacao',
        logger=logger,
        finalizar_logger=False,
    )
    if not resultado_dataset.get('ok'):
        logger.finalizar('FALHA')
        return resultado_dataset

    resultado = ok_result(
        mensagens=(
            resultado_status.get('mensagens', [])
            + resultado_dataset.get('mensagens', [])
        ),
        metricas={
            'total_status': resultado_status.get('total_status', 0),
            'com_match': resultado_status.get('com_match', 0),
            'sem_match': resultado_status.get('sem_match', 0),
            'total_linhas': resultado_dataset.get('total_linhas', 0),
        },
        arquivos={'arquivo_status_dataset': resultado_dataset.get('arquivo_saida')},
    )
    logger.finalizar('SUCESSO')
    return resultado


def run_complicacao_pipeline_criar_dataset_status(
    arquivo_origem_dataset='src/data/complicacao.xlsx',
    arquivo_status_integrado='src/data/arquivo_limpo/status_complicacao.csv',
    arquivo_saida_dataset='src/data/arquivo_limpo/complicacao_status.xlsx',
    nome_logger='criacao_dataset_complicacao',
    contexto='complicacao',
    logger=None,
    finalizar_logger=True,
):
    return _run_criacao_dataset_status(
        arquivo_origem_dataset=arquivo_origem_dataset,
        arquivo_status_integrado=arquivo_status_integrado,
        arquivo_saida_dataset=arquivo_saida_dataset,
        nome_logger=nome_logger,
        contexto=contexto,
        logger=logger,
        finalizar_logger=finalizar_logger,
    )
