from core.logger import PipelineLogger
from core.error_codes import ERRO_INTEGRACAO
from src.contexts.integracao_contextos import (
    CONTEXTO_INTEGRACAO_COMPLICACAO,
    CONTEXTO_INTEGRACAO_INTERNACAO_ELETIVO,
)
from src.services.integracao_service import (
    integrar_com_filtro_hsm,
    integrar_somente_status_com_filtro_hsm,
)


def _resolver_logger_pipeline(nome_logger, logger=None):
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(nome_pipeline=nome_logger)
    return logger, logger_externo


def _erro_pipeline(logger, logger_externo, nome_logger, erro):
    logger.exception('ERRO_EXECUCAO', erro)
    if not logger_externo:
        logger.finalizar('ERRO')
    return {
        'ok': False,
        'mensagens': [f'Erro no pipeline {nome_logger}: {type(erro).__name__}: {erro}'],
        'codigo_erro': ERRO_INTEGRACAO,
    }


def _finalizar_sucesso_pipeline(logger, logger_externo, resultado, descartados_status, descartados_resposta):
    mensagens = resultado.get('mensagens', [])
    resultado['mensagens'] = mensagens + [
        f'Descartados por data invalida (status): {descartados_status}',
        f'Descartados por data invalida (status_resposta): {descartados_resposta}',
    ]
    if not logger_externo:
        logger.finalizar('SUCESSO')
    return resultado


def _run_unificar_status_resposta_pipeline(
    arquivo_status,
    arquivo_status_resposta,
    arquivo_saida,
    hsms_permitidos,
    colunas_limpar=None,
    nome_logger='unificar_status_resposta',
    logger=None,
):
    logger, logger_externo = _resolver_logger_pipeline(nome_logger, logger=logger)
    logger.info('INICIO', f'arquivo_status={arquivo_status}')
    logger.info('INICIO', f'arquivo_status_resposta={arquivo_status_resposta}')
    logger.info('INICIO', f'arquivo_saida={arquivo_saida}')
    logger.info('INICIO', f'hsms_permitidos={hsms_permitidos}')

    try:
        resultado = integrar_com_filtro_hsm(
            arquivo_status=arquivo_status,
            arquivo_status_resposta=arquivo_status_resposta,
            arquivo_saida=arquivo_saida,
            hsms_permitidos=hsms_permitidos,
            colunas_limpar=colunas_limpar,
        )
        resumo_filtro = resultado['resumo_filtro']
        logger.info('FILTRO_HSM', f"status antes={resumo_filtro['total_antes']}")
        logger.info('FILTRO_HSM', f"status depois={resumo_filtro['total_depois']}")
        logger.info('MATCH', f"total_status={resultado['total_status']}")
        logger.info('MATCH', f"com_match={resultado['com_match']}")
        logger.info('MATCH', f"sem_match={resultado['sem_match']}")
        descartados_status = int(resultado.get('descartados_status_data_invalida', 0))
        descartados_resposta = int(resultado.get('descartados_resposta_data_invalida', 0))
        logger.info('MATCH', f'descartados_status_data_invalida={descartados_status}')
        logger.info('MATCH', f'descartados_resposta_data_invalida={descartados_resposta}')
        return _finalizar_sucesso_pipeline(
            logger,
            logger_externo,
            resultado,
            descartados_status,
            descartados_resposta,
        )
    except Exception as erro:
        return _erro_pipeline(logger, logger_externo, nome_logger, erro)


def run_unificar_status_resposta_complicacao_pipeline(
    arquivo_status='src/data/arquivo_limpo/status_limpo.csv',
    arquivo_status_resposta='src/data/arquivo_limpo/flow_resposta_complicacao_limpo.csv',
    arquivo_saida='src/data/arquivo_limpo/status.csv',
    logger=None,
):
    return _run_unificar_status_resposta_pipeline(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta,
        arquivo_saida=arquivo_saida,
        hsms_permitidos=list(CONTEXTO_INTEGRACAO_COMPLICACAO.hsms_permitidos),
        colunas_limpar=list(CONTEXTO_INTEGRACAO_COMPLICACAO.colunas_limpar),
        nome_logger='unificar_status_resposta_complicacao',
        logger=logger,
    )


def run_unificar_status_resposta_internacao_eletivo_pipeline(
    arquivo_status='src/data/arquivo_limpo/status_limpo.csv',
    arquivo_status_resposta='src/data/arquivo_limpo/flow_resposta_eletivo_internacao_limpo.csv',
    arquivo_saida='src/data/arquivo_limpo/status.csv',
    logger=None,
):
    return _run_unificar_status_resposta_pipeline(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta,
        arquivo_saida=arquivo_saida,
        hsms_permitidos=list(CONTEXTO_INTEGRACAO_INTERNACAO_ELETIVO.hsms_permitidos),
        colunas_limpar=list(CONTEXTO_INTEGRACAO_INTERNACAO_ELETIVO.colunas_limpar),
        nome_logger='unificar_status_resposta_internacao_eletivo',
        logger=logger,
    )


def _run_status_somente_pipeline(
    arquivo_status,
    arquivo_saida,
    hsms_permitidos,
    colunas_limpar=None,
    nome_logger='unificar_status_somente',
    logger=None,
):
    logger, logger_externo = _resolver_logger_pipeline(nome_logger, logger=logger)
    logger.info('INICIO', f'arquivo_status={arquivo_status}')
    logger.info('INICIO', f'arquivo_saida={arquivo_saida}')
    logger.info('INICIO', f'hsms_permitidos={hsms_permitidos}')

    try:
        resultado = integrar_somente_status_com_filtro_hsm(
            arquivo_status=arquivo_status,
            arquivo_saida=arquivo_saida,
            hsms_permitidos=hsms_permitidos,
            colunas_limpar=colunas_limpar,
        )
        resumo_filtro = resultado['resumo_filtro']
        logger.info('FILTRO_HSM', f"status antes={resumo_filtro['total_antes']}")
        logger.info('FILTRO_HSM', f"status depois={resumo_filtro['total_depois']}")
        logger.info('RESULTADO', f"total_status={resultado['total_status']}")
        descartados_status = int(resultado.get('descartados_status_data_invalida', 0))
        descartados_resposta = int(resultado.get('descartados_resposta_data_invalida', 0))
        logger.info('RESULTADO', f'descartados_status_data_invalida={descartados_status}')
        logger.info('RESULTADO', f'descartados_resposta_data_invalida={descartados_resposta}')
        return _finalizar_sucesso_pipeline(
            logger,
            logger_externo,
            resultado,
            descartados_status,
            descartados_resposta,
        )
    except Exception as erro:
        return _erro_pipeline(logger, logger_externo, nome_logger, erro)


def run_status_somente_complicacao_pipeline(
    arquivo_status='src/data/arquivo_limpo/status_limpo.csv',
    arquivo_saida='src/data/arquivo_limpo/status.csv',
    logger=None,
):
    return _run_status_somente_pipeline(
        arquivo_status=arquivo_status,
        arquivo_saida=arquivo_saida,
        hsms_permitidos=list(CONTEXTO_INTEGRACAO_COMPLICACAO.hsms_permitidos),
        colunas_limpar=list(CONTEXTO_INTEGRACAO_COMPLICACAO.colunas_limpar),
        nome_logger='unificar_status_somente_complicacao',
        logger=logger,
    )


def run_status_somente_internacao_eletivo_pipeline(
    arquivo_status='src/data/arquivo_limpo/status_limpo.csv',
    arquivo_saida='src/data/arquivo_limpo/status.csv',
    logger=None,
):
    return _run_status_somente_pipeline(
        arquivo_status=arquivo_status,
        arquivo_saida=arquivo_saida,
        hsms_permitidos=list(CONTEXTO_INTEGRACAO_INTERNACAO_ELETIVO.hsms_permitidos),
        colunas_limpar=list(CONTEXTO_INTEGRACAO_INTERNACAO_ELETIVO.colunas_limpar),
        nome_logger='unificar_status_somente_internacao_eletivo',
        logger=logger,
    )
