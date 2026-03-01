from core.logger import PipelineLogger
from src.services.integracao_service import (
    integrar_com_filtro_hsm,
    integrar_somente_status_com_filtro_hsm,
)


def _run_unificar_status_resposta_pipeline(
    arquivo_status,
    arquivo_status_resposta,
    arquivo_saida,
    hsms_permitidos,
    colunas_limpar=None,
    nome_logger='unificar_status_resposta',
):
    logger = PipelineLogger(nome_pipeline=nome_logger)
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
        logger.finalizar('SUCESSO')
        return resultado
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        logger.finalizar('ERRO')
        return {
            'ok': False,
            'mensagens': [f'Erro no pipeline {nome_logger}: {type(erro).__name__}: {erro}'],
        }


def run_unificar_status_resposta_complicacao_pipeline(
    arquivo_status='src/data/arquivo_limpo/status_limpo.csv',
    arquivo_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    arquivo_saida='src/data/arquivo_limpo/status.csv',
):
    return _run_unificar_status_resposta_pipeline(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta,
        arquivo_saida=arquivo_saida,
        hsms_permitidos=['Pesquisa Complicacoes Cirurgicas', 'Pesquisa Complicações Cirurgicas'],
        colunas_limpar=[
            'Conta',
            'Mensagem',
            'Categoria',
            'Template',
            'Agendamento',
            'Status agendamento',
            'Agente',
        ],
        nome_logger='unificar_status_resposta_complicacao',
    )


def run_unificar_status_resposta_internacao_eletivo_pipeline(
    arquivo_status='src/data/arquivo_limpo/status_limpo.csv',
    arquivo_status_resposta='src/data/arquivo_limpo/status_resposta_eletivo_internacao_limpo.csv',
    arquivo_saida='src/data/arquivo_limpo/status.csv',
):
    return _run_unificar_status_resposta_pipeline(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta,
        arquivo_saida=arquivo_saida,
        hsms_permitidos=['Pesquisa_Pos_cir_urg_intern', 'Pesquisa_Pos_cir_eletivo'],
        colunas_limpar=[],
        nome_logger='unificar_status_resposta_internacao_eletivo',
    )


def _run_status_somente_pipeline(
    arquivo_status,
    arquivo_saida,
    hsms_permitidos,
    colunas_limpar=None,
    nome_logger='unificar_status_somente',
):
    logger = PipelineLogger(nome_pipeline=nome_logger)
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
        logger.finalizar('SUCESSO')
        return resultado
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        logger.finalizar('ERRO')
        return {
            'ok': False,
            'mensagens': [f'Erro no pipeline {nome_logger}: {type(erro).__name__}: {erro}'],
        }


def run_status_somente_complicacao_pipeline(
    arquivo_status='src/data/arquivo_limpo/status_limpo.csv',
    arquivo_saida='src/data/arquivo_limpo/status.csv',
):
    return _run_status_somente_pipeline(
        arquivo_status=arquivo_status,
        arquivo_saida=arquivo_saida,
        hsms_permitidos=['Pesquisa Complicacoes Cirurgicas', 'Pesquisa Complicações Cirurgicas'],
        colunas_limpar=[
            'Conta',
            'Mensagem',
            'Categoria',
            'Template',
            'Agendamento',
            'Status agendamento',
            'Agente',
        ],
        nome_logger='unificar_status_somente_complicacao',
    )


def run_status_somente_internacao_eletivo_pipeline(
    arquivo_status='src/data/arquivo_limpo/status_limpo.csv',
    arquivo_saida='src/data/arquivo_limpo/status.csv',
):
    return _run_status_somente_pipeline(
        arquivo_status=arquivo_status,
        arquivo_saida=arquivo_saida,
        hsms_permitidos=['Pesquisa_Pos_cir_urg_intern', 'Pesquisa_Pos_cir_eletivo'],
        colunas_limpar=[],
        nome_logger='unificar_status_somente_internacao_eletivo',
    )
