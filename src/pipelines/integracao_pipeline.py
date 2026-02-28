from core.logger import PipelineLogger
from src.services.integracao_service import integrar_com_filtro_hsm


def _integrar_com_filtro_hsm(
    arquivo_status,
    arquivo_status_resposta,
    arquivo_saida,
    hsms_permitidos,
    nome_logger,
    colunas_limpar=None,
):
    logger = PipelineLogger(nome_pipeline=nome_logger)
    logger.info('MODO', f'Integracao iniciada com HSM permitidos={hsms_permitidos}')
    logger.info('INICIO', f'arquivo_status={arquivo_status}')
    logger.info('INICIO', f'arquivo_status_resposta={arquivo_status_resposta}')
    logger.info('INICIO', f'arquivo_saida={arquivo_saida}')

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
        return {'ok': False, 'mensagens': [str(erro)]}


def integrar_dados_status_complicacao(
    arquivo_status='src/data/arquivo_limpo/status_limpo.csv',
    arquivo_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    arquivo_saida='src/data/arquivo_limpo/status.csv',
):
    return _integrar_com_filtro_hsm(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta,
        arquivo_saida=arquivo_saida,
        hsms_permitidos=['Pesquisa Complicacoes Cirurgicas', 'Pesquisa Complicações Cirurgicas'],
        nome_logger='integracao_complicacao',
        colunas_limpar=[
            'Conta',
            'Mensagem',
            'Categoria',
            'Template',
            'Agendamento',
            'Status agendamento',
            'Agente',
        ],
    )


def integrar_dados_status_unificar(
    arquivo_status='src/data/arquivo_limpo/status_limpo.csv',
    arquivo_status_resposta='src/data/arquivo_limpo/status_resposta_eletivo_internacao_limpo.csv',
    arquivo_saida='src/data/arquivo_limpo/status.csv',
):
    return _integrar_com_filtro_hsm(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta,
        arquivo_saida=arquivo_saida,
        hsms_permitidos=['Pesquisa_Pos_cir_urg_intern', 'Pesquisa_Pos_cir_eletivo'],
        nome_logger='integracao_unificar_execucao',
        colunas_limpar=[],
    )
