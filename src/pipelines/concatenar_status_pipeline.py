from core.error_codes import ERRO_CONCATENACAO
from core.logger import PipelineLogger
from src.services.dataset_service import concatenar_status_complicacao_internacao_eletivo


def run_unificar_status_pipeline(
    arquivo_status_complicacao='src/data/arquivo_limpo/status_sem_internacao_eletivo.csv',
    arquivo_status_internacao_eletivo='src/data/arquivo_limpo/status_sem_complicacao.csv',
    arquivo_saida='src/data/arquivo_limpo/status_complicacao_internacao_eletivo.csv',
    arquivo_saida_normalizado=None,  # mantido por compatibilidade; não é usado
    logger=None,
):
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(nome_pipeline='unificar_status')
    logger.info('INICIO', f'arquivo_status_complicacao={arquivo_status_complicacao}')
    logger.info('INICIO', f'arquivo_status_internacao_eletivo={arquivo_status_internacao_eletivo}')
    logger.info('INICIO', f'arquivo_saida={arquivo_saida}')
    if arquivo_saida_normalizado:
        logger.info(
            'INICIO',
            (
                f'arquivo_saida_normalizado={arquivo_saida_normalizado} '
                '(ignorado: concatenação agora não executa normalização)'
            ),
        )

    try:
        resultado = concatenar_status_complicacao_internacao_eletivo(
            arquivo_status_complicacao=arquivo_status_complicacao,
            arquivo_status_internacao_eletivo=arquivo_status_internacao_eletivo,
            arquivo_saida=arquivo_saida,
        )
        logger.info('RESULTADO', f"concatenacao_ok={resultado.get('ok', False)}")
        for mensagem in resultado.get('mensagens', []):
            logger.info('RESULTADO', mensagem)
        if not resultado.get('ok'):
            if not resultado.get('codigo_erro'):
                resultado['codigo_erro'] = ERRO_CONCATENACAO
            if not logger_externo:
                logger.finalizar('FALHA')
            return resultado

        resultado_final = {
            **resultado,
            'arquivo_status_concatenado': arquivo_saida,
            'arquivo_saida': arquivo_saida,
            'total_status_complicacao': resultado.get('total_status_complicacao', 0),
            'total_status_internacao_eletivo': resultado.get('total_status_internacao_eletivo', 0),
            'total_concatenado': resultado.get('total_concatenado', 0),
        }
        if not logger_externo:
            logger.finalizar('SUCESSO')
        return resultado_final
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        if not logger_externo:
            logger.finalizar('ERRO')
        return {
            'ok': False,
            'mensagens': [f'Erro no pipeline unificar_status: {type(erro).__name__}: {erro}'],
            'codigo_erro': ERRO_CONCATENACAO,
        }
