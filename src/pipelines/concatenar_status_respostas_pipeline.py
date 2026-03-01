from core.logger import PipelineLogger
from src.services.dataset_service import concatenar_status_resposta_eletivo_internacao


def run_unificar_status_respostas_pipeline(
    arquivo_eletivo='src/data/status_respostas_eletivo.csv',
    arquivo_internacao='src/data/status_resposta_internacao.csv',
    arquivo_saida='src/data/status_resposta_eletivo_internacao.csv',
    logger=None,
):
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(nome_pipeline='unificar_status_respostas')
    logger.info('INICIO', f'arquivo_eletivo={arquivo_eletivo}')
    logger.info('INICIO', f'arquivo_internacao={arquivo_internacao}')
    logger.info('INICIO', f'arquivo_saida={arquivo_saida}')

    try:
        resultado = concatenar_status_resposta_eletivo_internacao(
            arquivo_eletivo=arquivo_eletivo,
            arquivo_internacao=arquivo_internacao,
            arquivo_saida=arquivo_saida,
        )
        logger.info('RESULTADO', f"ok={resultado['ok']}")
        for mensagem in resultado.get('mensagens', []):
            logger.info('RESULTADO', mensagem)
        if 'total_eletivo' in resultado:
            logger.info('RESULTADO', f"total_eletivo={resultado['total_eletivo']}")
        if 'total_internacao' in resultado:
            logger.info('RESULTADO', f"total_internacao={resultado['total_internacao']}")
        if 'total_concatenado' in resultado:
            logger.info('RESULTADO', f"total_concatenado={resultado['total_concatenado']}")

        if not logger_externo:
            logger.finalizar('SUCESSO' if resultado.get('ok') else 'FALHA')
        return resultado
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        if not logger_externo:
            logger.finalizar('ERRO')
        return {
            'ok': False,
            'mensagens': [f'Erro no pipeline unificar_status_respostas: {type(erro).__name__}: {erro}'],
        }
