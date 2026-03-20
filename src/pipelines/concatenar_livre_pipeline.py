from core.error_codes import ERRO_CONCATENACAO
from core.logger import PipelineLogger
from src.services.dataset_service import concatenar_arquivos_livre


def run_unificar_arquivos_livre_pipeline(
    arquivo_a,
    arquivo_b,
    arquivo_saida,
    logger=None,
):
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(nome_pipeline='unificar_arquivos_livre')

    logger.info('INICIO', f'arquivo_a={arquivo_a}')
    logger.info('INICIO', f'arquivo_b={arquivo_b}')
    logger.info('INICIO', f'arquivo_saida={arquivo_saida}')

    try:
        resultado = concatenar_arquivos_livre(
            arquivo_a=arquivo_a,
            arquivo_b=arquivo_b,
            arquivo_saida=arquivo_saida,
        )

        logger.info('RESULTADO', f"ok={resultado.get('ok', False)}")
        for mensagem in resultado.get('mensagens', []):
            logger.info('RESULTADO', mensagem)

        if not logger_externo:
            logger.finalizar('SUCESSO' if resultado.get('ok') else 'FALHA')
        return resultado
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        if not logger_externo:
            logger.finalizar('ERRO')
        return {
            'ok': False,
            'mensagens': [f'Erro no pipeline unificar_arquivos_livre: {type(erro).__name__}: {erro}'],
            'codigo_erro': ERRO_CONCATENACAO,
        }
