from core.logger import PipelineLogger
from src.services.finalizacao_service import gerar_dataset_final


def run_finalizacao_pipeline(
    arquivo_dataset_entrada,
    arquivo_dataset_saida,
    nome_logger='finalizacao_dataset',
):
    logger = PipelineLogger(nome_pipeline=nome_logger)
    logger.info('INICIO', f'arquivo_dataset_entrada={arquivo_dataset_entrada}')
    logger.info('INICIO', f'arquivo_dataset_saida={arquivo_dataset_saida}')

    try:
        resultado = gerar_dataset_final(
            arquivo_dataset_entrada=arquivo_dataset_entrada,
            arquivo_dataset_saida=arquivo_dataset_saida,
        )
        logger.info('RESULTADO', f"ok={resultado.get('ok', False)}")
        logger.info('RESULTADO', f"total_usuarios={resultado.get('total_usuarios', 0)}")
        logger.info(
            'RESULTADO',
            f"total_usuarios_resolvidos={resultado.get('total_usuarios_resolvidos', 0)}",
        )
        for mensagem in resultado.get('mensagens', []):
            logger.info('RESULTADO', mensagem)
        logger.finalizar('SUCESSO' if resultado.get('ok') else 'FALHA')
        return resultado
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        logger.finalizar('ERRO')
        return {
            'ok': False,
            'mensagens': [f'Erro na finalizacao do dataset: {type(erro).__name__}: {erro}'],
        }
