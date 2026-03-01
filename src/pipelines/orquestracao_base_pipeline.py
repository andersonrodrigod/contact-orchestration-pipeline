from core.logger import PipelineLogger
from src.services.orquestracao_service import gerar_dataset_final
from src.utils.arquivos import validar_arquivos_existem


def executar_orquestracao_pipeline(arquivo_dataset_entrada, arquivo_dataset_saida, nome_logger):
    logger = PipelineLogger(nome_pipeline=nome_logger)
    logger.info('INICIO', f'arquivo_dataset_entrada={arquivo_dataset_entrada}')
    logger.info('INICIO', f'arquivo_dataset_saida={arquivo_dataset_saida}')
    try:
        validacao_arquivos = validar_arquivos_existem(
            {'arquivo_dataset_entrada': arquivo_dataset_entrada}
        )
        if not validacao_arquivos['ok']:
            for mensagem in validacao_arquivos['mensagens']:
                logger.error('VALIDACAO_ARQUIVOS', mensagem)
            logger.finalizar('FALHA_VALIDACAO_ARQUIVOS')
            return {'ok': False, 'mensagens': validacao_arquivos['mensagens']}

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
            'mensagens': [f'Erro na orquestracao do dataset: {type(erro).__name__}: {erro}'],
        }
