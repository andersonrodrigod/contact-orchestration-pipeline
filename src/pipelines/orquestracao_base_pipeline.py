from core.logger import PipelineLogger
from core.error_codes import ERRO_ORQUESTRACAO, ERRO_VALIDACAO_ARQUIVOS
from src.services.analise_dados_fase3_orquestracao_service import (
    gerar_analise_dados_fase3_orquestracao,
)
from src.services.graficos_orquestracao_service import gerar_graficos_orquestracao
from src.services.orquestracao_service import gerar_dataset_final
from src.utils.arquivos import validar_arquivos_existem


def _resolver_raiz_analise(nome_logger):
    base = 'src/data/analise_dados'
    logger_norm = str(nome_logger or '').lower()
    if 'complicacao' in logger_norm:
        return f'{base}/complicacao'
    if 'internacao' in logger_norm or 'eletivo' in logger_norm:
        return f'{base}/internacao'
    return base


def _resolver_contexto(nome_logger):
    logger_norm = str(nome_logger or '').lower()
    if 'complicacao' in logger_norm:
        return 'complicacao'
    if 'internacao' in logger_norm or 'eletivo' in logger_norm:
        return 'internacao'
    return 'complicacao'


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
            return {
                'ok': False,
                'mensagens': validacao_arquivos['mensagens'],
                'codigo_erro': ERRO_VALIDACAO_ARQUIVOS,
            }

        resultado = gerar_dataset_final(
            arquivo_dataset_entrada=arquivo_dataset_entrada,
            arquivo_dataset_saida=arquivo_dataset_saida,
        )
        if resultado.get('ok'):
            resultado_analise_fase3 = gerar_analise_dados_fase3_orquestracao(
                arquivo_dataset_orquestrado=arquivo_dataset_saida,
                raiz_analise=_resolver_raiz_analise(nome_logger),
                nome_processo='orquestracao',
                pipeline_nome=nome_logger,
            )
            for mensagem in resultado_analise_fase3.get('mensagens', []):
                logger.warning('ANALISE_DADOS', mensagem)
            resultado_graficos_fase3 = gerar_graficos_orquestracao(
                contexto=_resolver_contexto(nome_logger),
                raiz_analise_contexto=_resolver_raiz_analise(nome_logger),
                pastas_origem_csv=resultado_analise_fase3.get('pastas_saida', []),
            )
            logger.info(
                'GRAFICOS',
                (
                    "Graficos da Fase 3 (orquestracao) gerados em: "
                    f"{resultado_graficos_fase3.get('pasta_base_saida', '')}"
                ),
            )
            resultado['metricas_por_etapa'] = {
                'orquestracao': {
                    'total_usuarios': resultado.get('total_usuarios', 0),
                    'total_usuarios_resolvidos': resultado.get('total_usuarios_resolvidos', 0),
                    'total_disparo': resultado.get('total_disparo', 0),
                    'pasta_analise_dados_fase3': resultado_analise_fase3.get('pasta_saida', ''),
                }
            }
            resultado['dados'] = resultado.get('dados', {})
            resultado['dados']['analise_dados_fase3'] = resultado_analise_fase3
            resultado['dados']['graficos_orquestracao'] = resultado_graficos_fase3
            resultado['mensagens'] = (
                resultado.get('mensagens', [])
                + resultado_analise_fase3.get('mensagens', [])
                + [
                    f"Analise de dados Fase 3 gerada em: {resultado_analise_fase3.get('pasta_saida', '')}",
                    f"Manifests de graficos Fase 3: {', '.join(resultado_graficos_fase3.get('manifests', []))}",
                ]
            )
        elif not resultado.get('codigo_erro'):
            resultado['codigo_erro'] = ERRO_ORQUESTRACAO
        logger.info('RESULTADO', f"ok={resultado.get('ok', False)}")
        logger.info('RESULTADO', f"total_usuarios={resultado.get('total_usuarios', 0)}")
        logger.info(
            'RESULTADO',
            f"total_usuarios_resolvidos={resultado.get('total_usuarios_resolvidos', 0)}",
        )
        logger.info('RESULTADO', f"total_disparo={resultado.get('total_disparo', 0)}")
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
            'codigo_erro': ERRO_ORQUESTRACAO,
        }
