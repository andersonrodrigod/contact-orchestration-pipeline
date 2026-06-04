from core.logger import PipelineLogger
from src.contexts.pipeline_contextos import CONTEXTO_PIPELINE_COMPLICACAO
from src.pipelines.complicacao_orquestracao_pipeline import run_complicacao_pipeline_orquestrar
from src.pipelines.complicacao_status_pipeline import run_complicacao_pipeline_criar_dataset_status
from src.pipelines.complicacao_status_pipeline import (
    run_complicacao_pipeline_enviar_status_com_resposta,
    run_complicacao_pipeline_gerar_status_dataset,
)
from src.services.ingestao_service import executar_ingestao_complicacao

DEFAULTS_COMPLICACAO = CONTEXTO_PIPELINE_COMPLICACAO.defaults


def _executar_modo_etapa(nome_modo, funcao_execucao):
    logger = PipelineLogger(nome_pipeline=f'main_{nome_modo}')
    logger.info('MODO_ETAPA', 'Modo de etapa iniciado')
    try:
        resultado = funcao_execucao()
        if not isinstance(resultado, dict):
            resultado = {
                'ok': False,
                'mensagens': [
                    f'Retorno invalido no modo "{nome_modo}": esperado dict, recebido {type(resultado).__name__}.'
                ],
            }
        logger.info('RESULTADO', f"ok={resultado.get('ok', False)}")
        for mensagem in resultado.get('mensagens', []):
            logger.info('RESULTADO', mensagem)
        logger.finalizar('SUCESSO' if resultado.get('ok') else 'FALHA')
        resultado['arquivo_log_individual'] = str(logger.caminho_arquivo)
        return resultado
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        logger.finalizar('ERRO')
        return {
            'ok': False,
            'mensagens': [f'Erro no modo de etapa "{nome_modo}": {type(erro).__name__}: {erro}'],
            'arquivo_log_individual': str(logger.caminho_arquivo),
        }


def obter_modos_etapas():
    def _run_complicacao_ingestao():
        return _executar_modo_etapa(
            'complicacao_ingestao',
            lambda: executar_ingestao_complicacao(
                arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
                arquivo_status_resposta_complicacao=DEFAULTS_COMPLICACAO['arquivo_status_resposta_complicacao'],
                saida_status=DEFAULTS_COMPLICACAO['saida_status'],
                saida_status_resposta=DEFAULTS_COMPLICACAO['saida_status_resposta'],
            ),
        )

    def _run_complicacao_integrar_status_resposta():
        return _executar_modo_etapa(
            'complicacao_integrar_status_resposta',
            lambda: run_complicacao_pipeline_enviar_status_com_resposta(
                arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
                arquivo_status_resposta_complicacao=DEFAULTS_COMPLICACAO['arquivo_status_resposta_complicacao'],
                saida_status=DEFAULTS_COMPLICACAO['saida_status'],
                saida_status_resposta=DEFAULTS_COMPLICACAO['saida_status_resposta'],
                saida_status_integrado=DEFAULTS_COMPLICACAO['saida_status_integrado'],
                executar_xlsx_adicional=True,
            ),
        )

    def _run_complicacao_criar_dataset_status():
        return _executar_modo_etapa(
            'complicacao_criar_dataset_status',
            lambda: run_complicacao_pipeline_criar_dataset_status(
                arquivo_origem_dataset=DEFAULTS_COMPLICACAO['arquivo_dataset_origem_complicacao'],
                arquivo_status_integrado=DEFAULTS_COMPLICACAO['saida_status_integrado'],
                arquivo_saida_dataset=DEFAULTS_COMPLICACAO['saida_dataset_status'],
                nome_logger='criacao_dataset_complicacao_individual',
                contexto='complicacao',
            ),
        )

    def _run_complicacao_gerar_dataset_status():
        return _executar_modo_etapa(
            'complicacao_gerar_dataset_status',
            lambda: run_complicacao_pipeline_gerar_status_dataset(
                arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
                arquivo_status_resposta_complicacao=DEFAULTS_COMPLICACAO['arquivo_status_resposta_complicacao'],
                arquivo_dataset_origem_complicacao=DEFAULTS_COMPLICACAO['arquivo_dataset_origem_complicacao'],
                saida_status=DEFAULTS_COMPLICACAO['saida_status'],
                saida_status_resposta=DEFAULTS_COMPLICACAO['saida_status_resposta'],
                saida_status_integrado=DEFAULTS_COMPLICACAO['saida_status_integrado'],
                saida_dataset_status=DEFAULTS_COMPLICACAO['saida_dataset_status'],
            ),
        )

    def _run_complicacao_orquestrar():
        return _executar_modo_etapa(
            'complicacao_orquestrar',
            lambda: run_complicacao_pipeline_orquestrar(
                arquivo_dataset_status=DEFAULTS_COMPLICACAO['saida_dataset_status'],
                arquivo_saida_final=DEFAULTS_COMPLICACAO['saida_dataset_final'],
                nome_logger='orquestracao_complicacao_individual',
            ),
        )

    return {
        'complicacao_ingestao': _run_complicacao_ingestao,
        'complicacao_integrar_status_resposta': _run_complicacao_integrar_status_resposta,
        'complicacao_criar_dataset_status': _run_complicacao_criar_dataset_status,
        'complicacao_gerar_dataset_status': _run_complicacao_gerar_dataset_status,
        'complicacao_orquestrar': _run_complicacao_orquestrar,
    }
