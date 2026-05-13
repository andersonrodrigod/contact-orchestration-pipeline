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


def _modo_individual_bloqueado(nome_modo):
    logger = PipelineLogger(nome_pipeline=f'main_{nome_modo}')
    logger.warning('MODO_INDIVIDUAL', 'Modo individual desabilitado por configuracao')
    logger.finalizar('BLOQUEADO')
    return {
        'ok': False,
        'mensagens': [
            f'Modo individual "{nome_modo}" desabilitado. '
            'Defina ALLOW_MODOS_INDIVIDUAIS = True no main.py para executar.'
        ],
        'arquivo_log_individual': str(logger.caminho_arquivo),
    }


def _executar_modo_individual(nome_modo, permitir_execucao, funcao_execucao):
    if not permitir_execucao:
        return _modo_individual_bloqueado(nome_modo)

    logger = PipelineLogger(nome_pipeline=f'main_{nome_modo}')
    logger.info('MODO_INDIVIDUAL', 'Modo individual habilitado')
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
            'mensagens': [f'Erro no modo individual "{nome_modo}": {type(erro).__name__}: {erro}'],
            'arquivo_log_individual': str(logger.caminho_arquivo),
        }


def obter_modos_individuais(permitir_execucao=False):
    def _run_individual_ingestao_complicacao():
        return _executar_modo_individual(
            'individual_ingestao_complicacao',
            permitir_execucao,
            lambda: executar_ingestao_complicacao(
                arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
                arquivo_status_resposta_complicacao=DEFAULTS_COMPLICACAO['arquivo_status_resposta_complicacao'],
                saida_status=DEFAULTS_COMPLICACAO['saida_status'],
                saida_status_resposta=DEFAULTS_COMPLICACAO['saida_status_resposta'],
            ),
        )

    def _run_individual_enviar_status_complicacao():
        return _executar_modo_individual(
            'individual_enviar_status_complicacao',
            permitir_execucao,
            lambda: run_complicacao_pipeline_enviar_status_com_resposta(
                arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
                arquivo_status_resposta_complicacao=DEFAULTS_COMPLICACAO['arquivo_status_resposta_complicacao'],
                saida_status=DEFAULTS_COMPLICACAO['saida_status'],
                saida_status_resposta=DEFAULTS_COMPLICACAO['saida_status_resposta'],
                saida_status_integrado=DEFAULTS_COMPLICACAO['saida_status_integrado'],
                executar_xlsx_adicional=True,
            ),
        )

    def _run_individual_criar_dataset_complicacao():
        return _executar_modo_individual(
            'individual_criar_dataset_complicacao',
            permitir_execucao,
            lambda: run_complicacao_pipeline_criar_dataset_status(
                arquivo_origem_dataset=DEFAULTS_COMPLICACAO['arquivo_dataset_origem_complicacao'],
                arquivo_status_integrado=DEFAULTS_COMPLICACAO['saida_status_integrado'],
                arquivo_saida_dataset=DEFAULTS_COMPLICACAO['saida_dataset_status'],
                nome_logger='criacao_dataset_complicacao_individual',
                contexto='complicacao',
            ),
        )

    def _run_individual_gerar_dataset_complicacao_com_resposta():
        return _executar_modo_individual(
            'individual_gerar_dataset_complicacao_com_resposta',
            permitir_execucao,
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

    def _run_individual_orquestrar_complicacao():
        return _executar_modo_individual(
            'individual_orquestrar_complicacao',
            permitir_execucao,
            lambda: run_complicacao_pipeline_orquestrar(
                arquivo_dataset_status=DEFAULTS_COMPLICACAO['saida_dataset_status'],
                arquivo_saida_final=DEFAULTS_COMPLICACAO['saida_dataset_final'],
                nome_logger='orquestracao_complicacao_individual',
            ),
        )

    return {
        'individual_ingestao_complicacao': _run_individual_ingestao_complicacao,
        'individual_enviar_status_complicacao': _run_individual_enviar_status_complicacao,
        'individual_criar_dataset_complicacao': _run_individual_criar_dataset_complicacao,
        'individual_gerar_dataset_complicacao_com_resposta': (
            _run_individual_gerar_dataset_complicacao_com_resposta
        ),
        'individual_orquestrar_complicacao': _run_individual_orquestrar_complicacao,
    }
