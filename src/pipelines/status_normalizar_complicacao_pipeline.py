from core.logger import PipelineLogger
from src.contexts.pipeline_contextos import CONTEXTO_PIPELINE_COMPLICACAO

DEFAULTS_COMPLICACAO = CONTEXTO_PIPELINE_COMPLICACAO.defaults


def run_status_normalizar_complicacao_pipeline(
    arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
    arquivo_status_normalizado=DEFAULTS_COMPLICACAO['saida_status'],
    arquivo_saida=DEFAULTS_COMPLICACAO['saida_status_sem_complicacao'],
    nome_logger='ingestao_individual_excluir_complicacao',
):
    logger = PipelineLogger(nome_pipeline=nome_logger)
    logger.warning(
        'MODO_REMOVIDO',
        'Normalizacao somente status sera recriada como etapa/CLI propria.',
    )
    logger.finalizar('REMOVIDO')
    return {
        'ok': False,
        'mensagens': [
            'Normalizacao somente status removida temporariamente. '
            'Recriar como CLI/etapa propria no plano de refatoracao.'
        ],
    }
