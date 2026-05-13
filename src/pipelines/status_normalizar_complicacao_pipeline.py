from core.logger import PipelineLogger


def run_status_normalizar_complicacao_pipeline(
    arquivo_status='src/data/status.csv',
    arquivo_status_normalizado='src/data/arquivo_limpo/status_complicacao_limpo.csv',
    arquivo_saida='src/data/arquivo_limpo/status_sem_complicacao.csv',
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
