from src.contexts.pipeline_contextos import CONTEXTO_PIPELINE_COMPLICACAO
from src.pipelines.orquestracao_base_pipeline import executar_orquestracao_pipeline

DEFAULTS_COMPLICACAO = CONTEXTO_PIPELINE_COMPLICACAO.defaults


def run_complicacao_pipeline_orquestrar(
    arquivo_dataset_status=DEFAULTS_COMPLICACAO['saida_dataset_status'],
    arquivo_saida_final=DEFAULTS_COMPLICACAO['saida_dataset_final'],
    nome_logger=CONTEXTO_PIPELINE_COMPLICACAO.logger_orquestracao,
):
    return executar_orquestracao_pipeline(
        arquivo_dataset_entrada=arquivo_dataset_status,
        arquivo_dataset_saida=arquivo_saida_final,
        nome_logger=nome_logger,
    )
