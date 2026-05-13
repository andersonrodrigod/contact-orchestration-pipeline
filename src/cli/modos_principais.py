from src.pipelines.complicacao_pipeline import (
    run_pipeline_complicacao_orquestracao,
    run_complicacao_pipeline_gerar_status_dataset,
    run_pipeline_complicacao_com_resposta,
)
from src.pipelines.preflight_pipeline import run_preflight_complicacao


MODOS_PRINCIPAIS = {
    'complicacao_com_resposta': run_pipeline_complicacao_com_resposta,
    'complicacao': run_pipeline_complicacao_com_resposta,
    'complicacao_gerar_status_dataset': run_complicacao_pipeline_gerar_status_dataset,
    'complicacao_orquestracao': run_pipeline_complicacao_orquestracao,
    'preflight_complicacao': run_preflight_complicacao,
}

MODOS_AGREGADOS = []
