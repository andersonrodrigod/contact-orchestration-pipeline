from src.pipelines.complicacao_pipeline import (
    run_pipeline_complicacao_orquestracao,
    run_complicacao_pipeline_gerar_status_dataset,
    run_pipeline_complicacao_com_resposta,
)
from src.cli.modos_individuais import obter_modos_etapas
from src.pipelines.preflight_pipeline import run_preflight_complicacao


MODOS_PRINCIPAIS = {
    'complicacao_com_resposta': run_pipeline_complicacao_com_resposta,
    'complicacao': run_pipeline_complicacao_com_resposta,
    'complicacao_gerar_status_dataset': run_complicacao_pipeline_gerar_status_dataset,
    'complicacao_orquestracao': run_pipeline_complicacao_orquestracao,
    'preflight_complicacao': run_preflight_complicacao,
}

MODOS_AGREGADOS = []


def obter_registro_modos():
    modos_etapas = obter_modos_etapas()
    return {
        **MODOS_PRINCIPAIS,
        **modos_etapas,
    }


def obter_escolhas_modo():
    return list(obter_registro_modos().keys()) + MODOS_AGREGADOS
