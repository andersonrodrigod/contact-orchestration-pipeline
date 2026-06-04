from src.pipelines.complicacao_pipeline import (
    run_pipeline_complicacao_orquestracao,
    run_complicacao_pipeline_gerar_status_dataset,
    run_pipeline_complicacao_com_resposta,
)
from src.cli.modos_individuais import obter_aliases_modos_etapas, obter_modos_etapas
from src.pipelines.preflight_pipeline import run_preflight_complicacao


MODOS_PRINCIPAIS = {
    'complicacao_com_resposta': run_pipeline_complicacao_com_resposta,
    'complicacao': run_pipeline_complicacao_com_resposta,
    'complicacao_gerar_status_dataset': run_complicacao_pipeline_gerar_status_dataset,
    'complicacao_orquestracao': run_pipeline_complicacao_orquestracao,
    'preflight_complicacao': run_preflight_complicacao,
}

MODOS_AGREGADOS = []


def obter_registro_modos(incluir_aliases=True):
    modos_etapas = obter_modos_etapas()
    modo_funcao = {
        **MODOS_PRINCIPAIS,
        **modos_etapas,
    }
    if incluir_aliases:
        modo_funcao.update(obter_aliases_modos_etapas(modos_etapas))
    return modo_funcao


def obter_escolhas_modo():
    return list(obter_registro_modos().keys()) + MODOS_AGREGADOS
