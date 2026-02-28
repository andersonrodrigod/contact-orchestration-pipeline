from src.pipelines.criacao_dataset_pipeline import run_criacao_dataset_pipeline
from src.pipelines.ingestao_pipeline import run_ingestao_complicacao, run_ingestao_somente_status
from src.pipelines.join_status_resposta_pipeline import (
    run_status_somente_complicacao_pipeline,
    run_unificar_status_resposta_complicacao_pipeline,
)
from src.config.paths import DEFAULTS_COMPLICACAO


def run_complicacao_pipeline(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_complicacao='src/data/status_resposta_complicacao.csv',
    arquivo_dataset_origem_complicacao='src/data/complicacao.xlsx',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    saida_status_integrado='src/data/arquivo_limpo/status_complicacao.csv',
    saida_dataset='src/data/arquivo_limpo/dataset_complicacao.xlsx',
):
    resultado_ingestao = run_ingestao_complicacao(
        arquivo_status=arquivo_status,
        arquivo_status_resposta_complicacao=arquivo_status_resposta_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
    )
    if not resultado_ingestao.get('ok'):
        return resultado_ingestao

    resultado_integracao = run_unificar_status_resposta_complicacao_pipeline(
        arquivo_status=saida_status,
        arquivo_status_resposta=saida_status_resposta,
        arquivo_saida=saida_status_integrado,
    )
    if not resultado_integracao.get('ok'):
        return resultado_integracao

    resultado_dataset = run_criacao_dataset_pipeline(
        arquivo_origem_dataset=arquivo_dataset_origem_complicacao,
        arquivo_saida_dataset=saida_dataset,
        nome_logger='criacao_dataset_complicacao',
    )
    if not resultado_dataset.get('ok'):
        return resultado_dataset

    return {
        **resultado_integracao,
        **resultado_dataset,
        'arquivo_saida': resultado_dataset.get('arquivo_saida'),
    }


def run_pipeline_complicacao_com_resposta():
    return run_complicacao_pipeline(
        arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
        arquivo_status_resposta_complicacao=DEFAULTS_COMPLICACAO['arquivo_status_resposta_complicacao'],
        arquivo_dataset_origem_complicacao=DEFAULTS_COMPLICACAO['arquivo_dataset_origem_complicacao'],
        saida_status=DEFAULTS_COMPLICACAO['saida_status'],
        saida_status_resposta=DEFAULTS_COMPLICACAO['saida_status_resposta'],
        saida_status_integrado=DEFAULTS_COMPLICACAO['saida_status_integrado'],
        saida_dataset=DEFAULTS_COMPLICACAO['saida_dataset'],
    )


def run_pipeline_complicacao_somente_status():
    resultado_ingestao = run_ingestao_somente_status(
        arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
        saida_status=DEFAULTS_COMPLICACAO['saida_status'],
        nome_logger='ingestao_complicacao_somente_status',
    )
    if not resultado_ingestao.get('ok'):
        return resultado_ingestao

    resultado_status = run_status_somente_complicacao_pipeline(
        arquivo_status=DEFAULTS_COMPLICACAO['saida_status'],
        arquivo_saida=DEFAULTS_COMPLICACAO['saida_status_integrado'],
    )
    if not resultado_status.get('ok'):
        return resultado_status

    resultado_dataset = run_criacao_dataset_pipeline(
        arquivo_origem_dataset=DEFAULTS_COMPLICACAO['arquivo_dataset_origem_complicacao'],
        arquivo_saida_dataset=DEFAULTS_COMPLICACAO['saida_dataset'],
        nome_logger='criacao_dataset_complicacao_somente_status',
    )
    if not resultado_dataset.get('ok'):
        return resultado_dataset

    return {
        **resultado_status,
        **resultado_dataset,
        'arquivo_saida': resultado_dataset.get('arquivo_saida'),
    }


def run_pipeline_complicacao():
    return run_pipeline_complicacao_com_resposta()
