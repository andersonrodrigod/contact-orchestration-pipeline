from src.pipelines.complicacao_orquestracao_pipeline import (
    run_complicacao_pipeline_orquestrar,
)
from src.pipelines.complicacao_status_pipeline import (
    run_complicacao_pipeline_gerar_status_dataset,
    run_complicacao_pipeline_gerar_status_dataset_somente_status,
)
from src.pipelines.contexto_pipeline_core import (
    run_pipeline_contexto_com_resposta,
    run_pipeline_contexto_somente_status,
)
from src.config.paths import DEFAULTS_COMPLICACAO


def run_complicacao_pipeline(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_complicacao='src/data/status_resposta_complicacao.csv',
    arquivo_dataset_origem_complicacao='src/data/complicacao.xlsx',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    saida_status_integrado='src/data/arquivo_limpo/status_complicacao.csv',
    saida_dataset_status='src/data/arquivo_limpo/complicacao_status.xlsx',
    saida_dataset_final='src/data/arquivo_limpo/complicacao_final.xlsx',
):
    return run_pipeline_contexto_com_resposta(
        funcao_status_dataset=run_complicacao_pipeline_gerar_status_dataset,
        kwargs_status_dataset={
            'arquivo_status': arquivo_status,
            'arquivo_status_resposta_complicacao': arquivo_status_resposta_complicacao,
            'arquivo_dataset_origem_complicacao': arquivo_dataset_origem_complicacao,
            'saida_status': saida_status,
            'saida_status_resposta': saida_status_resposta,
            'saida_status_integrado': saida_status_integrado,
            'saida_dataset_status': saida_dataset_status,
        },
        funcao_orquestracao=run_complicacao_pipeline_orquestrar,
        kwargs_orquestracao={
            'arquivo_dataset_status': saida_dataset_status,
            'arquivo_saida_final': saida_dataset_final,
            'nome_logger': 'orquestracao_complicacao',
        },
    )


def run_pipeline_complicacao_com_resposta():
    return run_complicacao_pipeline(
        arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
        arquivo_status_resposta_complicacao=DEFAULTS_COMPLICACAO['arquivo_status_resposta_complicacao'],
        arquivo_dataset_origem_complicacao=DEFAULTS_COMPLICACAO['arquivo_dataset_origem_complicacao'],
        saida_status=DEFAULTS_COMPLICACAO['saida_status'],
        saida_status_resposta=DEFAULTS_COMPLICACAO['saida_status_resposta'],
        saida_status_integrado=DEFAULTS_COMPLICACAO['saida_status_integrado'],
        saida_dataset_status=DEFAULTS_COMPLICACAO['saida_dataset_status'],
        saida_dataset_final=DEFAULTS_COMPLICACAO['saida_dataset_final'],
    )


def run_pipeline_complicacao_somente_status():
    return run_pipeline_contexto_somente_status(
        funcao_status_dataset=run_complicacao_pipeline_gerar_status_dataset_somente_status,
        kwargs_status_dataset={
            'arquivo_status': DEFAULTS_COMPLICACAO['arquivo_status'],
            'arquivo_origem_dataset': DEFAULTS_COMPLICACAO['arquivo_dataset_origem_complicacao'],
            'saida_status': DEFAULTS_COMPLICACAO['saida_status'],
            'saida_status_integrado': DEFAULTS_COMPLICACAO['saida_status_integrado'],
            'saida_dataset_status': DEFAULTS_COMPLICACAO['saida_dataset_status'],
        },
        funcao_orquestracao=run_complicacao_pipeline_orquestrar,
        kwargs_orquestracao={
            'arquivo_dataset_status': DEFAULTS_COMPLICACAO['saida_dataset_status'],
            'arquivo_saida_final': DEFAULTS_COMPLICACAO['saida_dataset_final'],
            'nome_logger': 'orquestracao_complicacao_somente_status',
        },
    )


def run_pipeline_complicacao_orquestracao():
    return run_complicacao_pipeline_orquestrar(
        arquivo_dataset_status=DEFAULTS_COMPLICACAO['saida_dataset_status'],
        arquivo_saida_final=DEFAULTS_COMPLICACAO['saida_dataset_final'],
        nome_logger='orquestracao_complicacao',
    )


def run_pipeline_complicacao():
    return run_pipeline_complicacao_com_resposta()
