from src.pipelines.ingestao_pipeline import run_ingestao_somente_status, run_ingestao_unificar
from src.pipelines.criacao_dataset_pipeline import run_criacao_dataset_pipeline
from src.pipelines.join_status_resposta_pipeline import (
    run_status_somente_internacao_eletivo_pipeline,
    run_unificar_status_resposta_internacao_eletivo_pipeline,
)


DEFAULTS_INTERNACAO_ELETIVO = {
    'arquivo_status': 'src/data/status.csv',
    'arquivo_status_resposta_eletivo': 'src/data/status_respostas_eletivo.csv',
    'arquivo_status_resposta_internacao': 'src/data/status_resposta_internacao.csv',
    'arquivo_status_resposta_unificado': 'src/data/status_resposta_eletivo_internacao.csv',
    'arquivo_dataset_origem_internacao': 'src/data/internacao.xlsx',
    'saida_status': 'src/data/arquivo_limpo/status_internacao_eletivo_limpo.csv',
    'saida_status_resposta': 'src/data/arquivo_limpo/status_resposta_eletivo_internacao_limpo.csv',
    'saida_status_integrado': 'src/data/arquivo_limpo/status_internacao_eletivo.csv',
    'saida_dataset': 'src/data/arquivo_limpo/dataset_internacao_eletivo.xlsx',
}


def run_internacao_eletivo_pipeline(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_eletivo='src/data/status_respostas_eletivo.csv',
    arquivo_status_resposta_internacao='src/data/status_resposta_internacao.csv',
    arquivo_status_resposta_unificado='src/data/status_resposta_eletivo_internacao.csv',
    arquivo_dataset_origem_internacao='src/data/internacao.xlsx',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_eletivo_internacao_limpo.csv',
    saida_status_integrado='src/data/arquivo_limpo/status_internacao_eletivo.csv',
    saida_dataset='src/data/arquivo_limpo/dataset_internacao_eletivo.xlsx',
):
    resultado_ingestao = run_ingestao_unificar(
        arquivo_status=arquivo_status,
        arquivo_status_resposta_eletivo=arquivo_status_resposta_eletivo,
        arquivo_status_resposta_internacao=arquivo_status_resposta_internacao,
        arquivo_status_resposta_unificado=arquivo_status_resposta_unificado,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
    )
    if not resultado_ingestao.get('ok'):
        return resultado_ingestao

    resultado_integracao = run_unificar_status_resposta_internacao_eletivo_pipeline(
        arquivo_status=saida_status,
        arquivo_status_resposta=saida_status_resposta,
        arquivo_saida=saida_status_integrado,
    )
    if not resultado_integracao.get('ok'):
        return resultado_integracao

    resultado_dataset = run_criacao_dataset_pipeline(
        arquivo_origem_dataset=arquivo_dataset_origem_internacao,
        arquivo_saida_dataset=saida_dataset,
        nome_logger='criacao_dataset_internacao_eletivo',
    )
    if not resultado_dataset.get('ok'):
        return resultado_dataset

    return {
        **resultado_integracao,
        **resultado_dataset,
        'arquivo_saida': resultado_dataset.get('arquivo_saida'),
    }


def run_pipeline_internacao_eletivo_com_resposta():
    return run_internacao_eletivo_pipeline(
        arquivo_status=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status'],
        arquivo_status_resposta_eletivo=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_eletivo'],
        arquivo_status_resposta_internacao=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_internacao'],
        arquivo_status_resposta_unificado=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_unificado'],
        arquivo_dataset_origem_internacao=DEFAULTS_INTERNACAO_ELETIVO['arquivo_dataset_origem_internacao'],
        saida_status=DEFAULTS_INTERNACAO_ELETIVO['saida_status'],
        saida_status_resposta=DEFAULTS_INTERNACAO_ELETIVO['saida_status_resposta'],
        saida_status_integrado=DEFAULTS_INTERNACAO_ELETIVO['saida_status_integrado'],
        saida_dataset=DEFAULTS_INTERNACAO_ELETIVO['saida_dataset'],
    )


def run_pipeline_internacao_eletivo_somente_status():
    resultado_ingestao = run_ingestao_somente_status(
        arquivo_status=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status'],
        saida_status=DEFAULTS_INTERNACAO_ELETIVO['saida_status'],
        nome_logger='ingestao_internacao_eletivo_somente_status',
    )
    if not resultado_ingestao.get('ok'):
        return resultado_ingestao

    resultado_status = run_status_somente_internacao_eletivo_pipeline(
        arquivo_status=DEFAULTS_INTERNACAO_ELETIVO['saida_status'],
        arquivo_saida=DEFAULTS_INTERNACAO_ELETIVO['saida_status_integrado'],
    )
    if not resultado_status.get('ok'):
        return resultado_status

    resultado_dataset = run_criacao_dataset_pipeline(
        arquivo_origem_dataset=DEFAULTS_INTERNACAO_ELETIVO['arquivo_dataset_origem_internacao'],
        arquivo_saida_dataset=DEFAULTS_INTERNACAO_ELETIVO['saida_dataset'],
        nome_logger='criacao_dataset_internacao_eletivo_somente_status',
    )
    if not resultado_dataset.get('ok'):
        return resultado_dataset

    return {
        **resultado_status,
        **resultado_dataset,
        'arquivo_saida': resultado_dataset.get('arquivo_saida'),
    }


def run_pipeline_internacao_eletivo():
    return run_pipeline_internacao_eletivo_com_resposta()
