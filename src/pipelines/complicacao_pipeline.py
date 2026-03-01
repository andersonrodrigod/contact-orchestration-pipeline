from src.pipelines.complicacao_orquestracao_pipeline import (
    run_complicacao_pipeline_finalizar,
)
from src.pipelines.complicacao_status_pipeline import (
    run_complicacao_pipeline_gerar_status_dataset,
    run_complicacao_pipeline_gerar_status_dataset_somente_status,
)
from src.config.paths import DEFAULTS_COMPLICACAO
from core.pipeline_result import ok_result


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
    resultado_status = run_complicacao_pipeline_gerar_status_dataset(
        arquivo_status=arquivo_status,
        arquivo_status_resposta_complicacao=arquivo_status_resposta_complicacao,
        arquivo_dataset_origem_complicacao=arquivo_dataset_origem_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        saida_status_integrado=saida_status_integrado,
        saida_dataset_status=saida_dataset_status,
    )
    if not resultado_status.get('ok'):
        return resultado_status

    resultado_finalizacao = run_complicacao_pipeline_finalizar(
        arquivo_dataset_status=saida_dataset_status,
        arquivo_saida_final=saida_dataset_final,
        nome_logger='finalizacao_complicacao',
    )
    if not resultado_finalizacao.get('ok'):
        return resultado_finalizacao

    return ok_result(
        mensagens=(
            resultado_status.get('mensagens', [])
            + resultado_finalizacao.get('mensagens', [])
        ),
        metricas={
            'total_status': resultado_status.get('total_status', 0),
            'com_match': resultado_status.get('com_match', 0),
            'sem_match': resultado_status.get('sem_match', 0),
            'total_linhas': resultado_status.get('total_linhas', 0),
        },
        arquivos={
            'arquivo_status_dataset': resultado_status.get('arquivo_status_dataset'),
            'arquivo_saida': resultado_finalizacao.get('arquivo_saida'),
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
    resultado_status = run_complicacao_pipeline_gerar_status_dataset_somente_status(
        arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
        arquivo_origem_dataset=DEFAULTS_COMPLICACAO['arquivo_dataset_origem_complicacao'],
        saida_status=DEFAULTS_COMPLICACAO['saida_status'],
        saida_status_integrado=DEFAULTS_COMPLICACAO['saida_status_integrado'],
        saida_dataset_status=DEFAULTS_COMPLICACAO['saida_dataset_status'],
    )
    if not resultado_status.get('ok'):
        return resultado_status

    resultado_finalizacao = run_complicacao_pipeline_finalizar(
        arquivo_dataset_status=DEFAULTS_COMPLICACAO['saida_dataset_status'],
        arquivo_saida_final=DEFAULTS_COMPLICACAO['saida_dataset_final'],
        nome_logger='finalizacao_complicacao_somente_status',
    )
    if not resultado_finalizacao.get('ok'):
        return resultado_finalizacao

    return ok_result(
        mensagens=(
            resultado_status.get('mensagens', [])
            + resultado_finalizacao.get('mensagens', [])
        ),
        metricas={
            'total_status': resultado_status.get('total_status', 0),
            'com_match': resultado_status.get('com_match', 0),
            'sem_match': resultado_status.get('sem_match', 0),
            'total_linhas': resultado_status.get('total_linhas', 0),
        },
        arquivos={'arquivo_saida': resultado_finalizacao.get('arquivo_saida')},
    )


def run_pipeline_complicacao():
    return run_pipeline_complicacao_com_resposta()
