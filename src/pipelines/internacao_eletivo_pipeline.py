from src.pipelines.internacao_eletivo_orquestracao_pipeline import (
    run_internacao_eletivo_pipeline_orquestrar,
)
from src.pipelines.internacao_eletivo_status_pipeline import (
    run_internacao_eletivo_pipeline_gerar_status_dataset,
    run_internacao_eletivo_pipeline_gerar_status_dataset_somente_status,
)
from src.config.paths import DEFAULTS_INTERNACAO_ELETIVO
from core.pipeline_result import ok_result


def run_internacao_eletivo_pipeline(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_eletivo='src/data/status_resposta_eletivo.csv',
    arquivo_status_resposta_internacao='src/data/status_resposta_internacao.csv',
    arquivo_status_resposta_unificado='src/data/status_resposta_eletivo_internacao.csv',
    arquivo_dataset_origem_internacao='src/data/internacao.xlsx',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_eletivo_internacao_limpo.csv',
    saida_status_integrado='src/data/arquivo_limpo/status_internacao_eletivo.csv',
    saida_dataset_status='src/data/arquivo_limpo/internacao_status.xlsx',
    saida_dataset_final='src/data/arquivo_limpo/internacao_final.xlsx',
):
    resultado_status = run_internacao_eletivo_pipeline_gerar_status_dataset(
        arquivo_status=arquivo_status,
        arquivo_status_resposta_eletivo=arquivo_status_resposta_eletivo,
        arquivo_status_resposta_internacao=arquivo_status_resposta_internacao,
        arquivo_status_resposta_unificado=arquivo_status_resposta_unificado,
        arquivo_dataset_origem_internacao=arquivo_dataset_origem_internacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        saida_status_integrado=saida_status_integrado,
        saida_dataset_status=saida_dataset_status,
    )
    if not resultado_status.get('ok'):
        return resultado_status

    resultado_orquestracao = run_internacao_eletivo_pipeline_orquestrar(
        arquivo_dataset_status=saida_dataset_status,
        arquivo_saida_final=saida_dataset_final,
        nome_logger='orquestracao_internacao_eletivo',
    )
    if not resultado_orquestracao.get('ok'):
        return resultado_orquestracao

    return ok_result(
        mensagens=(
            resultado_status.get('mensagens', [])
            + resultado_orquestracao.get('mensagens', [])
        ),
        metricas={
            'total_status': resultado_status.get('total_status', 0),
            'com_match': resultado_status.get('com_match', 0),
            'sem_match': resultado_status.get('sem_match', 0),
            'total_linhas': resultado_status.get('total_linhas', 0),
        },
        arquivos={
            'arquivo_status_dataset': resultado_status.get('arquivo_status_dataset'),
            'arquivo_saida': resultado_orquestracao.get('arquivo_saida'),
        },
    )


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
        saida_dataset_status=DEFAULTS_INTERNACAO_ELETIVO['saida_dataset_status'],
        saida_dataset_final=DEFAULTS_INTERNACAO_ELETIVO['saida_dataset_final'],
    )


def run_pipeline_internacao_eletivo_somente_status():
    resultado_status = run_internacao_eletivo_pipeline_gerar_status_dataset_somente_status(
        arquivo_status=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status'],
        arquivo_origem_dataset=DEFAULTS_INTERNACAO_ELETIVO['arquivo_dataset_origem_internacao'],
        saida_status=DEFAULTS_INTERNACAO_ELETIVO['saida_status'],
        saida_status_integrado=DEFAULTS_INTERNACAO_ELETIVO['saida_status_integrado'],
        saida_dataset_status=DEFAULTS_INTERNACAO_ELETIVO['saida_dataset_status'],
    )
    if not resultado_status.get('ok'):
        return resultado_status

    resultado_orquestracao = run_internacao_eletivo_pipeline_orquestrar(
        arquivo_dataset_status=DEFAULTS_INTERNACAO_ELETIVO['saida_dataset_status'],
        arquivo_saida_final=DEFAULTS_INTERNACAO_ELETIVO['saida_dataset_final'],
        nome_logger='orquestracao_internacao_eletivo_somente_status',
    )
    if not resultado_orquestracao.get('ok'):
        return resultado_orquestracao

    return ok_result(
        mensagens=(
            resultado_status.get('mensagens', [])
            + resultado_orquestracao.get('mensagens', [])
        ),
        metricas={
            'total_status': resultado_status.get('total_status', 0),
            'com_match': resultado_status.get('com_match', 0),
            'sem_match': resultado_status.get('sem_match', 0),
            'total_linhas': resultado_status.get('total_linhas', 0),
        },
        arquivos={'arquivo_saida': resultado_orquestracao.get('arquivo_saida')},
    )


def run_pipeline_internacao_eletivo_orquestracao():
    return run_internacao_eletivo_pipeline_orquestrar(
        arquivo_dataset_status=DEFAULTS_INTERNACAO_ELETIVO['saida_dataset_status'],
        arquivo_saida_final=DEFAULTS_INTERNACAO_ELETIVO['saida_dataset_final'],
        nome_logger='orquestracao_internacao_eletivo',
    )


def run_pipeline_internacao_eletivo():
    return run_pipeline_internacao_eletivo_com_resposta()
