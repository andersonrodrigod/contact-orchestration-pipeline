from src.contexts.pipeline_contextos import CONTEXTO_PIPELINE_COMPLICACAO
from src.pipelines.complicacao_orquestracao_pipeline import (
    run_complicacao_pipeline_orquestrar,
)
from src.pipelines.complicacao_status_pipeline import (
    run_complicacao_pipeline_gerar_status_dataset,
)
from core.pipeline_result import ok_result

DEFAULTS_COMPLICACAO = CONTEXTO_PIPELINE_COMPLICACAO.defaults


def run_complicacao_pipeline(
    arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
    arquivo_status_resposta_complicacao=DEFAULTS_COMPLICACAO[
        'arquivo_status_resposta_complicacao'
    ],
    arquivo_dataset_origem_complicacao=DEFAULTS_COMPLICACAO[
        'arquivo_dataset_origem_complicacao'
    ],
    saida_status=DEFAULTS_COMPLICACAO['saida_status'],
    saida_status_resposta=DEFAULTS_COMPLICACAO['saida_status_resposta'],
    saida_status_integrado=DEFAULTS_COMPLICACAO['saida_status_integrado'],
    saida_dataset_status=DEFAULTS_COMPLICACAO['saida_dataset_status'],
    saida_dataset_final=DEFAULTS_COMPLICACAO['saida_dataset_final'],
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

    resultado_orquestracao = run_complicacao_pipeline_orquestrar(
        arquivo_dataset_status=saida_dataset_status,
        arquivo_saida_final=saida_dataset_final,
        nome_logger=CONTEXTO_PIPELINE_COMPLICACAO.logger_orquestracao,
    )
    if not resultado_orquestracao.get('ok'):
        return resultado_orquestracao

    return ok_result(
        mensagens=(
            resultado_status.get('mensagens', [])
            + resultado_orquestracao.get('mensagens', [])
        ),
        arquivos={
            'arquivo_status_dataset': resultado_status.get('arquivo_status_dataset'),
            'arquivo_saida': resultado_orquestracao.get('arquivo_saida'),
        },
    )


def run_pipeline_complicacao_com_resposta():
    return run_complicacao_pipeline(
        arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
        arquivo_status_resposta_complicacao=DEFAULTS_COMPLICACAO[
            'arquivo_status_resposta_complicacao'
        ],
        arquivo_dataset_origem_complicacao=DEFAULTS_COMPLICACAO[
            'arquivo_dataset_origem_complicacao'
        ],
        saida_status=DEFAULTS_COMPLICACAO['saida_status'],
        saida_status_resposta=DEFAULTS_COMPLICACAO['saida_status_resposta'],
        saida_status_integrado=DEFAULTS_COMPLICACAO['saida_status_integrado'],
        saida_dataset_status=DEFAULTS_COMPLICACAO['saida_dataset_status'],
        saida_dataset_final=DEFAULTS_COMPLICACAO['saida_dataset_final'],
    )


def run_pipeline_complicacao_orquestracao():
    return run_complicacao_pipeline_orquestrar(
        arquivo_dataset_status=DEFAULTS_COMPLICACAO['saida_dataset_status'],
        arquivo_saida_final=DEFAULTS_COMPLICACAO['saida_dataset_final'],
        nome_logger=CONTEXTO_PIPELINE_COMPLICACAO.logger_orquestracao,
    )


def run_pipeline_complicacao():
    return run_pipeline_complicacao_com_resposta()
