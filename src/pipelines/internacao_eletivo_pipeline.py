from src.pipelines.internacao_eletivo_orquestracao_pipeline import (
    run_internacao_eletivo_pipeline_orquestrar,
)
from src.pipelines.internacao_eletivo_status_pipeline import (
    run_internacao_eletivo_pipeline_gerar_status_dataset,
    run_internacao_eletivo_pipeline_gerar_status_dataset_somente_status,
)
from src.contexts.pipeline_contextos import CONTEXTO_PIPELINE_INTERNACAO_ELETIVO
from src.pipelines.contexto_pipeline_core import (
    run_pipeline_contexto_com_resposta,
    run_pipeline_contexto_somente_status,
)


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
    return run_pipeline_contexto_com_resposta(
        funcao_status_dataset=run_internacao_eletivo_pipeline_gerar_status_dataset,
        kwargs_status_dataset={
            'arquivo_status': arquivo_status,
            'arquivo_status_resposta_eletivo': arquivo_status_resposta_eletivo,
            'arquivo_status_resposta_internacao': arquivo_status_resposta_internacao,
            'arquivo_status_resposta_unificado': arquivo_status_resposta_unificado,
            'arquivo_dataset_origem_internacao': arquivo_dataset_origem_internacao,
            'saida_status': saida_status,
            'saida_status_resposta': saida_status_resposta,
            'saida_status_integrado': saida_status_integrado,
            'saida_dataset_status': saida_dataset_status,
        },
        funcao_orquestracao=run_internacao_eletivo_pipeline_orquestrar,
        kwargs_orquestracao={
            'arquivo_dataset_status': saida_dataset_status,
            'arquivo_saida_final': saida_dataset_final,
            'nome_logger': 'orquestracao_internacao_eletivo',
        },
    )


def run_pipeline_internacao_eletivo_com_resposta():
    defaults = CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults
    return run_internacao_eletivo_pipeline(
        arquivo_status=defaults['arquivo_status'],
        arquivo_status_resposta_eletivo=defaults['arquivo_status_resposta_eletivo'],
        arquivo_status_resposta_internacao=defaults['arquivo_status_resposta_internacao'],
        arquivo_status_resposta_unificado=defaults['arquivo_status_resposta_unificado'],
        arquivo_dataset_origem_internacao=defaults['arquivo_dataset_origem_internacao'],
        saida_status=defaults['saida_status'],
        saida_status_resposta=defaults['saida_status_resposta'],
        saida_status_integrado=defaults['saida_status_integrado'],
        saida_dataset_status=defaults['saida_dataset_status'],
        saida_dataset_final=defaults['saida_dataset_final'],
    )


def run_pipeline_internacao_eletivo_somente_status():
    defaults = CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults
    return run_pipeline_contexto_somente_status(
        funcao_status_dataset=run_internacao_eletivo_pipeline_gerar_status_dataset_somente_status,
        kwargs_status_dataset={
            'arquivo_status': defaults['arquivo_status'],
            'arquivo_origem_dataset': defaults['arquivo_dataset_origem_internacao'],
            'saida_status': defaults['saida_status'],
            'saida_status_integrado': defaults['saida_status_integrado'],
            'saida_dataset_status': defaults['saida_dataset_status'],
        },
        funcao_orquestracao=run_internacao_eletivo_pipeline_orquestrar,
        kwargs_orquestracao={
            'arquivo_dataset_status': defaults['saida_dataset_status'],
            'arquivo_saida_final': defaults['saida_dataset_final'],
            'nome_logger': CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.logger_orquestracao_somente_status,
        },
    )


def run_pipeline_internacao_eletivo_orquestracao():
    defaults = CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults
    return run_internacao_eletivo_pipeline_orquestrar(
        arquivo_dataset_status=defaults['saida_dataset_status'],
        arquivo_saida_final=defaults['saida_dataset_final'],
        nome_logger=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.logger_orquestracao,
    )


def run_pipeline_internacao_eletivo():
    return run_pipeline_internacao_eletivo_com_resposta()
