from __future__ import annotations

from core.logger import PipelineLogger
from src.contexts.pipeline_contextos import (
    CONTEXTO_PIPELINE_COMPLICACAO,
    CONTEXTO_PIPELINE_INTERNACAO_ELETIVO,
)
from src.services.dataset_service import (
    aplicar_status_integrado_em_dataset,
    criar_dataset_base_complicacao,
)


def _run_with_logger(nome_pipeline: str, fn, **kwargs):
    logger = PipelineLogger(nome_pipeline=nome_pipeline)
    try:
        resultado = fn(**kwargs)
        if not isinstance(resultado, dict):
            resultado = {
                'ok': False,
                'mensagens': [f'Retorno invalido: {type(resultado).__name__}'],
            }
        logger.finalizar('SUCESSO' if resultado.get('ok') else 'FALHA')
        return resultado
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        logger.finalizar('ERRO')
        return {
            'ok': False,
            'mensagens': [f'Erro no pipeline {nome_pipeline}: {type(erro).__name__}: {erro}'],
        }


def run_complicacao_pipeline_criar_dataset_base(
    arquivo_origem_dataset=CONTEXTO_PIPELINE_COMPLICACAO.defaults['arquivo_dataset_origem_complicacao'],
    arquivo_saida_dataset=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_dataset_status'],
):
    return _run_with_logger(
        'criacao_dataset_base_complicacao',
        criar_dataset_base_complicacao,
        arquivo_complicacao=arquivo_origem_dataset,
        arquivo_saida_dataset=arquivo_saida_dataset,
        contexto='complicacao',
    )


def run_internacao_eletivo_pipeline_criar_dataset_base(
    arquivo_origem_dataset=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['arquivo_dataset_origem_internacao'],
    arquivo_saida_dataset=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_dataset_status'],
):
    return _run_with_logger(
        'criacao_dataset_base_internacao_eletivo',
        criar_dataset_base_complicacao,
        arquivo_complicacao=arquivo_origem_dataset,
        arquivo_saida_dataset=arquivo_saida_dataset,
        contexto='internacao_eletivo',
    )


def run_complicacao_pipeline_aplicar_status_no_dataset(
    arquivo_dataset_base=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_dataset_status'],
    arquivo_status_integrado=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_status_integrado'],
    arquivo_saida_dataset=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_dataset_status'],
):
    return _run_with_logger(
        'aplicar_status_dataset_complicacao',
        aplicar_status_integrado_em_dataset,
        arquivo_dataset_base=arquivo_dataset_base,
        arquivo_status_integrado=arquivo_status_integrado,
        arquivo_saida_dataset=arquivo_saida_dataset,
        contexto='complicacao',
    )


def run_internacao_eletivo_pipeline_aplicar_status_no_dataset(
    arquivo_dataset_base=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_dataset_status'],
    arquivo_status_integrado=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_status_integrado'],
    arquivo_saida_dataset=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_dataset_status'],
):
    return _run_with_logger(
        'aplicar_status_dataset_internacao_eletivo',
        aplicar_status_integrado_em_dataset,
        arquivo_dataset_base=arquivo_dataset_base,
        arquivo_status_integrado=arquivo_status_integrado,
        arquivo_saida_dataset=arquivo_saida_dataset,
        contexto='internacao_eletivo',
    )
