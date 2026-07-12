from pathlib import Path

from core.logger import PipelineLogger
from core.pipeline_result import ok_result
from src.contexts.pipeline_contextos import CONTEXTO_PIPELINE_COMPLICACAO
from src.pipelines.contexto_status_pipeline_base import (
    caminho_xlsx_pareado,
    run_criacao_dataset_status_base,
)
from src.pipelines.join_status_resposta_pipeline import (
    run_unificar_status_resposta_complicacao_pipeline,
)
from src.services.dataset_service import criar_dataset_complicacao
from src.services.ingestao_service import executar_ingestao_complicacao


def run_complicacao_pipeline_enviar_status_com_resposta(
    arquivo_status=CONTEXTO_PIPELINE_COMPLICACAO.defaults['arquivo_status'],
    arquivo_status_resposta_complicacao=CONTEXTO_PIPELINE_COMPLICACAO.defaults[
        'arquivo_status_resposta_complicacao'
    ],
    saida_status=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_status'],
    saida_status_resposta=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_status_resposta'],
    saida_status_integrado=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_status_integrado'],
    executar_xlsx_adicional=False,
    logger=None,
):
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(nome_pipeline=CONTEXTO_PIPELINE_COMPLICACAO.logger_status_com_resposta)

    resultado_ingestao = executar_ingestao_complicacao(
        arquivo_status=arquivo_status,
        arquivo_status_resposta_complicacao=arquivo_status_resposta_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        executar_xlsx_adicional=executar_xlsx_adicional,
        logger=logger,
    )
    if not resultado_ingestao.get('ok'):
        if not logger_externo:
            logger.finalizar('FALHA_INGESTAO')
        return resultado_ingestao

    resultado_integracao = run_unificar_status_resposta_complicacao_pipeline(
        arquivo_status=saida_status,
        arquivo_status_resposta=saida_status_resposta,
        arquivo_saida=saida_status_integrado,
        logger=logger,
    )
    if not resultado_integracao.get('ok'):
        if not logger_externo:
            logger.finalizar('FALHA_INTEGRACAO')
        return resultado_integracao

    arquivo_status_xlsx = caminho_xlsx_pareado(saida_status)
    arquivo_resposta_xlsx = caminho_xlsx_pareado(saida_status_resposta)
    if executar_xlsx_adicional and Path(arquivo_status_xlsx).exists() and Path(arquivo_resposta_xlsx).exists():
        arquivo_saida_xlsx = caminho_xlsx_pareado(saida_status_integrado)
        logger.info('MODO_XLSX', 'Execucao adicional XLSX iniciada (integracao status + resposta).')
        resultado_integracao_xlsx = run_unificar_status_resposta_complicacao_pipeline(
            arquivo_status=arquivo_status_xlsx,
            arquivo_status_resposta=arquivo_resposta_xlsx,
            arquivo_saida=arquivo_saida_xlsx,
            logger=logger,
        )
        if resultado_integracao_xlsx.get('ok'):
            logger.info('MODO_XLSX', 'Integracao adicional XLSX finalizada com sucesso.')
            resultado_integracao['mensagens'] = resultado_integracao.get('mensagens', []) + [
                f'Saida XLSX gerada: {arquivo_saida_xlsx}',
            ]
        else:
            logger.warning('MODO_XLSX', 'Falha na integracao adicional XLSX; fluxo CSV foi mantido.')
            resultado_integracao['mensagens'] = resultado_integracao.get('mensagens', []) + [
                'Aviso: falha na execucao adicional XLSX durante integracao de status.',
            ]
    elif executar_xlsx_adicional:
        logger.info('MODO_XLSX', 'Arquivos limpos XLSX nao encontrados para integracao adicional.')

    resultado = ok_result(
        mensagens=resultado_integracao.get('mensagens', []),
        arquivos={'arquivo_status_integrado': resultado_integracao.get('arquivo_saida')},
    )
    if not logger_externo:
        logger.finalizar('SUCESSO')
    return resultado


def run_complicacao_pipeline_gerar_status_dataset(
    arquivo_status=CONTEXTO_PIPELINE_COMPLICACAO.defaults['arquivo_status'],
    arquivo_status_resposta_complicacao=CONTEXTO_PIPELINE_COMPLICACAO.defaults[
        'arquivo_status_resposta_complicacao'
    ],
    arquivo_dataset_origem_complicacao=CONTEXTO_PIPELINE_COMPLICACAO.defaults[
        'arquivo_dataset_origem_complicacao'
    ],
    saida_status=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_status'],
    saida_status_resposta=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_status_resposta'],
    saida_status_integrado=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_status_integrado'],
    saida_dataset_status=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_dataset_status'],
):
    logger = PipelineLogger(nome_pipeline=CONTEXTO_PIPELINE_COMPLICACAO.logger_status_com_resposta)
    resultado_status = run_complicacao_pipeline_enviar_status_com_resposta(
        arquivo_status=arquivo_status,
        arquivo_status_resposta_complicacao=arquivo_status_resposta_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        saida_status_integrado=saida_status_integrado,
        logger=logger,
    )
    if not resultado_status.get('ok'):
        logger.finalizar('FALHA')
        return resultado_status

    resultado_dataset = run_complicacao_pipeline_criar_dataset_status(
        arquivo_origem_dataset=arquivo_dataset_origem_complicacao,
        arquivo_status_integrado=saida_status_integrado,
        arquivo_saida_dataset=saida_dataset_status,
        nome_logger=CONTEXTO_PIPELINE_COMPLICACAO.logger_criacao_dataset,
        contexto=CONTEXTO_PIPELINE_COMPLICACAO.nome,
        logger=logger,
        finalizar_logger=False,
    )
    if not resultado_dataset.get('ok'):
        logger.finalizar('FALHA')
        return resultado_dataset

    resultado = ok_result(
        mensagens=resultado_status.get('mensagens', []) + resultado_dataset.get('mensagens', []),
        arquivos={'arquivo_status_dataset': resultado_dataset.get('arquivo_saida')},
    )
    logger.finalizar('SUCESSO')
    return resultado


def run_complicacao_pipeline_criar_dataset_status(
    arquivo_origem_dataset=CONTEXTO_PIPELINE_COMPLICACAO.defaults['arquivo_dataset_origem_complicacao'],
    arquivo_status_integrado=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_status_integrado'],
    arquivo_saida_dataset=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_dataset_status'],
    nome_logger=CONTEXTO_PIPELINE_COMPLICACAO.logger_criacao_dataset,
    contexto=CONTEXTO_PIPELINE_COMPLICACAO.nome,
    logger=None,
    finalizar_logger=True,
):
    return run_criacao_dataset_status_base(
        arquivo_origem_dataset=arquivo_origem_dataset,
        arquivo_status_integrado=arquivo_status_integrado,
        arquivo_saida_dataset=arquivo_saida_dataset,
        criar_dataset_fn=criar_dataset_complicacao,
        nome_logger=nome_logger,
        contexto=contexto,
        logger=logger,
        finalizar_logger=finalizar_logger,
    )
