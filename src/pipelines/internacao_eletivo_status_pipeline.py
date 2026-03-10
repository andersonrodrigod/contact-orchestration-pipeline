from core.logger import PipelineLogger
from core.pipeline_result import ok_result
from pathlib import Path
from src.contexts.pipeline_contextos import CONTEXTO_PIPELINE_INTERNACAO_ELETIVO
from src.pipelines.contexto_status_pipeline_base import (
    caminho_xlsx_pareado,
    run_criacao_dataset_status_base,
)
from src.pipelines.join_status_resposta_pipeline import (
    run_status_somente_internacao_eletivo_pipeline,
    run_unificar_status_resposta_internacao_eletivo_pipeline,
)
from src.services.dataset_service import criar_dataset_complicacao
from src.services.ingestao_service import executar_ingestao_somente_status, executar_ingestao_unificar


def run_internacao_eletivo_pipeline_enviar_status_com_resposta(
    arquivo_status=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['arquivo_status'],
    arquivo_status_resposta_eletivo=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults[
        'arquivo_status_resposta_eletivo'
    ],
    arquivo_status_resposta_internacao=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults[
        'arquivo_status_resposta_internacao'
    ],
    arquivo_status_resposta_unificado=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults[
        'arquivo_status_resposta_unificado'
    ],
    saida_status=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_status'],
    saida_status_resposta=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_status_resposta'],
    saida_status_integrado=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_status_integrado'],
    executar_xlsx_adicional=False,
    logger=None,
):
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(
            nome_pipeline=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.logger_status_com_resposta
        )
    resultado_ingestao = executar_ingestao_unificar(
        arquivo_status=arquivo_status,
        arquivo_status_resposta_eletivo=arquivo_status_resposta_eletivo,
        arquivo_status_resposta_internacao=arquivo_status_resposta_internacao,
        arquivo_status_resposta_unificado=arquivo_status_resposta_unificado,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        executar_xlsx_adicional=executar_xlsx_adicional,
        logger=logger,
    )
    if not resultado_ingestao.get('ok'):
        if not logger_externo:
            logger.finalizar('FALHA_INGESTAO')
        return resultado_ingestao

    resultado_integracao = run_unificar_status_resposta_internacao_eletivo_pipeline(
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
        logger.info('MODO_XLSX', f'arquivo_status={arquivo_status_xlsx}')
        logger.info('MODO_XLSX', f'arquivo_status_resposta={arquivo_resposta_xlsx}')
        logger.info('MODO_XLSX', f'arquivo_saida={arquivo_saida_xlsx}')
        resultado_integracao_xlsx = run_unificar_status_resposta_internacao_eletivo_pipeline(
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

    metricas_por_etapa = {
        **resultado_ingestao.get('metricas_por_etapa', {}),
        'integracao_status_resposta': {
            'total_status': resultado_integracao.get('total_status', 0),
            'com_match': resultado_integracao.get('com_match', 0),
            'sem_match': resultado_integracao.get('sem_match', 0),
            'descartados_status_data_invalida': resultado_integracao.get(
                'descartados_status_data_invalida', 0
            ),
            'descartados_resposta_data_invalida': resultado_integracao.get(
                'descartados_resposta_data_invalida', 0
            ),
        },
    }
    resultado = ok_result(
        mensagens=resultado_integracao.get('mensagens', []),
        metricas={
            'total_status': resultado_integracao.get('total_status', 0),
            'com_match': resultado_integracao.get('com_match', 0),
            'sem_match': resultado_integracao.get('sem_match', 0),
            'descartados_status_data_invalida': resultado_integracao.get(
                'descartados_status_data_invalida', 0
            ),
            'descartados_resposta_data_invalida': resultado_integracao.get(
                'descartados_resposta_data_invalida', 0
            ),
            'nat_data_agendamento': resultado_ingestao.get('nat_data_agendamento', 0),
            'pct_nat_data_agendamento': resultado_ingestao.get('pct_nat_data_agendamento', 0.0),
            'nat_dt_atendimento': resultado_ingestao.get('nat_dt_atendimento', 0),
            'pct_nat_dt_atendimento': resultado_ingestao.get('pct_nat_dt_atendimento', 0.0),
            'limiar_nat_data_em_uso': resultado_ingestao.get('limiar_nat_data_em_uso'),
        },
        arquivos={'arquivo_status_integrado': resultado_integracao.get('arquivo_saida')},
        dados={
            'qualidade_data': resultado_ingestao.get('qualidade_data', {}),
            'metricas_por_etapa': metricas_por_etapa,
        },
    )
    if not logger_externo:
        logger.finalizar('SUCESSO')
    return resultado


def run_internacao_eletivo_pipeline_enviar_status_somente_status(
    arquivo_status=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['arquivo_status'],
    saida_status=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_status'],
    saida_status_integrado=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_status_integrado'],
    logger=None,
):
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(
            nome_pipeline=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.logger_status_somente_status
        )
    resultado_ingestao = executar_ingestao_somente_status(
        arquivo_status=arquivo_status,
        saida_status=saida_status,
        nome_logger='ingestao_internacao_eletivo_somente_status',
        contexto=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.nome,
        logger=logger,
    )
    if not resultado_ingestao.get('ok'):
        if not logger_externo:
            logger.finalizar('FALHA_INGESTAO')
        return resultado_ingestao

    resultado_status = run_status_somente_internacao_eletivo_pipeline(
        arquivo_status=saida_status,
        arquivo_saida=saida_status_integrado,
        logger=logger,
    )
    if not resultado_status.get('ok'):
        if not logger_externo:
            logger.finalizar('FALHA_INTEGRACAO')
        return resultado_status

    metricas_por_etapa = {
        **resultado_ingestao.get('metricas_por_etapa', {}),
        'integracao_status_resposta': {
            'total_status': resultado_status.get('total_status', 0),
            'com_match': resultado_status.get('com_match', 0),
            'sem_match': resultado_status.get('sem_match', 0),
            'descartados_status_data_invalida': resultado_status.get(
                'descartados_status_data_invalida', 0
            ),
            'descartados_resposta_data_invalida': resultado_status.get(
                'descartados_resposta_data_invalida', 0
            ),
        },
    }
    resultado = ok_result(
        mensagens=resultado_status.get('mensagens', []),
        metricas={
            'total_status': resultado_status.get('total_status', 0),
            'com_match': resultado_status.get('com_match', 0),
            'sem_match': resultado_status.get('sem_match', 0),
            'descartados_status_data_invalida': resultado_status.get(
                'descartados_status_data_invalida', 0
            ),
            'descartados_resposta_data_invalida': resultado_status.get(
                'descartados_resposta_data_invalida', 0
            ),
            'nat_data_agendamento': resultado_ingestao.get('nat_data_agendamento', 0),
            'pct_nat_data_agendamento': resultado_ingestao.get('pct_nat_data_agendamento', 0.0),
            'nat_dt_atendimento': resultado_ingestao.get('nat_dt_atendimento', 0),
            'pct_nat_dt_atendimento': resultado_ingestao.get('pct_nat_dt_atendimento', 0.0),
            'limiar_nat_data_em_uso': resultado_ingestao.get('limiar_nat_data_em_uso'),
        },
        arquivos={'arquivo_status_integrado': resultado_status.get('arquivo_saida')},
        dados={
            'qualidade_data': resultado_ingestao.get('qualidade_data', {}),
            'metricas_por_etapa': metricas_por_etapa,
        },
    )
    if not logger_externo:
        logger.finalizar('SUCESSO')
    return resultado


def run_internacao_eletivo_pipeline_gerar_status_dataset(
    arquivo_status=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['arquivo_status'],
    arquivo_status_resposta_eletivo=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults[
        'arquivo_status_resposta_eletivo'
    ],
    arquivo_status_resposta_internacao=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults[
        'arquivo_status_resposta_internacao'
    ],
    arquivo_status_resposta_unificado=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults[
        'arquivo_status_resposta_unificado'
    ],
    arquivo_dataset_origem_internacao=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults[
        'arquivo_dataset_origem_internacao'
    ],
    saida_status=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_status'],
    saida_status_resposta=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_status_resposta'],
    saida_status_integrado=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_status_integrado'],
    saida_dataset_status=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_dataset_status'],
):
    logger = PipelineLogger(
        nome_pipeline=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.logger_status_com_resposta
    )
    resultado_status = run_internacao_eletivo_pipeline_enviar_status_com_resposta(
        arquivo_status=arquivo_status,
        arquivo_status_resposta_eletivo=arquivo_status_resposta_eletivo,
        arquivo_status_resposta_internacao=arquivo_status_resposta_internacao,
        arquivo_status_resposta_unificado=arquivo_status_resposta_unificado,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        saida_status_integrado=saida_status_integrado,
        logger=logger,
    )
    if not resultado_status.get('ok'):
        logger.finalizar('FALHA')
        return resultado_status

    resultado_dataset = run_internacao_eletivo_pipeline_criar_dataset_status(
        arquivo_origem_dataset=arquivo_dataset_origem_internacao,
        arquivo_status_integrado=saida_status_integrado,
        arquivo_saida_dataset=saida_dataset_status,
        nome_logger=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.logger_criacao_dataset,
        contexto=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.nome,
        logger=logger,
        finalizar_logger=False,
    )
    if not resultado_dataset.get('ok'):
        logger.finalizar('FALHA')
        return resultado_dataset

    metricas_por_etapa = {
        **resultado_status.get('metricas_por_etapa', {}),
        'criacao_dataset_status': {
            'total_linhas': resultado_dataset.get('total_linhas', 0),
        },
    }
    resultado = ok_result(
        mensagens=(
            resultado_status.get('mensagens', [])
            + resultado_dataset.get('mensagens', [])
        ),
        metricas={
            'total_status': resultado_status.get('total_status', 0),
            'com_match': resultado_status.get('com_match', 0),
            'sem_match': resultado_status.get('sem_match', 0),
            'descartados_status_data_invalida': resultado_status.get(
                'descartados_status_data_invalida', 0
            ),
            'descartados_resposta_data_invalida': resultado_status.get(
                'descartados_resposta_data_invalida', 0
            ),
            'nat_data_agendamento': resultado_status.get('nat_data_agendamento', 0),
            'pct_nat_data_agendamento': resultado_status.get('pct_nat_data_agendamento', 0.0),
            'nat_dt_atendimento': resultado_status.get('nat_dt_atendimento', 0),
            'pct_nat_dt_atendimento': resultado_status.get('pct_nat_dt_atendimento', 0.0),
            'limiar_nat_data_em_uso': resultado_status.get('limiar_nat_data_em_uso'),
            'total_linhas': resultado_dataset.get('total_linhas', 0),
        },
        arquivos={'arquivo_status_dataset': resultado_dataset.get('arquivo_saida')},
        dados={
            'qualidade_data': resultado_status.get('qualidade_data', {}),
            'metricas_por_etapa': metricas_por_etapa,
        },
    )
    logger.finalizar('SUCESSO')
    return resultado


def run_internacao_eletivo_pipeline_gerar_status_dataset_somente_status(
    arquivo_status=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['arquivo_status'],
    arquivo_dataset_origem_internacao=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults[
        'arquivo_dataset_origem_internacao'
    ],
    saida_status=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_status'],
    saida_status_integrado=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_status_integrado'],
    saida_dataset_status=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_dataset_status'],
):
    logger = PipelineLogger(
        nome_pipeline=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.logger_status_somente_status
    )
    resultado_status = run_internacao_eletivo_pipeline_enviar_status_somente_status(
        arquivo_status=arquivo_status,
        saida_status=saida_status,
        saida_status_integrado=saida_status_integrado,
        logger=logger,
    )
    if not resultado_status.get('ok'):
        logger.finalizar('FALHA')
        return resultado_status

    resultado_dataset = run_internacao_eletivo_pipeline_criar_dataset_status(
        arquivo_origem_dataset=arquivo_dataset_origem_internacao,
        arquivo_status_integrado=saida_status_integrado,
        arquivo_saida_dataset=saida_dataset_status,
        nome_logger=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.logger_criacao_dataset_somente_status,
        contexto=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.nome,
        logger=logger,
        finalizar_logger=False,
    )
    if not resultado_dataset.get('ok'):
        logger.finalizar('FALHA')
        return resultado_dataset

    metricas_por_etapa = {
        **resultado_status.get('metricas_por_etapa', {}),
        'criacao_dataset_status': {
            'total_linhas': resultado_dataset.get('total_linhas', 0),
        },
    }
    resultado = ok_result(
        mensagens=(
            resultado_status.get('mensagens', [])
            + resultado_dataset.get('mensagens', [])
        ),
        metricas={
            'total_status': resultado_status.get('total_status', 0),
            'com_match': resultado_status.get('com_match', 0),
            'sem_match': resultado_status.get('sem_match', 0),
            'descartados_status_data_invalida': resultado_status.get(
                'descartados_status_data_invalida', 0
            ),
            'descartados_resposta_data_invalida': resultado_status.get(
                'descartados_resposta_data_invalida', 0
            ),
            'nat_data_agendamento': resultado_status.get('nat_data_agendamento', 0),
            'pct_nat_data_agendamento': resultado_status.get('pct_nat_data_agendamento', 0.0),
            'nat_dt_atendimento': resultado_status.get('nat_dt_atendimento', 0),
            'pct_nat_dt_atendimento': resultado_status.get('pct_nat_dt_atendimento', 0.0),
            'limiar_nat_data_em_uso': resultado_status.get('limiar_nat_data_em_uso'),
            'total_linhas': resultado_dataset.get('total_linhas', 0),
        },
        arquivos={'arquivo_status_dataset': resultado_dataset.get('arquivo_saida')},
        dados={
            'qualidade_data': resultado_status.get('qualidade_data', {}),
            'metricas_por_etapa': metricas_por_etapa,
        },
    )
    logger.finalizar('SUCESSO')
    return resultado


def run_internacao_eletivo_pipeline_criar_dataset_status(
    arquivo_origem_dataset=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults[
        'arquivo_dataset_origem_internacao'
    ],
    arquivo_status_integrado=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_status_integrado'],
    arquivo_saida_dataset=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.defaults['saida_dataset_status'],
    nome_logger=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.logger_criacao_dataset,
    contexto=CONTEXTO_PIPELINE_INTERNACAO_ELETIVO.nome,
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
