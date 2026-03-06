from core.logger import PipelineLogger
from pathlib import Path
from core.error_codes import (
    ERRO_CRIACAO_DATASET,
    ERRO_VALIDACAO_ARQUIVOS,
    ERRO_VALIDACAO_COLUNAS,
)
from core.pipeline_result import error_result
from core.pipeline_result import ok_result
from src.contexts.pipeline_contextos import CONTEXTO_PIPELINE_COMPLICACAO
from src.pipelines.join_status_resposta_pipeline import (
    run_status_somente_complicacao_pipeline,
    run_unificar_status_resposta_complicacao_pipeline,
)
from src.services.dataset_service import criar_dataset_complicacao
from src.services.ingestao_service import executar_ingestao_complicacao, executar_ingestao_somente_status
from src.services.validacao_service import validar_colunas_origem_dataset_complicacao
from src.utils.arquivos import ler_arquivo_csv, validar_arquivos_existem


def _caminho_xlsx_pareado(caminho_arquivo):
    caminho = Path(caminho_arquivo)
    return str(caminho.with_suffix('.xlsx'))


def _run_criacao_dataset_status(
    arquivo_origem_dataset,
    arquivo_status_integrado,
    arquivo_saida_dataset,
    nome_logger='criacao_dataset_status_complicacao',
    contexto='complicacao',
    logger=None,
    finalizar_logger=True,
):
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(nome_pipeline=nome_logger)
    logger.info('INICIO', f'arquivo_origem_dataset={arquivo_origem_dataset}')
    logger.info('INICIO', f'arquivo_status_integrado={arquivo_status_integrado}')
    logger.info('INICIO', f'arquivo_saida_dataset={arquivo_saida_dataset}')

    try:
        validacao_arquivos = validar_arquivos_existem(
            {
                'arquivo_origem_dataset': arquivo_origem_dataset,
                'arquivo_status_integrado': arquivo_status_integrado,
            }
        )
        if not validacao_arquivos['ok']:
            for mensagem in validacao_arquivos['mensagens']:
                logger.error('VALIDACAO_ARQUIVOS', mensagem)
            if not logger_externo and finalizar_logger:
                logger.finalizar('FALHA_VALIDACAO_ARQUIVOS')
            return error_result(
                mensagens=validacao_arquivos['mensagens'],
                codigo_erro=ERRO_VALIDACAO_ARQUIVOS,
            )

        df_origem = ler_arquivo_csv(arquivo_origem_dataset)
        colunas_arquivo = [str(col).strip() for col in df_origem.columns]
        resultado_validacao = validar_colunas_origem_dataset_complicacao(colunas_arquivo, contexto=contexto)
        logger.info('VALIDACAO_COLUNAS', f"ok={resultado_validacao['ok']}")
        for mensagem in resultado_validacao.get('mensagens', []):
            logger.info('VALIDACAO_COLUNAS', mensagem)
        if not resultado_validacao['ok']:
            if not logger_externo and finalizar_logger:
                logger.finalizar('FALHA_VALIDACAO_COLUNAS')
            return error_result(
                mensagens=resultado_validacao['mensagens'],
                codigo_erro=ERRO_VALIDACAO_COLUNAS,
            )

        resultado = criar_dataset_complicacao(
            arquivo_complicacao=arquivo_origem_dataset,
            arquivo_saida_dataset=arquivo_saida_dataset,
            arquivo_status_integrado=arquivo_status_integrado,
            contexto=contexto,
        )
        if not resultado.get('ok'):
            if not resultado.get('codigo_erro'):
                resultado['codigo_erro'] = ERRO_CRIACAO_DATASET
            for mensagem in resultado.get('mensagens', []):
                logger.warning('CRIACAO_DATASET', mensagem)
            if not logger_externo and finalizar_logger:
                logger.finalizar('FALHA_CRIACAO_DATASET')
            return resultado

        logger.info('SAIDA', f"arquivo_saida={resultado.get('arquivo_saida', '')}")
        logger.info('SAIDA', f"total_linhas={resultado.get('total_linhas', 0)}")
        if not logger_externo and finalizar_logger:
            logger.finalizar('SUCESSO')
        return resultado
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        if not logger_externo and finalizar_logger:
            logger.finalizar('ERRO')
        return error_result(
            mensagens=[f'Erro na criacao do dataset status: {erro}'],
            codigo_erro=ERRO_CRIACAO_DATASET,
        )


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

    arquivo_status_xlsx = _caminho_xlsx_pareado(saida_status)
    arquivo_resposta_xlsx = _caminho_xlsx_pareado(saida_status_resposta)
    if executar_xlsx_adicional and Path(arquivo_status_xlsx).exists() and Path(arquivo_resposta_xlsx).exists():
        arquivo_saida_xlsx = _caminho_xlsx_pareado(saida_status_integrado)
        logger.info('MODO_XLSX', 'Execucao adicional XLSX iniciada (integracao status + resposta).')
        logger.info('MODO_XLSX', f'arquivo_status={arquivo_status_xlsx}')
        logger.info('MODO_XLSX', f'arquivo_status_resposta={arquivo_resposta_xlsx}')
        logger.info('MODO_XLSX', f'arquivo_saida={arquivo_saida_xlsx}')
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


def run_complicacao_pipeline_enviar_status_somente_status(
    arquivo_status=CONTEXTO_PIPELINE_COMPLICACAO.defaults['arquivo_status'],
    saida_status=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_status'],
    saida_status_integrado=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_status_integrado'],
    logger=None,
):
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(
            nome_pipeline=CONTEXTO_PIPELINE_COMPLICACAO.logger_status_somente_status
        )
    resultado_ingestao = executar_ingestao_somente_status(
        arquivo_status=arquivo_status,
        saida_status=saida_status,
        nome_logger='ingestao_complicacao_somente_status',
        contexto=CONTEXTO_PIPELINE_COMPLICACAO.nome,
        logger=logger,
    )
    if not resultado_ingestao.get('ok'):
        if not logger_externo:
            logger.finalizar('FALHA_INGESTAO')
        return resultado_ingestao

    resultado_status = run_status_somente_complicacao_pipeline(
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


def run_complicacao_pipeline_gerar_status_dataset_somente_status(
    arquivo_status=CONTEXTO_PIPELINE_COMPLICACAO.defaults['arquivo_status'],
    arquivo_dataset_origem_complicacao=CONTEXTO_PIPELINE_COMPLICACAO.defaults[
        'arquivo_dataset_origem_complicacao'
    ],
    saida_status=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_status'],
    saida_status_integrado=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_status_integrado'],
    saida_dataset_status=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_dataset_status'],
):
    logger = PipelineLogger(nome_pipeline=CONTEXTO_PIPELINE_COMPLICACAO.logger_status_somente_status)
    resultado_status = run_complicacao_pipeline_enviar_status_somente_status(
        arquivo_status=arquivo_status,
        saida_status=saida_status,
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
        nome_logger=CONTEXTO_PIPELINE_COMPLICACAO.logger_criacao_dataset_somente_status,
        contexto=CONTEXTO_PIPELINE_COMPLICACAO.nome,
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


def run_complicacao_pipeline_criar_dataset_status(
    arquivo_origem_dataset=CONTEXTO_PIPELINE_COMPLICACAO.defaults['arquivo_dataset_origem_complicacao'],
    arquivo_status_integrado=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_status_integrado'],
    arquivo_saida_dataset=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_dataset_status'],
    nome_logger=CONTEXTO_PIPELINE_COMPLICACAO.logger_criacao_dataset,
    contexto=CONTEXTO_PIPELINE_COMPLICACAO.nome,
    logger=None,
    finalizar_logger=True,
):
    return _run_criacao_dataset_status(
        arquivo_origem_dataset=arquivo_origem_dataset,
        arquivo_status_integrado=arquivo_status_integrado,
        arquivo_saida_dataset=arquivo_saida_dataset,
        nome_logger=nome_logger,
        contexto=contexto,
        logger=logger,
        finalizar_logger=finalizar_logger,
    )
