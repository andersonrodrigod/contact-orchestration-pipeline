from core.logger import PipelineLogger
from core.pipeline_result import ok_result
from pathlib import Path
from src.contexts.pipeline_contextos import CONTEXTO_PIPELINE_COMPLICACAO
from src.pipelines.contexto_status_pipeline_base import (
    caminho_xlsx_pareado,
    run_criacao_dataset_status_base,
)
from src.pipelines.join_status_resposta_pipeline import (
    run_status_somente_complicacao_pipeline,
    run_unificar_status_resposta_complicacao_pipeline,
)
from src.services.analise_dados_fase1_service import gerar_analise_dados_fase1_csv
from src.services.analise_dados_fase2_service import gerar_analise_dados_fase2_csv
from src.services.dataset_service import criar_dataset_complicacao
from src.services.graficos_uniao_status_resposta_service import gerar_graficos_uniao_status_resposta
from src.services.ingestao_service import executar_ingestao_complicacao, executar_ingestao_somente_status
from src.services.resumo_complicacao_service import gerar_resumo_complicacao_csv


def run_complicacao_pipeline_enviar_status_com_resposta(
    arquivo_status=CONTEXTO_PIPELINE_COMPLICACAO.defaults['arquivo_status'],
    arquivo_status_resposta_complicacao=CONTEXTO_PIPELINE_COMPLICACAO.defaults[
        'arquivo_status_resposta_complicacao'
    ],
    saida_status=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_status'],
    saida_status_resposta=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_status_resposta'],
    saida_status_integrado=CONTEXTO_PIPELINE_COMPLICACAO.defaults['saida_status_integrado'],
    raiz_analise_dados='src/data/analise_dados/complicacao',
    nome_execucao_analise=None,
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

    resultado_analise_fase1 = gerar_analise_dados_fase1_csv(
        arquivo_status=saida_status,
        arquivo_status_resposta=saida_status_resposta,
        arquivo_status_integrado=saida_status_integrado,
        com_match=resultado_integracao.get('com_match', 0),
        sem_match=resultado_integracao.get('sem_match', 0),
        raiz_analise=raiz_analise_dados,
        nome_execucao=nome_execucao_analise,
        nome_processo='uniao_status_resposta',
        respostas_canonicas=['Sim', 'Nao', 'Sem resposta'],
    )
    logger.info(
        'ANALISE_DADOS',
        f"CSVs da Fase 1 gerados em: {resultado_analise_fase1.get('pasta_saida', '')}",
    )
    resultado_graficos_fase1 = gerar_graficos_uniao_status_resposta(
        contexto='complicacao',
        raiz_analise_contexto=raiz_analise_dados,
        pasta_origem_csv=resultado_analise_fase1.get('pasta_saida', ''),
    )
    logger.info(
        'GRAFICOS',
        (
            "Graficos da Fase 1 (uniao_status_resposta) gerados em: "
            f"{resultado_graficos_fase1.get('pasta_saida', '')}"
        ),
    )

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
            'pasta_analise_dados_fase1': resultado_analise_fase1.get('pasta_saida', ''),
        },
    }
    resultado = ok_result(
        mensagens=resultado_integracao.get('mensagens', [])
        + [
            f"Analise de dados Fase 1 gerada em: {resultado_analise_fase1.get('pasta_saida', '')}",
            f"Manifest de graficos Fase 1: {resultado_graficos_fase1.get('arquivo_manifest', '')}",
        ],
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
        arquivos={
            'arquivo_status_integrado': resultado_integracao.get('arquivo_saida'),
            'pasta_analise_dados_fase1': resultado_analise_fase1.get('pasta_saida', ''),
        },
        dados={
            'qualidade_data': resultado_ingestao.get('qualidade_data', {}),
            'metricas_por_etapa': metricas_por_etapa,
            'analise_dados_fase1': resultado_analise_fase1,
            'graficos_uniao_status_resposta': resultado_graficos_fase1,
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
    raiz_analise_dados='src/data/analise_dados/complicacao',
    nome_execucao_analise_fase2=None,
):
    logger = PipelineLogger(nome_pipeline=CONTEXTO_PIPELINE_COMPLICACAO.logger_status_com_resposta)
    resultado_status = run_complicacao_pipeline_enviar_status_com_resposta(
        arquivo_status=arquivo_status,
        arquivo_status_resposta_complicacao=arquivo_status_resposta_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        saida_status_integrado=saida_status_integrado,
        raiz_analise_dados=raiz_analise_dados,
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

    resultado_resumo_complicacao = gerar_resumo_complicacao_csv(
        arquivo_origem_complicacao=arquivo_dataset_origem_complicacao,
        raiz_analise=raiz_analise_dados,
    )
    logger.info(
        'ANALISE_DADOS',
        'Resumo complicacao gerado em: '
        f"{resultado_resumo_complicacao.get('pasta_saida', '')}",
    )
    for mensagem in resultado_resumo_complicacao.get('mensagens', []):
        logger.warning('ANALISE_DADOS', mensagem)

    resultado_analise_fase2 = gerar_analise_dados_fase2_csv(
        arquivo_dataset_status=saida_dataset_status,
        raiz_analise=raiz_analise_dados,
        nome_execucao=nome_execucao_analise_fase2,
        nome_processo='envio_status',
    )
    logger.info(
        'ANALISE_DADOS',
        f"CSVs da Fase 2 gerados em: {resultado_analise_fase2.get('pasta_saida', '')}",
    )

    metricas_por_etapa = {
        **resultado_status.get('metricas_por_etapa', {}),
        'criacao_dataset_status': {
            'total_linhas': resultado_dataset.get('total_linhas', 0),
        },
        'resumo_complicacao': {
            'pasta_analise': resultado_resumo_complicacao.get('pasta_saida', ''),
            'arquivos_gerados': resultado_resumo_complicacao.get('arquivos_gerados', []),
        },
        'envio_status_metricas': {
            'pasta_analise_dados_fase2': resultado_analise_fase2.get('pasta_saida', ''),
        },
    }
    resultado = ok_result(
        mensagens=(
            resultado_status.get('mensagens', [])
            + resultado_dataset.get('mensagens', [])
            + resultado_resumo_complicacao.get('mensagens', [])
            + [
                'Resumo complicacao gerado em: '
                f"{resultado_resumo_complicacao.get('pasta_saida', '')}",
                f"Analise de dados Fase 2 gerada em: {resultado_analise_fase2.get('pasta_saida', '')}",
            ]
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
            'resumo_complicacao': resultado_resumo_complicacao,
            'analise_dados_fase2': resultado_analise_fase2,
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
    raiz_analise_dados='src/data/analise_dados/complicacao',
    nome_execucao_analise_fase2=None,
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

    resultado_resumo_complicacao = gerar_resumo_complicacao_csv(
        arquivo_origem_complicacao=arquivo_dataset_origem_complicacao,
        raiz_analise=raiz_analise_dados,
    )
    logger.info(
        'ANALISE_DADOS',
        'Resumo complicacao gerado em: '
        f"{resultado_resumo_complicacao.get('pasta_saida', '')}",
    )
    for mensagem in resultado_resumo_complicacao.get('mensagens', []):
        logger.warning('ANALISE_DADOS', mensagem)

    resultado_analise_fase2 = gerar_analise_dados_fase2_csv(
        arquivo_dataset_status=saida_dataset_status,
        raiz_analise=raiz_analise_dados,
        nome_execucao=nome_execucao_analise_fase2,
        nome_processo='envio_status',
    )
    logger.info(
        'ANALISE_DADOS',
        f"CSVs da Fase 2 gerados em: {resultado_analise_fase2.get('pasta_saida', '')}",
    )

    metricas_por_etapa = {
        **resultado_status.get('metricas_por_etapa', {}),
        'criacao_dataset_status': {
            'total_linhas': resultado_dataset.get('total_linhas', 0),
        },
        'resumo_complicacao': {
            'pasta_analise': resultado_resumo_complicacao.get('pasta_saida', ''),
            'arquivos_gerados': resultado_resumo_complicacao.get('arquivos_gerados', []),
        },
        'envio_status_metricas': {
            'pasta_analise_dados_fase2': resultado_analise_fase2.get('pasta_saida', ''),
        },
    }
    resultado = ok_result(
        mensagens=(
            resultado_status.get('mensagens', [])
            + resultado_dataset.get('mensagens', [])
            + resultado_resumo_complicacao.get('mensagens', [])
            + [
                'Resumo complicacao gerado em: '
                f"{resultado_resumo_complicacao.get('pasta_saida', '')}",
                f"Analise de dados Fase 2 gerada em: {resultado_analise_fase2.get('pasta_saida', '')}",
            ]
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
            'resumo_complicacao': resultado_resumo_complicacao,
            'analise_dados_fase2': resultado_analise_fase2,
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
