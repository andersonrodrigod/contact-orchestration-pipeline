from core.logger import PipelineLogger
from src.config.paths import DEFAULTS_COMPLICACAO, DEFAULTS_INTERNACAO_ELETIVO
from src.pipelines.criacao_dataset_pipeline import run_criacao_dataset_pipeline
from src.pipelines.finalizacao_pipeline import run_finalizacao_pipeline
from src.pipelines.ingestao_pipeline import (
    run_ingestao_complicacao,
    run_ingestao_somente_status,
    run_ingestao_unificar,
)
from src.pipelines.join_status_resposta_pipeline import (
    run_status_somente_complicacao_pipeline,
    run_status_somente_internacao_eletivo_pipeline,
    run_unificar_status_resposta_complicacao_pipeline,
    run_unificar_status_resposta_internacao_eletivo_pipeline,
)


def _combinar_etapas(resultado_etapa_1, resultado_etapa_2):
    combinado = dict(resultado_etapa_2)
    combinado['mensagens'] = (
        resultado_etapa_1.get('mensagens', [])
        + resultado_etapa_2.get('mensagens', [])
    )
    return combinado


def _modo_individual_bloqueado(nome_modo):
    logger = PipelineLogger(nome_pipeline=f'main_{nome_modo}')
    logger.warning('MODO_INDIVIDUAL', 'Modo individual desabilitado por configuracao')
    logger.finalizar('BLOQUEADO')
    return {
        'ok': False,
        'mensagens': [
            f'Modo individual "{nome_modo}" desabilitado. '
            'Defina ALLOW_MODOS_INDIVIDUAIS = True no main.py para executar.'
        ],
    }


def _executar_modo_individual(nome_modo, permitir_execucao, funcao_execucao):
    if not permitir_execucao:
        return _modo_individual_bloqueado(nome_modo)

    logger = PipelineLogger(nome_pipeline=f'main_{nome_modo}')
    logger.info('MODO_INDIVIDUAL', 'Modo individual habilitado')
    try:
        resultado = funcao_execucao()
        logger.info('RESULTADO', f"ok={resultado.get('ok', False)}")
        for mensagem in resultado.get('mensagens', []):
            logger.info('RESULTADO', mensagem)
        logger.finalizar('SUCESSO' if resultado.get('ok') else 'FALHA')
        return resultado
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        logger.finalizar('ERRO')
        return {
            'ok': False,
            'mensagens': [f'Erro no modo individual "{nome_modo}": {type(erro).__name__}: {erro}'],
        }


def obter_modos_individuais(permitir_execucao=False):
    def _run_individual_unificar_status_respostas():
        # Executa concatenacao + normalizacao via ingestao_unificar.
        return _executar_modo_individual(
            'individual_unificar_status_respostas',
            permitir_execucao,
            lambda: run_ingestao_unificar(
                arquivo_status=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status'],
                arquivo_status_resposta_eletivo=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_eletivo'],
                arquivo_status_resposta_internacao=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_internacao'],
                arquivo_status_resposta_unificado=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_unificado'],
                saida_status=DEFAULTS_INTERNACAO_ELETIVO['saida_status'],
                saida_status_resposta=DEFAULTS_INTERNACAO_ELETIVO['saida_status_resposta'],
            ),
        )

    def _run_individual_ingestao_complicacao():
        return _executar_modo_individual(
            'individual_ingestao_complicacao',
            permitir_execucao,
            lambda: run_ingestao_complicacao(
                arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
                arquivo_status_resposta_complicacao=DEFAULTS_COMPLICACAO['arquivo_status_resposta_complicacao'],
                saida_status=DEFAULTS_COMPLICACAO['saida_status'],
                saida_status_resposta=DEFAULTS_COMPLICACAO['saida_status_resposta'],
            ),
        )

    def _run_individual_ingestao_internacao_eletivo():
        return _executar_modo_individual(
            'individual_ingestao_internacao_eletivo',
            permitir_execucao,
            lambda: run_ingestao_unificar(
                arquivo_status=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status'],
                arquivo_status_resposta_eletivo=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_eletivo'],
                arquivo_status_resposta_internacao=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_internacao'],
                arquivo_status_resposta_unificado=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_unificado'],
                saida_status=DEFAULTS_INTERNACAO_ELETIVO['saida_status'],
                saida_status_resposta=DEFAULTS_INTERNACAO_ELETIVO['saida_status_resposta'],
            ),
        )

    def _run_individual_status_somente_complicacao():
        return _executar_modo_individual(
            'individual_status_somente_complicacao',
            permitir_execucao,
            lambda: run_ingestao_somente_status(
                arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
                saida_status=DEFAULTS_COMPLICACAO['saida_status'],
                nome_logger='ingestao_complicacao_individual_status',
            ),
        )

    def _run_individual_status_somente_internacao_eletivo():
        return _executar_modo_individual(
            'individual_status_somente_internacao_eletivo',
            permitir_execucao,
            lambda: run_ingestao_somente_status(
                arquivo_status=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status'],
                saida_status=DEFAULTS_INTERNACAO_ELETIVO['saida_status'],
                nome_logger='ingestao_internacao_eletivo_individual_status',
            ),
        )

    def _run_individual_enviar_status_complicacao():
        def _executar():
            resultado_ingestao = run_ingestao_complicacao(
                arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
                arquivo_status_resposta_complicacao=DEFAULTS_COMPLICACAO['arquivo_status_resposta_complicacao'],
                saida_status=DEFAULTS_COMPLICACAO['saida_status'],
                saida_status_resposta=DEFAULTS_COMPLICACAO['saida_status_resposta'],
            )
            if not resultado_ingestao.get('ok'):
                return resultado_ingestao

            resultado_unificacao = run_unificar_status_resposta_complicacao_pipeline(
                arquivo_status=DEFAULTS_COMPLICACAO['saida_status'],
                arquivo_status_resposta=DEFAULTS_COMPLICACAO['saida_status_resposta'],
                arquivo_saida=DEFAULTS_COMPLICACAO['saida_status_integrado'],
            )
            if not resultado_unificacao.get('ok'):
                return resultado_unificacao
            return _combinar_etapas(resultado_ingestao, resultado_unificacao)

        return _executar_modo_individual(
            'individual_enviar_status_complicacao',
            permitir_execucao,
            _executar,
        )

    def _run_individual_enviar_status_internacao_eletivo():
        def _executar():
            resultado_ingestao = run_ingestao_unificar(
                arquivo_status=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status'],
                arquivo_status_resposta_eletivo=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_eletivo'],
                arquivo_status_resposta_internacao=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_internacao'],
                arquivo_status_resposta_unificado=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_unificado'],
                saida_status=DEFAULTS_INTERNACAO_ELETIVO['saida_status'],
                saida_status_resposta=DEFAULTS_INTERNACAO_ELETIVO['saida_status_resposta'],
            )
            if not resultado_ingestao.get('ok'):
                return resultado_ingestao

            resultado_unificacao = run_unificar_status_resposta_internacao_eletivo_pipeline(
                arquivo_status=DEFAULTS_INTERNACAO_ELETIVO['saida_status'],
                arquivo_status_resposta=DEFAULTS_INTERNACAO_ELETIVO['saida_status_resposta'],
                arquivo_saida=DEFAULTS_INTERNACAO_ELETIVO['saida_status_integrado'],
            )
            if not resultado_unificacao.get('ok'):
                return resultado_unificacao
            return _combinar_etapas(resultado_ingestao, resultado_unificacao)

        return _executar_modo_individual(
            'individual_enviar_status_internacao_eletivo',
            permitir_execucao,
            _executar,
        )

    def _run_individual_status_filtrado_complicacao():
        def _executar():
            resultado_ingestao = run_ingestao_somente_status(
                arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
                saida_status=DEFAULTS_COMPLICACAO['saida_status'],
                nome_logger='ingestao_complicacao_individual_status_filtrado',
            )
            if not resultado_ingestao.get('ok'):
                return resultado_ingestao

            resultado_status = run_status_somente_complicacao_pipeline(
                arquivo_status=DEFAULTS_COMPLICACAO['saida_status'],
                arquivo_saida=DEFAULTS_COMPLICACAO['saida_status_integrado'],
            )
            if not resultado_status.get('ok'):
                return resultado_status
            return _combinar_etapas(resultado_ingestao, resultado_status)

        return _executar_modo_individual(
            'individual_status_filtrado_complicacao',
            permitir_execucao,
            _executar,
        )

    def _run_individual_status_filtrado_internacao_eletivo():
        def _executar():
            resultado_ingestao = run_ingestao_somente_status(
                arquivo_status=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status'],
                saida_status=DEFAULTS_INTERNACAO_ELETIVO['saida_status'],
                nome_logger='ingestao_internacao_eletivo_individual_status_filtrado',
            )
            if not resultado_ingestao.get('ok'):
                return resultado_ingestao

            resultado_status = run_status_somente_internacao_eletivo_pipeline(
                arquivo_status=DEFAULTS_INTERNACAO_ELETIVO['saida_status'],
                arquivo_saida=DEFAULTS_INTERNACAO_ELETIVO['saida_status_integrado'],
            )
            if not resultado_status.get('ok'):
                return resultado_status
            return _combinar_etapas(resultado_ingestao, resultado_status)

        return _executar_modo_individual(
            'individual_status_filtrado_internacao_eletivo',
            permitir_execucao,
            _executar,
        )

    def _run_individual_criar_dataset_complicacao():
        return _executar_modo_individual(
            'individual_criar_dataset_complicacao',
            permitir_execucao,
            lambda: run_criacao_dataset_pipeline(
                arquivo_origem_dataset=DEFAULTS_COMPLICACAO['arquivo_dataset_origem_complicacao'],
                arquivo_status_integrado=DEFAULTS_COMPLICACAO['saida_status_integrado'],
                arquivo_saida_dataset=DEFAULTS_COMPLICACAO['saida_dataset_status'],
                nome_logger='criacao_dataset_complicacao_individual',
                contexto='complicacao',
            ),
        )

    def _run_individual_criar_dataset_internacao_eletivo():
        return _executar_modo_individual(
            'individual_criar_dataset_internacao_eletivo',
            permitir_execucao,
            lambda: run_criacao_dataset_pipeline(
                arquivo_origem_dataset=DEFAULTS_INTERNACAO_ELETIVO['arquivo_dataset_origem_internacao'],
                arquivo_status_integrado=DEFAULTS_INTERNACAO_ELETIVO['saida_status_integrado'],
                arquivo_saida_dataset=DEFAULTS_INTERNACAO_ELETIVO['saida_dataset_status'],
                nome_logger='criacao_dataset_internacao_eletivo_individual',
                contexto='internacao_eletivo',
            ),
        )

    def _run_individual_orquestrar_complicacao():
        return _executar_modo_individual(
            'individual_orquestrar_complicacao',
            permitir_execucao,
            lambda: run_finalizacao_pipeline(
                arquivo_dataset_entrada=DEFAULTS_COMPLICACAO['saida_dataset_status'],
                arquivo_dataset_saida=DEFAULTS_COMPLICACAO['saida_dataset_final'],
                nome_logger='finalizacao_complicacao_individual',
            ),
        )

    def _run_individual_orquestrar_internacao_eletivo():
        return _executar_modo_individual(
            'individual_orquestrar_internacao_eletivo',
            permitir_execucao,
            lambda: run_finalizacao_pipeline(
                arquivo_dataset_entrada=DEFAULTS_INTERNACAO_ELETIVO['saida_dataset_status'],
                arquivo_dataset_saida=DEFAULTS_INTERNACAO_ELETIVO['saida_dataset_final'],
                nome_logger='finalizacao_internacao_eletivo_individual',
            ),
        )

    return {
        'individual_unificar_status_respostas': _run_individual_unificar_status_respostas,
        'individual_ingestao_complicacao': _run_individual_ingestao_complicacao,
        'individual_ingestao_internacao_eletivo': _run_individual_ingestao_internacao_eletivo,
        'individual_status_somente_complicacao': _run_individual_status_somente_complicacao,
        'individual_status_somente_internacao_eletivo': _run_individual_status_somente_internacao_eletivo,
        'individual_status_filtrado_complicacao': _run_individual_status_filtrado_complicacao,
        'individual_status_filtrado_internacao_eletivo': _run_individual_status_filtrado_internacao_eletivo,
        'individual_enviar_status_complicacao': _run_individual_enviar_status_complicacao,
        'individual_enviar_status_internacao_eletivo': _run_individual_enviar_status_internacao_eletivo,
        'individual_criar_dataset_complicacao': _run_individual_criar_dataset_complicacao,
        'individual_criar_dataset_internacao_eletivo': _run_individual_criar_dataset_internacao_eletivo,
        'individual_orquestrar_complicacao': _run_individual_orquestrar_complicacao,
        'individual_orquestrar_internacao_eletivo': _run_individual_orquestrar_internacao_eletivo,
    }
