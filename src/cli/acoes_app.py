from core.logger import PipelineLogger
from src.pipelines.complicacao_orquestracao_pipeline import run_complicacao_pipeline_orquestrar
from src.pipelines.complicacao_status_pipeline import (
    run_complicacao_pipeline_enviar_status_com_resposta,
    run_complicacao_pipeline_enviar_status_somente_status,
    run_complicacao_pipeline_gerar_status_dataset,
    run_complicacao_pipeline_gerar_status_dataset_somente_status,
)
from src.pipelines.internacao_eletivo_orquestracao_pipeline import (
    run_internacao_eletivo_pipeline_orquestrar,
)
from src.pipelines.internacao_eletivo_status_pipeline import (
    run_internacao_eletivo_pipeline_enviar_status_com_resposta,
    run_internacao_eletivo_pipeline_enviar_status_somente_status,
    run_internacao_eletivo_pipeline_gerar_status_dataset,
    run_internacao_eletivo_pipeline_gerar_status_dataset_somente_status,
)
from src.services.ingestao_service import executar_ingestao_complicacao, executar_ingestao_unificar


def _validar_parametros_obrigatorios(parametros):
    faltando = []
    for chave, valor in parametros.items():
        if valor is None:
            faltando.append(chave)
            continue
        if isinstance(valor, str) and valor.strip() == '':
            faltando.append(chave)
    return faltando


def _executar_acao_app(nome_acao, funcao_execucao, **parametros):
    logger = PipelineLogger(nome_pipeline=f'app_{nome_acao}')
    logger.info('INICIO', f'acao={nome_acao}')
    for chave, valor in parametros.items():
        logger.info('PARAMETRO', f'{chave}={valor}')

    faltando = _validar_parametros_obrigatorios(parametros)
    if faltando:
        logger.warning('VALIDACAO_PARAMETROS', f'Parametros obrigatorios ausentes: {faltando}')
        logger.finalizar('FALHA_VALIDACAO_PARAMETROS')
        return {
            'ok': False,
            'mensagens': [f'Parametros obrigatorios ausentes: {faltando}'],
            'arquivo_log_individual': str(logger.caminho_arquivo),
        }

    try:
        resultado = funcao_execucao(**parametros)
        if not isinstance(resultado, dict):
            resultado = {
                'ok': False,
                'mensagens': [
                    f'Retorno invalido da acao "{nome_acao}": esperado dict, recebido {type(resultado).__name__}.'
                ],
            }

        logger.info('RESULTADO', f"ok={resultado.get('ok', False)}")
        for mensagem in resultado.get('mensagens', []):
            logger.info('RESULTADO', mensagem)
        logger.finalizar('SUCESSO' if resultado.get('ok') else 'FALHA')
        resultado['arquivo_log_individual'] = str(logger.caminho_arquivo)
        return resultado
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        logger.finalizar('ERRO')
        return {
            'ok': False,
            'mensagens': [f'Erro na acao "{nome_acao}": {type(erro).__name__}: {erro}'],
            'arquivo_log_individual': str(logger.caminho_arquivo),
        }


def normalizar_status_e_status_resposta_complicacao(
    arquivo_status,
    arquivo_status_resposta_complicacao,
    saida_status,
    saida_status_resposta,
):
    return _executar_acao_app(
        'normalizar_status_e_status_resposta_complicacao',
        executar_ingestao_complicacao,
        arquivo_status=arquivo_status,
        arquivo_status_resposta_complicacao=arquivo_status_resposta_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
    )


def normalizar_status_e_status_respostas_internacao_eletivo(
    arquivo_status,
    arquivo_status_resposta_eletivo,
    arquivo_status_resposta_internacao,
    arquivo_status_resposta_unificado,
    saida_status,
    saida_status_resposta,
):
    return _executar_acao_app(
        'normalizar_status_e_status_respostas_internacao_eletivo',
        executar_ingestao_unificar,
        arquivo_status=arquivo_status,
        arquivo_status_resposta_eletivo=arquivo_status_resposta_eletivo,
        arquivo_status_resposta_internacao=arquivo_status_resposta_internacao,
        arquivo_status_resposta_unificado=arquivo_status_resposta_unificado,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
    )


def integrar_status_com_status_resposta_complicacao(
    arquivo_status,
    arquivo_status_resposta_complicacao,
    saida_status,
    saida_status_resposta,
    saida_status_integrado,
):
    return _executar_acao_app(
        'integrar_status_com_status_resposta_complicacao',
        run_complicacao_pipeline_enviar_status_com_resposta,
        arquivo_status=arquivo_status,
        arquivo_status_resposta_complicacao=arquivo_status_resposta_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        saida_status_integrado=saida_status_integrado,
    )


def integrar_status_com_status_resposta_internacao_eletivo(
    arquivo_status,
    arquivo_status_resposta_eletivo,
    arquivo_status_resposta_internacao,
    arquivo_status_resposta_unificado,
    saida_status,
    saida_status_resposta,
    saida_status_integrado,
):
    return _executar_acao_app(
        'integrar_status_com_status_resposta_internacao_eletivo',
        run_internacao_eletivo_pipeline_enviar_status_com_resposta,
        arquivo_status=arquivo_status,
        arquivo_status_resposta_eletivo=arquivo_status_resposta_eletivo,
        arquivo_status_resposta_internacao=arquivo_status_resposta_internacao,
        arquivo_status_resposta_unificado=arquivo_status_resposta_unificado,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        saida_status_integrado=saida_status_integrado,
    )


def integrar_status_somente_complicacao(
    arquivo_status,
    saida_status,
    saida_status_integrado,
):
    return _executar_acao_app(
        'integrar_status_somente_complicacao',
        run_complicacao_pipeline_enviar_status_somente_status,
        arquivo_status=arquivo_status,
        saida_status=saida_status,
        saida_status_integrado=saida_status_integrado,
    )


def integrar_status_somente_internacao_eletivo(
    arquivo_status,
    saida_status,
    saida_status_integrado,
):
    return _executar_acao_app(
        'integrar_status_somente_internacao_eletivo',
        run_internacao_eletivo_pipeline_enviar_status_somente_status,
        arquivo_status=arquivo_status,
        saida_status=saida_status,
        saida_status_integrado=saida_status_integrado,
    )


def gerar_dataset_complicacao_com_resposta(
    arquivo_status,
    arquivo_status_resposta_complicacao,
    arquivo_dataset_origem_complicacao,
    saida_status,
    saida_status_resposta,
    saida_status_integrado,
    saida_dataset_status,
):
    return _executar_acao_app(
        'gerar_dataset_complicacao_com_resposta',
        run_complicacao_pipeline_gerar_status_dataset,
        arquivo_status=arquivo_status,
        arquivo_status_resposta_complicacao=arquivo_status_resposta_complicacao,
        arquivo_dataset_origem_complicacao=arquivo_dataset_origem_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        saida_status_integrado=saida_status_integrado,
        saida_dataset_status=saida_dataset_status,
    )


def gerar_dataset_internacao_eletivo_com_resposta(
    arquivo_status,
    arquivo_status_resposta_eletivo,
    arquivo_status_resposta_internacao,
    arquivo_status_resposta_unificado,
    arquivo_dataset_origem_internacao,
    saida_status,
    saida_status_resposta,
    saida_status_integrado,
    saida_dataset_status,
):
    return _executar_acao_app(
        'gerar_dataset_internacao_eletivo_com_resposta',
        run_internacao_eletivo_pipeline_gerar_status_dataset,
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


def gerar_dataset_complicacao_somente_status(
    arquivo_status,
    arquivo_dataset_origem_complicacao,
    saida_status,
    saida_status_integrado,
    saida_dataset_status,
):
    return _executar_acao_app(
        'gerar_dataset_complicacao_somente_status',
        run_complicacao_pipeline_gerar_status_dataset_somente_status,
        arquivo_status=arquivo_status,
        arquivo_dataset_origem_complicacao=arquivo_dataset_origem_complicacao,
        saida_status=saida_status,
        saida_status_integrado=saida_status_integrado,
        saida_dataset_status=saida_dataset_status,
    )


def gerar_dataset_internacao_eletivo_somente_status(
    arquivo_status,
    arquivo_dataset_origem_internacao,
    saida_status,
    saida_status_integrado,
    saida_dataset_status,
):
    return _executar_acao_app(
        'gerar_dataset_internacao_eletivo_somente_status',
        run_internacao_eletivo_pipeline_gerar_status_dataset_somente_status,
        arquivo_status=arquivo_status,
        arquivo_dataset_origem_internacao=arquivo_dataset_origem_internacao,
        saida_status=saida_status,
        saida_status_integrado=saida_status_integrado,
        saida_dataset_status=saida_dataset_status,
    )


def orquestrar_complicacao(
    arquivo_dataset_status,
    arquivo_saida_final,
):
    return _executar_acao_app(
        'orquestrar_complicacao',
        run_complicacao_pipeline_orquestrar,
        arquivo_dataset_status=arquivo_dataset_status,
        arquivo_saida_final=arquivo_saida_final,
        nome_logger='orquestracao_complicacao_app',
    )


def orquestrar_internacao_eletivo(
    arquivo_dataset_status,
    arquivo_saida_final,
):
    return _executar_acao_app(
        'orquestrar_internacao_eletivo',
        run_internacao_eletivo_pipeline_orquestrar,
        arquivo_dataset_status=arquivo_dataset_status,
        arquivo_saida_final=arquivo_saida_final,
        nome_logger='orquestracao_internacao_eletivo_app',
    )
