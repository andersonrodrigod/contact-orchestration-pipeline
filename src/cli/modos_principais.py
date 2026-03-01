from core.logger import PipelineLogger
from core.pipeline_result import PipelineResult
from src.pipelines.complicacao_pipeline import (
    run_complicacao_pipeline_finalizar,
    run_complicacao_pipeline_gerar_status_dataset,
    run_pipeline_complicacao_com_resposta,
    run_pipeline_complicacao_somente_status,
)
from src.pipelines.internacao_eletivo_pipeline import (
    run_internacao_eletivo_pipeline_finalizar,
    run_internacao_eletivo_pipeline_gerar_status_dataset,
    run_pipeline_internacao_eletivo_com_resposta,
    run_pipeline_internacao_eletivo_somente_status,
)


MODOS_PRINCIPAIS = {
    'complicacao_com_resposta': run_pipeline_complicacao_com_resposta,
    'complicacao': run_pipeline_complicacao_com_resposta,
    'complicacao_somente_status': run_pipeline_complicacao_somente_status,
    'complicacao_gerar_status_dataset': run_complicacao_pipeline_gerar_status_dataset,
    'complicacao_finalizar_status': run_complicacao_pipeline_finalizar,
    'internacao_eletivo_com_resposta': run_pipeline_internacao_eletivo_com_resposta,
    'internacao_eletivo': run_pipeline_internacao_eletivo_com_resposta,
    'internacao_eletivo_somente_status': run_pipeline_internacao_eletivo_somente_status,
    'internacao_eletivo_gerar_status_dataset': run_internacao_eletivo_pipeline_gerar_status_dataset,
    'internacao_eletivo_finalizar_status': run_internacao_eletivo_pipeline_finalizar,
}

MODOS_AGREGADOS = ['ambos_com_resposta', 'ambos_somente_status', 'ambos']


def executar_modo_ambos(modo):
    if modo in ['ambos_com_resposta', 'ambos']:
        logger_ambos = PipelineLogger(nome_pipeline='main_ambos_com_resposta')
        resultado_complicacao = run_pipeline_complicacao_com_resposta()
        resultado_internacao_eletivo = run_pipeline_internacao_eletivo_com_resposta()
    else:
        logger_ambos = PipelineLogger(nome_pipeline='main_ambos_somente_status')
        resultado_complicacao = run_pipeline_complicacao_somente_status()
        resultado_internacao_eletivo = run_pipeline_internacao_eletivo_somente_status()

    ok_complicacao = resultado_complicacao.get('ok', False)
    ok_internacao = resultado_internacao_eletivo.get('ok', False)
    ok_geral = ok_complicacao and ok_internacao

    logger_ambos.info('RESULTADO_EXECUCAO', f'complicacao_ok={ok_complicacao}')
    logger_ambos.info('RESULTADO_EXECUCAO', f'internacao_eletivo_ok={ok_internacao}')

    mensagens_complicacao = resultado_complicacao.get('mensagens', [])
    mensagens_internacao = resultado_internacao_eletivo.get('mensagens', [])

    if not ok_complicacao:
        logger_ambos.error('FALHA_PARCIAL', 'Falha na execucao: complicacao')
        for mensagem in mensagens_complicacao:
            logger_ambos.error('FALHA_PARCIAL', f'complicacao: {mensagem}')

    if not ok_internacao:
        logger_ambos.error('FALHA_PARCIAL', 'Falha na execucao: internacao_eletivo')
        for mensagem in mensagens_internacao:
            logger_ambos.error('FALHA_PARCIAL', f'internacao_eletivo: {mensagem}')

    if ok_geral:
        mensagem_resumo = 'Execucao em modo ambos finalizada com sucesso.'
        logger_ambos.finalizar('SUCESSO')
    elif ok_complicacao != ok_internacao:
        mensagem_resumo = (
            'Execucao em modo ambos finalizada com falha parcial. '
            'Verifique qual pipeline falhou nas mensagens e no log.'
        )
        logger_ambos.finalizar('FALHA_PARCIAL')
    else:
        mensagem_resumo = 'Execucao em modo ambos finalizada com falha total.'
        logger_ambos.finalizar('FALHA_TOTAL')

    return PipelineResult(
        ok=ok_geral,
        mensagens=[
            mensagem_resumo,
            f'complicacao_ok={ok_complicacao}',
            f'internacao_eletivo_ok={ok_internacao}',
        ],
        dados={
            'resultados': {
                'complicacao': resultado_complicacao,
                'internacao_eletivo': resultado_internacao_eletivo,
            },
        },
    ).to_dict()
