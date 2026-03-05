from core.pipeline_result import ok_result


def _montar_resultado_final(resultado_status, resultado_orquestracao):
    return ok_result(
        mensagens=(
            resultado_status.get('mensagens', [])
            + resultado_orquestracao.get('mensagens', [])
        ),
        metricas={
            'total_status': resultado_status.get('total_status', 0),
            'com_match': resultado_status.get('com_match', 0),
            'sem_match': resultado_status.get('sem_match', 0),
            'total_linhas': resultado_status.get('total_linhas', 0),
        },
        arquivos={
            'arquivo_status_dataset': resultado_status.get('arquivo_status_dataset'),
            'arquivo_saida': resultado_orquestracao.get('arquivo_saida'),
        },
    )


def run_pipeline_contexto_com_resposta(
    funcao_status_dataset,
    kwargs_status_dataset,
    funcao_orquestracao,
    kwargs_orquestracao,
):
    resultado_status = funcao_status_dataset(**kwargs_status_dataset)
    if not resultado_status.get('ok'):
        return resultado_status

    resultado_orquestracao = funcao_orquestracao(**kwargs_orquestracao)
    if not resultado_orquestracao.get('ok'):
        return resultado_orquestracao

    return _montar_resultado_final(resultado_status, resultado_orquestracao)


def run_pipeline_contexto_somente_status(
    funcao_status_dataset,
    kwargs_status_dataset,
    funcao_orquestracao,
    kwargs_orquestracao,
):
    resultado_status = funcao_status_dataset(**kwargs_status_dataset)
    if not resultado_status.get('ok'):
        return resultado_status

    resultado_orquestracao = funcao_orquestracao(**kwargs_orquestracao)
    if not resultado_orquestracao.get('ok'):
        return resultado_orquestracao

    return _montar_resultado_final(resultado_status, resultado_orquestracao)
