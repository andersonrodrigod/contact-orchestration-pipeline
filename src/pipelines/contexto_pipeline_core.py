from core.pipeline_result import ok_result


def _montar_resultado_final(resultado_status, resultado_orquestracao):
    metricas = {
        'total_status': resultado_status.get('total_status', 0),
        'com_match': resultado_status.get('com_match', 0),
        'sem_match': resultado_status.get('sem_match', 0),
        'total_linhas': resultado_status.get('total_linhas', 0),
    }
    for chave in [
        'descartados_status_data_invalida',
        'descartados_resposta_data_invalida',
        'nat_data_agendamento',
        'pct_nat_data_agendamento',
        'nat_dt_atendimento',
        'pct_nat_dt_atendimento',
        'limiar_nat_data_em_uso',
    ]:
        if chave in resultado_status:
            metricas[chave] = resultado_status.get(chave)

    metricas_por_etapa = {
        **resultado_status.get('metricas_por_etapa', {}),
        'orquestracao': {
            'total_usuarios': resultado_orquestracao.get('total_usuarios', 0),
            'total_usuarios_resolvidos': resultado_orquestracao.get(
                'total_usuarios_resolvidos', 0
            ),
        },
    }
    return ok_result(
        mensagens=(
            resultado_status.get('mensagens', [])
            + resultado_orquestracao.get('mensagens', [])
        ),
        metricas=metricas,
        arquivos={
            'arquivo_status_dataset': resultado_status.get('arquivo_status_dataset'),
            'arquivo_saida': resultado_orquestracao.get('arquivo_saida'),
        },
        dados={
            'qualidade_data': resultado_status.get('qualidade_data', {}),
            'metricas_por_etapa': metricas_por_etapa,
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
