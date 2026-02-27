def padronizar_colunas_status(df):
    mapa_colunas = {
        'Data agendamento': 'DT_ENVIO',
    }
    return df.rename(columns=mapa_colunas)


def padronizar_colunas_status_resposta(df):
    mapa_colunas = {
        'dat_atendimento': 'DT_ATENDIMENTO',
    }
    return df.rename(columns=mapa_colunas)


def validar_colunas_origem_para_padronizacao(df_status, df_status_resposta):
    resultado = {
        'ok': True,
        'mensagens': [],
    }

    if 'Data agendamento' not in df_status.columns:
        resultado['ok'] = False
        resultado['mensagens'].append(
            'Coluna obrigatoria Data agendamento nao encontrada no arquivo status.'
        )

    if 'dat_atendimento' not in df_status_resposta.columns:
        resultado['ok'] = False
        resultado['mensagens'].append(
            'Coluna obrigatoria dat_atendimento nao encontrada no arquivo status_resposta.'
        )

    if resultado['ok']:
        resultado['mensagens'].append(
            'Colunas de origem para padronizacao foram encontradas com sucesso.'
        )

    return resultado


def validar_padronizacao_colunas_data(df_status, df_status_resposta):
    resultado = {
        'ok': True,
        'mensagens': [],
    }

    if 'DT_ENVIO' not in df_status.columns:
        resultado['ok'] = False
        resultado['mensagens'].append('Coluna DT_ENVIO nao encontrada no arquivo status.')
    else:
        qtd_nulos = int(df_status['DT_ENVIO'].isna().sum())
        if qtd_nulos > 0:
            resultado['ok'] = False
            resultado['mensagens'].append(
                f'Coluna DT_ENVIO possui {qtd_nulos} valores invalidos ou vazios.'
            )

    if 'DT_ATENDIMENTO' not in df_status_resposta.columns:
        resultado['ok'] = False
        resultado['mensagens'].append(
            'Coluna DT_ATENDIMENTO nao encontrada no arquivo status_resposta.'
        )
    else:
        qtd_nulos = int(df_status_resposta['DT_ATENDIMENTO'].isna().sum())
        if qtd_nulos > 0:
            resultado['ok'] = False
            resultado['mensagens'].append(
                f'Coluna DT_ATENDIMENTO possui {qtd_nulos} valores invalidos ou vazios.'
            )

    if resultado['ok']:
        resultado['mensagens'].append('Padronizacao de datas validada com sucesso.')

    return resultado
