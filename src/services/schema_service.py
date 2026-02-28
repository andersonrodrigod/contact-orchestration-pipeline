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
