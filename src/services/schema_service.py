def padronizar_colunas_status(df):
    return df


def padronizar_colunas_status_resposta(df):
    mapa_colunas = {
        'dat_atendimento': 'DT_ATENDIMENTO',
    }
    return df.rename(columns=mapa_colunas)
