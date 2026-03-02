"""Regras simples de padronizacao de nomes de colunas entre fontes."""


def padronizar_colunas_status(df):
    # Mantido explicitamente para futura evolucao de padronizacao do status.
    return df


def padronizar_colunas_status_resposta(df):
    mapa_colunas = {
        'dat_atendimento': 'DT_ATENDIMENTO',
        'Resposta': 'resposta',
        'RESPOSTA': 'resposta',
    }
    df = df.rename(columns=mapa_colunas)
    if 'resposta' not in df.columns:
        df['resposta'] = ''
    return df
