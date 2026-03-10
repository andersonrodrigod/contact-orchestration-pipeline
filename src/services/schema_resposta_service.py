import pandas as pd


COLUNA_RESPOSTA_CANONICA = 'resposta'
ALIASES_COLUNA_RESPOSTA = (
    COLUNA_RESPOSTA_CANONICA,
    'Resposta',
    'RESPOSTA',
)


def colunas_resposta_presentes(df):
    return [col for col in ALIASES_COLUNA_RESPOSTA if col in df.columns]


def tem_coluna_resposta(df):
    return len(colunas_resposta_presentes(df)) > 0


def normalizar_coluna_resposta(
    df,
    criar_vazia=True,
    remover_alias=True,
):
    presentes = colunas_resposta_presentes(df)

    if len(presentes) == 0:
        if criar_vazia and COLUNA_RESPOSTA_CANONICA not in df.columns:
            df[COLUNA_RESPOSTA_CANONICA] = ''
        return df

    resposta_canonica = pd.Series('', index=df.index, dtype='object')
    for coluna in ALIASES_COLUNA_RESPOSTA:
        if coluna not in df.columns:
            continue
        serie = df[coluna].fillna('').astype(str).str.strip()
        mask_preencher = (resposta_canonica == '') & (serie != '')
        resposta_canonica.loc[mask_preencher] = serie.loc[mask_preencher]

    df[COLUNA_RESPOSTA_CANONICA] = resposta_canonica

    if remover_alias:
        colunas_dropar = [
            col
            for col in presentes
            if col != COLUNA_RESPOSTA_CANONICA
        ]
        if len(colunas_dropar) > 0:
            df = df.drop(columns=colunas_dropar, errors='ignore')

    return df
