import pandas as pd


COLUNA_RESPOSTA_CANONICA = 'resposta'
COLUNA_DATA_ATENDIMENTO_CANONICA = 'DT_ATENDIMENTO'
ALIASES_COLUNA_RESPOSTA = (
    COLUNA_RESPOSTA_CANONICA,
    'Resposta',
    'RESPOSTA',
)
ALIASES_COLUNA_DATA_ATENDIMENTO = (
    COLUNA_DATA_ATENDIMENTO_CANONICA,
    'dat_atendimento',
)


def colunas_resposta_presentes(df):
    return [col for col in ALIASES_COLUNA_RESPOSTA if col in df.columns]


def tem_coluna_resposta(df):
    return len(colunas_resposta_presentes(df)) > 0


def aliases_resposta_legado_presentes(df):
    return [
        col
        for col in ALIASES_COLUNA_RESPOSTA
        if col != COLUNA_RESPOSTA_CANONICA and col in df.columns
    ]


def colunas_data_atendimento_presentes(df):
    return [col for col in ALIASES_COLUNA_DATA_ATENDIMENTO if col in df.columns]


def tem_coluna_data_atendimento(df):
    return len(colunas_data_atendimento_presentes(df)) > 0


def aliases_data_atendimento_legado_presentes(df):
    return [
        col
        for col in ALIASES_COLUNA_DATA_ATENDIMENTO
        if col != COLUNA_DATA_ATENDIMENTO_CANONICA and col in df.columns
    ]


def diagnosticar_coluna_resposta(df):
    aliases_presentes = colunas_resposta_presentes(df)
    qtd_linhas_conflito = 0

    if len(aliases_presentes) > 1:
        df_aliases = df[aliases_presentes].fillna('').astype(str).apply(lambda s: s.str.strip())

        def _ha_conflito(row):
            valores = {valor for valor in row if valor != ''}
            return len(valores) > 1

        qtd_linhas_conflito = int(df_aliases.apply(_ha_conflito, axis=1).sum())

    return {
        'aliases_presentes': aliases_presentes,
        'qtd_aliases_presentes': len(aliases_presentes),
        'qtd_linhas_conflito': qtd_linhas_conflito,
    }


def garantir_contrato_resposta_canonica(df, contexto=''):
    erros = []
    if COLUNA_RESPOSTA_CANONICA not in df.columns:
        erros.append(f'coluna obrigatoria ausente: {COLUNA_RESPOSTA_CANONICA}')

    legado = aliases_resposta_legado_presentes(df)
    if len(legado) > 0:
        erros.append(f'aliases legados presentes: {legado}')

    if len(erros) > 0:
        prefixo = f'Contrato de resposta invalido em {contexto}: ' if contexto else 'Contrato de resposta invalido: '
        raise ValueError(prefixo + '; '.join(erros))
    return df


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


def normalizar_coluna_data_atendimento(df, remover_alias=True):
    presentes = colunas_data_atendimento_presentes(df)
    if COLUNA_DATA_ATENDIMENTO_CANONICA in df.columns:
        if remover_alias:
            legado = aliases_data_atendimento_legado_presentes(df)
            if len(legado) > 0:
                df = df.drop(columns=legado, errors='ignore')
        return df

    if 'dat_atendimento' in df.columns:
        df = df.rename(columns={'dat_atendimento': COLUNA_DATA_ATENDIMENTO_CANONICA})
    return df
