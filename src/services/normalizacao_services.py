import pandas as pd


def corrigir_texto_bugado(texto):
    if pd.isna(texto):
        return texto

    texto = str(texto)

    trocas = {
        'Voc\u03a9s': 'Voc\u00eas',
        'N\u03c0o': 'N\u00e3o',
        'n\u03c0o': 'n\u00e3o',
        'n\u00cf\u20aco': 'n\u00e3o',
        'Complica\u00cf\u201e\u00e2\u0152\u00a1es': 'Complica\u00e7\u00f5es',
        'N\u00c2\u00b7mero \u00ce\u02dc': 'N\u00famero \u00e9',
        'N\u00c2\u00b7mero': 'N\u00famero',
        'Usu\u00c3\u0178rio': 'Usu\u00e1rio',
        'Mensagem n\u00cf\u20aco': 'Mensagem n\u00e3o',
        '\u00cf\u20ac': '\u00e3',
        '\u00ce\u02dc': '\u00e9',
        '\u00c3\u00a7': '\u00e7',
        '\u00c3\u00a3': '\u00e3',
        '\u00c3\u00a1': '\u00e1',
        '\u00c3\u00a9': '\u00e9',
        '\u00c3\u00aa': '\u00ea',
        '\u00c3\u00b3': '\u00f3',
        '\u00c3\u00ba': '\u00fa',
        '\u00c3\u00b5': '\u00f5',
    }

    for antigo, novo in trocas.items():
        texto = texto.replace(antigo, novo)

    return texto


def limpar_texto_dataframe(df):
    for coluna in df.columns:
        if df[coluna].dtype == 'object':
            df[coluna] = df[coluna].apply(corrigir_texto_bugado)
    return df


def normalizar_tipos_dataframe(df, colunas_data=None):
    if colunas_data is None:
        colunas_data = []

    for coluna in df.columns:
        if coluna not in colunas_data:
            df[coluna] = df[coluna].astype(str)

    for coluna_data in colunas_data:
        if coluna_data in df.columns:
            df[coluna_data] = pd.to_datetime(
                df[coluna_data],
                errors='coerce',
                dayfirst=True,
            )

    return df


def limpar_texto_exceto_colunas(df, colunas_ignorar=None):
    if colunas_ignorar is None:
        colunas_ignorar = []

    for coluna in df.columns:
        if coluna in colunas_ignorar:
            continue
        if df[coluna].dtype == 'object':
            df[coluna] = df[coluna].apply(corrigir_texto_bugado)

    return df


def formatar_coluna_data_br(df, coluna):
    if coluna in df.columns:
        df[coluna] = df[coluna].dt.strftime('%d/%m/%Y')

    return df


def criar_coluna_dt_envio_por_data_agendamento(
    df,
    coluna_origem='Data agendamento',
    coluna_destino='DT ENVIO',
):
    if coluna_origem in df.columns:
        serie_data = pd.to_datetime(df[coluna_origem], errors='coerce', dayfirst=True)
        df[coluna_destino] = serie_data.dt.strftime('%d/%m/%Y')

    return df
