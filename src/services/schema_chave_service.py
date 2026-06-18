COLUNA_CHAVE_SENHA = 'CHAVE_SENHA'


def extrair_chave_principal_valor(valor):
    texto = str(valor).strip()
    if texto == '':
        return ''
    return texto.rsplit('_', 1)[-1].strip()


def _adicionar_chave_extraida(df, colunas_origem, coluna_destino):
    df_resultado = df.copy()
    if coluna_destino not in df_resultado.columns:
        df_resultado[coluna_destino] = ''
    else:
        df_resultado[coluna_destino] = df_resultado[coluna_destino].astype(str).str.strip()

    for origem in colunas_origem:
        if origem == coluna_destino or origem not in df_resultado.columns:
            continue

        chave_origem = df_resultado[origem].apply(extrair_chave_principal_valor)
        mask_vazia = df_resultado[coluna_destino].astype(str).str.strip() == ''
        df_resultado.loc[mask_vazia, coluna_destino] = chave_origem.loc[mask_vazia]

    return df_resultado


def adicionar_chave_senha(df, colunas_origem, coluna_destino=COLUNA_CHAVE_SENHA):
    return _adicionar_chave_extraida(df, colunas_origem, coluna_destino=coluna_destino)
