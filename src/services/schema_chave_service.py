COLUNA_CHAVE_SENHA = 'CHAVE_SENHA'


def extrair_chave_principal_valor(valor):
    texto = str(valor).strip()
    if texto == '':
        return ''
    return texto.rsplit('_', 1)[-1].strip()


def _adicionar_chave_extraida(df, colunas_origem, coluna_destino):
    df_resultado = df.copy()
    origem = next((col for col in colunas_origem if col in df_resultado.columns), None)
    if origem is None:
        if coluna_destino not in df_resultado.columns:
            df_resultado[coluna_destino] = ''
        return df_resultado

    df_resultado[coluna_destino] = df_resultado[origem].apply(extrair_chave_principal_valor)
    return df_resultado


def adicionar_chave_senha(df, colunas_origem, coluna_destino=COLUNA_CHAVE_SENHA):
    return _adicionar_chave_extraida(df, colunas_origem, coluna_destino=coluna_destino)
