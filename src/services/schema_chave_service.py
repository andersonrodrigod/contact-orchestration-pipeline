COLUNA_CHAVE_PRINCIPAL = 'CHAVE PRINCIPAL'


def extrair_chave_principal_valor(valor):
    texto = str(valor).strip()
    if texto == '':
        return ''
    return texto.rsplit('_', 1)[-1].strip()


def adicionar_chave_principal(df, colunas_origem, coluna_destino=COLUNA_CHAVE_PRINCIPAL):
    df_resultado = df.copy()
    origem = next((col for col in colunas_origem if col in df_resultado.columns), None)
    if origem is None:
        if coluna_destino not in df_resultado.columns:
            df_resultado[coluna_destino] = ''
        return df_resultado

    df_resultado[coluna_destino] = df_resultado[origem].apply(extrair_chave_principal_valor)
    return df_resultado
