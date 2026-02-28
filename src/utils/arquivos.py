import pandas as pd


def ler_arquivo_csv(caminho_arquivo, separador=';'):
    encodings = ['utf-8-sig', 'utf-8', 'cp1252', 'latin1']
    separadores_teste = [separador]
    if separador == ';':
        separadores_teste.append(',')
    elif separador == ',':
        separadores_teste.append(';')

    for sep in separadores_teste:
        for encoding in encodings:
            try:
                df = pd.read_csv(
                    caminho_arquivo,
                    sep=sep,
                    dtype=str,
                    encoding=encoding,
                    keep_default_na=False,
                )

                # Se leu com 1 coluna e o cabecalho ainda contem delimitador,
                # tenta outro separador antes de aceitar o resultado.
                if len(df.columns) == 1:
                    nome_coluna = str(df.columns[0])
                    if ';' in nome_coluna or ',' in nome_coluna:
                        continue

                return df
            except UnicodeDecodeError:
                continue

    raise UnicodeDecodeError(
        'csv',
        b'',
        0,
        1,
        f'Nao foi possivel ler {caminho_arquivo} com os encodings: {encodings}',
    )

