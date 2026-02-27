import pandas as pd


def ler_arquivo_csv(caminho_arquivo, separador=';'):
    encodings = ['utf-8-sig', 'utf-8', 'cp1252', 'latin1']

    for encoding in encodings:
        try:
            return pd.read_csv(
                caminho_arquivo,
                sep=separador,
                dtype=str,
                encoding=encoding,
                keep_default_na=False,
            )
        except UnicodeDecodeError:
            continue

    raise UnicodeDecodeError(
        'csv',
        b'',
        0,
        1,
        f'Nao foi possivel ler {caminho_arquivo} com os encodings: {encodings}',
    )
