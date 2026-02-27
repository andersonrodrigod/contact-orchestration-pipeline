import pandas as pd
import os


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


def salvar_output_validacao(resultado, arquivo_output):
    pasta = os.path.dirname(arquivo_output)
    if pasta:
        os.makedirs(pasta, exist_ok=True)

    with open(arquivo_output, 'w', encoding='utf-8') as arquivo:
        arquivo.write(f"OK={resultado['ok']}\n")
        for mensagem in resultado['mensagens']:
            arquivo.write(f"- {mensagem}\n")
