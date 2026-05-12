import pandas as pd
from pathlib import Path


def _limpar_colunas_excel(df):
    colunas_manter = []
    for coluna in df.columns:
        nome_coluna = str(coluna).strip()
        if nome_coluna == "" or nome_coluna.lower().startswith("unnamed:"):
            serie = df[coluna]
            if serie.fillna("").astype(str).str.strip().eq("").all():
                continue
        colunas_manter.append(coluna)
    if len(colunas_manter) == len(df.columns):
        return df
    return df.loc[:, colunas_manter].copy()


def _delimitador_mais_provavel(caminho_arquivo):
    encodings = ['utf-8-sig', 'utf-8', 'cp1252', 'latin1']
    for encoding in encodings:
        try:
            with open(caminho_arquivo, 'r', encoding=encoding) as arquivo:
                primeira_linha = arquivo.readline()
            if primeira_linha.count(';') >= primeira_linha.count(','):
                return ';'
            return ','
        except UnicodeDecodeError:
            continue
        except OSError:
            break
    return ';'


def ler_arquivo_csv(caminho_arquivo, separador=';'):
    caminho_texto = str(caminho_arquivo).lower()
    if caminho_texto.endswith('.xlsx') or caminho_texto.endswith('.xls'):
        df = pd.read_excel(caminho_arquivo, dtype=str, keep_default_na=False)
        return _limpar_colunas_excel(df)

    encodings = ['utf-8-sig', 'utf-8', 'cp1252', 'latin1']
    delimitador_provavel = _delimitador_mais_provavel(caminho_arquivo)
    separadores_teste = [delimitador_provavel]
    if separador not in separadores_teste:
        separadores_teste.append(separador)
    if ';' not in separadores_teste:
        separadores_teste.append(';')
    if ',' not in separadores_teste:
        separadores_teste.append(',')

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
            except (UnicodeDecodeError, pd.errors.ParserError, ValueError):
                continue

    raise UnicodeDecodeError(
        'csv',
        b'',
        0,
        1,
        f'Nao foi possivel ler {caminho_arquivo} com os encodings: {encodings}',
    )


def salvar_dataframe(df, caminho_arquivo, separador=';'):
    caminho = Path(caminho_arquivo)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho_texto = str(caminho_arquivo).lower()
    if caminho_texto.endswith('.xlsx') or caminho_texto.endswith('.xls'):
        df.to_excel(caminho_arquivo, index=False)
        return

    df.to_csv(caminho_arquivo, sep=separador, index=False, encoding='utf-8-sig')


def arquivo_existe(caminho_arquivo):
    try:
        return Path(caminho_arquivo).exists()
    except OSError:
        return False


def validar_arquivos_existem(caminhos_por_nome):
    faltando = []
    for nome, caminho in caminhos_por_nome.items():
        if not arquivo_existe(caminho):
            faltando.append(f'{nome}: {caminho}')

    if len(faltando) > 0:
        return {
            'ok': False,
            'mensagens': ['Arquivos obrigatorios nao encontrados:'] + faltando,
            'faltando': faltando,
        }

    return {
        'ok': True,
        'mensagens': ['Todos os arquivos obrigatorios foram encontrados.'],
        'faltando': [],
    }

