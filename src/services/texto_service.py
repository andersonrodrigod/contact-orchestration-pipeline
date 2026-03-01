import re
import unicodedata

import pandas as pd

from src.services.normalizacao_services import corrigir_texto_bugado


def normalizar_texto_serie(serie):
    return serie.fillna('').astype(str).str.strip()


def normalizar_nome_serie(serie):
    return normalizar_texto_serie(serie).str.upper()


def limpar_valor_texto(valor):
    if pd.isna(valor):
        return ''

    texto = str(valor).strip()
    texto = corrigir_texto_bugado(texto)
    if texto in {'0', '0.0', 'nan', 'NaN', 'None'}:
        return ''
    return texto


def limpar_coluna_texto(df, coluna):
    if coluna in df.columns:
        df[coluna] = df[coluna].apply(limpar_valor_texto)
    return df


def simplificar_texto(valor):
    texto = limpar_valor_texto(valor).lower()
    texto = ''.join(
        caractere for caractere in unicodedata.normalize('NFKD', texto)
        if not unicodedata.combining(caractere)
    )
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto
