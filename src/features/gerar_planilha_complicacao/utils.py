import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd


def primeira_palavra(valor):
    texto = str(valor).strip().upper()
    if not texto:
        return ""
    return texto.split()[0]


def normalizar_chave(valor):
    if pd.isna(valor):
        return ""

    texto = str(valor).strip()
    if texto.lower() in {"nan", "none", "<na>"}:
        return ""

    if re.fullmatch(r"\d+\.0+", texto):
        return texto.split(".")[0]

    return texto


def normalizar_telefone(valor):
    if pd.isna(valor):
        return ""
    if isinstance(valor, bool):
        return ""
    if isinstance(valor, int):
        return str(valor)
    if isinstance(valor, float):
        if pd.isna(valor):
            return ""
        if float(valor).is_integer():
            return str(int(valor))
        texto_float = format(valor, "f").rstrip("0").rstrip(".")
        return re.sub(r"\D", "", texto_float)

    texto = str(valor).strip()
    if texto.lower() in {"nan", "none", "<na>"}:
        return ""

    texto_num = texto.replace(",", ".")
    if re.fullmatch(r"[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?", texto_num):
        try:
            valor_decimal = Decimal(texto_num)
            if valor_decimal == valor_decimal.to_integral_value():
                return str(int(valor_decimal))
        except (InvalidOperation, ValueError):
            pass

    return re.sub(r"\D", "", texto)


def ajustar_nono_digito(telefone):
    if telefone == "":
        return telefone

    if len(telefone) >= 8:
        oitavo_da_direita = telefone[-8]
        if oitavo_da_direita in {"2", "3", "4", "5"}:
            return telefone

    if len(telefone) == 12:
        return telefone[:4] + "9" + telefone[4:]

    return telefone


def primeiro_nao_vazio(serie):
    for valor in serie:
        if isinstance(valor, str) and valor != "":
            return valor
    return ""


def validar_merge_sem_duplicar_linhas(qtd_antes, df_depois, nome_merge):
    qtd_depois = len(df_depois)
    if qtd_depois != qtd_antes:
        raise ValueError(
            f"O merge por {nome_merge} alterou a quantidade de linhas da base "
            f"({qtd_antes} -> {qtd_depois}). Verifique duplicidades na chave antes de continuar."
        )


def detectar_mes_referencia(serie):
    textos = serie.dropna().astype(str).str.strip()
    meses = textos.str.extract(r"^\d{1,2}/(\d{1,2})/\d{4}$", expand=False)
    meses = pd.to_numeric(meses, errors="coerce").dropna()

    if meses.empty:
        return None

    return int(meses.mode().iloc[0])


def normalizar_data_internacao(df):
    df = df.copy()
    mes_referencia = detectar_mes_referencia(df["DT INTERNACAO"])

    def normalizar(valor):
        if pd.isna(valor) or valor == "":
            return pd.NaT

        if isinstance(valor, datetime):
            if (
                mes_referencia is not None
                and valor.day == mes_referencia
                and valor.month != mes_referencia
            ):
                return pd.Timestamp(year=valor.year, month=mes_referencia, day=valor.month)
            return pd.Timestamp(valor)

        return pd.to_datetime(valor, errors="coerce", dayfirst=True)

    df["DT INTERNACAO"] = df["DT INTERNACAO"].apply(normalizar)
    return df


def ordenar_por_data_internacao(df):
    df = df.copy()
    data_ordenacao = pd.to_datetime(df["DT INTERNACAO"], errors="coerce", dayfirst=True)
    df["_DT_INTERNACAO_ORDENACAO"] = data_ordenacao
    df = df.sort_values(
        by=["_DT_INTERNACAO_ORDENACAO", "COD USUARIO"],
        ascending=[True, True],
        na_position="last",
    )
    return df.drop(columns=["_DT_INTERNACAO_ORDENACAO"]).reset_index(drop=True)


def sinalizar_duplicidade(df):
    df = df.copy()
    codigo_usuario = df["COD USUARIO"].fillna("").astype(str).str.strip()
    df["DUPLICIDADE"] = codigo_usuario.ne("") & codigo_usuario.duplicated(keep=False)
    return df


def sinalizar_duplicado_por_usuario_idade(df):
    df = df.copy()
    usuario = df["USUARIO"].fillna("").astype(str).str.strip().str.upper()
    idade = df["IDADE T"].fillna("").astype(str).str.strip()
    chave_valida = usuario.ne("") & idade.ne("")
    duplicado = chave_valida & df.assign(_USUARIO_DUP=usuario, _IDADE_DUP=idade).duplicated(
        subset=["_USUARIO_DUP", "_IDADE_DUP"],
        keep=False,
    )
    df["DUPLICADO"] = duplicado.map({True: "VERDADEIRO", False: "FALSO"})
    return df


def adicionar_motivo_exclusao(df, motivo):
    df = df.copy()
    df["MOTIVO_EXCLUSAO"] = motivo
    return df


def juntar_excluidos(lista_excluidos, colunas_base):
    lista_validada = [df for df in lista_excluidos if not df.empty]
    colunas_excluidos = list(colunas_base) + ["MOTIVO_EXCLUSAO"]

    if not lista_validada:
        return pd.DataFrame(columns=colunas_excluidos)

    return pd.concat(lista_validada, ignore_index=True).reindex(columns=colunas_excluidos)
