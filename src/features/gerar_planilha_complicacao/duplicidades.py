import re

import pandas as pd

from src.features.gerar_planilha_complicacao.config import PALAVRAS_PRIORITARIAS
from src.features.gerar_planilha_complicacao.utils import adicionar_motivo_exclusao, juntar_excluidos, primeira_palavra


def remover_parto_laqueadura(df):
    # Remove procedimentos de parto e laqueadura da base ajustada e separa as linhas excluidas para conferencia.
    procedimento = df["PROCEDIMENTO"].fillna("").astype(str).str.upper().str.strip()
    mascara_excluir = procedimento.str.contains("PARTO|LAQUEADURA", na=False)

    df_filtrado = df[~mascara_excluir].copy()
    df_excluidos = adicionar_motivo_exclusao(df[mascara_excluir], "PARTO OU LAQUEADURA")

    return df_filtrado, df_excluidos


def remover_duplicados_por_regras(df):
    # Aplica as regras de duplicidade por COD USUARIO e separa as linhas excluidas para auditoria.
    df = df.copy()
    df["COD USUARIO"] = df["COD USUARIO"].astype(str).str.strip()
    df["PROCEDIMENTO"] = df["PROCEDIMENTO"].astype(str).str.upper().str.strip()

    padrao_prioridades = r"\b(?:" + "|".join(re.escape(p) for p in PALAVRAS_PRIORITARIAS) + r")\b"

    duplicados = df[df["COD USUARIO"].duplicated(keep=False)]
    nao_duplicados = df[~df["COD USUARIO"].duplicated(keep=False)]

    mantidos = []
    excluidos = []

    for codigo, grupo in duplicados.groupby("COD USUARIO"):
        mascara_prioridade = grupo["PROCEDIMENTO"].str.contains(padrao_prioridades, na=False, regex=True)

        if mascara_prioridade.any():
            manter = grupo[mascara_prioridade]
            excluir = grupo.drop(manter.index)
            motivo = "DUPLICADO - MANTIDO PROCEDIMENTO PRIORITARIO"

        elif grupo["PROCEDIMENTO"].str.contains("INTERNACAO", na=False).any():
            manter = grupo[~grupo["PROCEDIMENTO"].str.contains("INTERNACAO", na=False)]
            excluir = grupo.drop(manter.index)
            motivo = "DUPLICADO - INTERNACAO"

            if manter.empty:
                manter = grupo
                excluir = grupo.iloc[0:0]

        else:
            manter = grupo
            excluir = grupo.iloc[0:0]
            motivo = ""

        mantidos.append(manter)

        if not excluir.empty:
            excluidos.append(adicionar_motivo_exclusao(excluir, motivo))

    if mantidos:
        df_mantidos = pd.concat(mantidos + [nao_duplicados], ignore_index=True)
    else:
        df_mantidos = nao_duplicados.copy()

    df_excluidos = juntar_excluidos(excluidos, df.columns)

    duplicados_finais = df_mantidos[df_mantidos["COD USUARIO"].duplicated(keep=False)]

    for codigo, grupo in duplicados_finais.groupby("COD USUARIO"):
        if grupo["PROCEDIMENTO"].str.contains("INTERNACAO", na=False).all():
            excluir_restante = grupo.iloc[1:]

            df_mantidos = df_mantidos.drop(excluir_restante.index)
            df_excluidos = pd.concat(
                [
                    df_excluidos,
                    adicionar_motivo_exclusao(excluir_restante, "DUPLICADO - INTERNACAO REPETIDA"),
                ],
                ignore_index=True,
            )

    mascara_cd_dup = df_mantidos["COD USUARIO"].duplicated(keep=False)
    mascara_proc_exato = df_mantidos.duplicated(
        subset=["COD USUARIO", "PROCEDIMENTO"], keep="first"
    )
    descartar_proc_exato = mascara_cd_dup & mascara_proc_exato

    linhas_descartadas_exatas = df_mantidos[descartar_proc_exato]
    df_mantidos = df_mantidos[~descartar_proc_exato]
    df_excluidos = pd.concat(
        [
            df_excluidos,
            adicionar_motivo_exclusao(linhas_descartadas_exatas, "DUPLICADO - PROCEDIMENTO REPETIDO"),
        ],
        ignore_index=True,
    )

    df_mantidos["_P1_PROC"] = df_mantidos["PROCEDIMENTO"].apply(primeira_palavra)
    mascara_cd_dup_2 = df_mantidos["COD USUARIO"].duplicated(keep=False)
    mascara_p1_repete = df_mantidos.duplicated(
        subset=["COD USUARIO", "_P1_PROC"], keep="first"
    )
    descartar_primeira_palavra = mascara_cd_dup_2 & mascara_p1_repete

    linhas_descartadas_p1 = df_mantidos[descartar_primeira_palavra].drop(columns=["_P1_PROC"])
    df_mantidos = df_mantidos[~descartar_primeira_palavra].drop(columns=["_P1_PROC"])
    df_excluidos = pd.concat(
        [
            df_excluidos,
            adicionar_motivo_exclusao(linhas_descartadas_p1, "DUPLICADO - PRIMEIRA PALAVRA REPETIDA"),
        ],
        ignore_index=True,
    )

    return df_mantidos.reset_index(drop=True), df_excluidos


