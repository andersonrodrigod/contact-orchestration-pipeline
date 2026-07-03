from pathlib import Path

import pandas as pd


NOME_ARQUIVO_UTILIDADE = "UTILIDADE.xlsx"


def localizar_arquivo_utilidade(pasta_utilidades_repositorio):
    caminho = Path(pasta_utilidades_repositorio) / NOME_ARQUIVO_UTILIDADE
    return caminho if caminho.exists() else None


def normalizar_chave_join(valor):
    if pd.isna(valor):
        return ""
    return str(valor).strip().upper()


def criar_mapa_utilidade(caminho_utilidade, aba, coluna_chave, coluna_valor):
    df_utilidade = pd.read_excel(
        caminho_utilidade,
        sheet_name=aba,
        usecols=[coluna_chave, coluna_valor],
        dtype=str,
    )
    df_utilidade[coluna_chave] = df_utilidade[coluna_chave].apply(normalizar_chave_join)
    df_utilidade[coluna_valor] = df_utilidade[coluna_valor].fillna("").astype(str).str.strip()
    df_utilidade = df_utilidade[
        (df_utilidade[coluna_chave] != "") & (df_utilidade[coluna_valor] != "")
    ].drop_duplicates(subset=[coluna_chave], keep="first")

    return df_utilidade.set_index(coluna_chave)[coluna_valor]


def preencher_por_mapa(df, coluna_chave_base, coluna_destino, mapa):
    chave = df[coluna_chave_base].apply(normalizar_chave_join)
    valores_utilidade = chave.map(mapa).replace("", pd.NA)
    valores_atuais = df[coluna_destino].replace("", pd.NA)
    df[coluna_destino] = valores_utilidade.combine_first(valores_atuais).fillna("")
    return int(valores_utilidade.notna().sum())


def enriquecer_com_utilidades(df, caminho_utilidade):
    if caminho_utilidade is None:
        return df, {
            "executado": False,
            "motivo": "Arquivo de utilidade nao encontrado.",
        }

    df_final = df.copy()
    qtd_linhas_antes = len(df_final)

    mapas = {
        "TIPO": criar_mapa_utilidade(caminho_utilidade, "VIDEO", "PROCEDIMENTO", "TIPO"),
        "ESTADO": criar_mapa_utilidade(
            caminho_utilidade,
            "PRESTADOR_SIGLA_UF_DISTRITO",
            "PRESTADOR",
            "ESTADO",
        ),
        "UF": criar_mapa_utilidade(
            caminho_utilidade,
            "PRESTADOR_SIGLA_UF_DISTRITO",
            "PRESTADOR",
            "UF",
        ),
        "SIGLA": criar_mapa_utilidade(
            caminho_utilidade,
            "PRESTADOR_SIGLA_UF_DISTRITO",
            "PRESTADOR",
            "SIGLA_1",
        ),
        "DISTRITO": criar_mapa_utilidade(
            caminho_utilidade,
            "PRESTADOR_SIGLA_UF_DISTRITO",
            "PRESTADOR",
            "DISTRITO",
        ),
        "ESPECIALISTA": criar_mapa_utilidade(
            caminho_utilidade,
            "ESPECIALISTA",
            "PROCEDIMENTO",
            "ESPECIALISTA",
        ),
    }

    preenchidos = {
        "TIPO": preencher_por_mapa(df_final, "PROCEDIMENTO", "TIPO", mapas["TIPO"]),
        "ESTADO": preencher_por_mapa(df_final, "PRESTADOR", "ESTADO", mapas["ESTADO"]),
        "UF": preencher_por_mapa(df_final, "PRESTADOR", "UF", mapas["UF"]),
        "SIGLA": preencher_por_mapa(df_final, "PRESTADOR", "SIGLA", mapas["SIGLA"]),
        "DISTRITO": preencher_por_mapa(df_final, "PRESTADOR", "DISTRITO", mapas["DISTRITO"]),
        "ESPECIALISTA": preencher_por_mapa(
            df_final,
            "PROCEDIMENTO",
            "ESPECIALISTA",
            mapas["ESPECIALISTA"],
        ),
    }

    if len(df_final) != qtd_linhas_antes:
        raise ValueError(
            "O enriquecimento por utilidade alterou a quantidade de linhas "
            f"({qtd_linhas_antes} -> {len(df_final)})."
        )

    return df_final, {
        "executado": True,
        "arquivo": caminho_utilidade,
        "preenchidos": preenchidos,
    }

