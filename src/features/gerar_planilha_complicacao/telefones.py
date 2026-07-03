import pandas as pd

from src.features.gerar_planilha_complicacao.utils import (
    ajustar_nono_digito,
    normalizar_chave,
    normalizar_telefone,
    primeiro_nao_vazio,
    validar_merge_sem_duplicar_linhas,
)


COLUNAS_TELEFONE = {
    "TELEFONE_1": "TELEFONE 1",
    "TELEFONE_2": "TELEFONE 2",
    "TELEFONE_3": "TELEFONE 3",
    "TELEFONE_4": "TELEFONE 4",
    "TELEFONE_5": "TELEFONE 5",
}


def adicionar_telefones_por_senha(df, caminho_csv):
    # Enriquece a base com telefones por SENHA quando o CSV estiver disponivel na entrada da feature.
    if not caminho_csv.exists():
        resumo = {
            "executado": False,
            "motivo": f"Arquivo de telefones nao encontrado: {caminho_csv}",
        }
        return df, resumo

    colunas_telefone_csv = list(COLUNAS_TELEFONE)
    colunas_telefone_final = list(COLUNAS_TELEFONE.values())
    cabecalho = pd.read_csv(caminho_csv, nrows=0)
    coluna_senha_csv = "CD_SENHA" if "CD_SENHA" in cabecalho.columns else "CD_SENHA_AUTORIZA"
    coluna_usuario_csv = "CD_USUARIO" if "CD_USUARIO" in cabecalho.columns else None
    colunas_obrigatorias = [coluna_senha_csv] + colunas_telefone_csv + ["CD_PESSOA"]
    colunas_csv = colunas_obrigatorias + ([coluna_usuario_csv] if coluna_usuario_csv else [])
    colunas_faltantes = [coluna for coluna in colunas_obrigatorias if coluna not in cabecalho.columns]

    df_final = df.copy()
    if colunas_faltantes:
        resumo = {
            "executado": False,
            "motivo": f"CSV de telefones sem colunas obrigatorias: {', '.join(colunas_faltantes)}",
        }
        return df_final, resumo

    df_final["SENHA"] = df_final["SENHA"].apply(normalizar_chave)
    df_final["COD USUARIO"] = df_final["COD USUARIO"].apply(normalizar_chave)
    senhas_base = set(df_final["SENHA"])
    codigos_usuario_base = set(df_final["COD USUARIO"])
    df_senhas = pd.read_csv(caminho_csv, usecols=colunas_csv, dtype=str, low_memory=False)
    df_senhas[coluna_senha_csv] = df_senhas[coluna_senha_csv].apply(normalizar_chave)
    mascara_csv = df_senhas[coluna_senha_csv].isin(senhas_base)

    if coluna_usuario_csv:
        df_senhas[coluna_usuario_csv] = df_senhas[coluna_usuario_csv].apply(normalizar_chave)
        mascara_csv = mascara_csv | df_senhas[coluna_usuario_csv].isin(codigos_usuario_base)

    df_senhas = df_senhas[mascara_csv]
    df_senhas = df_senhas.rename(columns={coluna_senha_csv: "CD_SENHA"})
    if coluna_usuario_csv:
        df_senhas = df_senhas.rename(columns={coluna_usuario_csv: "CD_USUARIO"})

    for coluna in colunas_telefone_csv:
        df_senhas[coluna] = df_senhas[coluna].apply(normalizar_telefone)
        df_senhas[coluna] = df_senhas[coluna].apply(
            lambda telefone: f"55{telefone}" if telefone != "" and not telefone.startswith("55") else telefone
        )
        df_senhas[coluna] = df_senhas[coluna].apply(ajustar_nono_digito)

    agregacoes = {coluna: primeiro_nao_vazio for coluna in colunas_telefone_csv}
    agregacoes["CD_PESSOA"] = primeiro_nao_vazio
    df_senhas_por_senha = df_senhas.groupby("CD_SENHA", as_index=False, dropna=False).agg(agregacoes)
    df_senhas_por_senha["_MATCH_SENHA"] = True
    df_senhas_por_senha = df_senhas_por_senha.rename(columns={"CD_PESSOA": "_CD_PESSOA_SENHA"})

    qtd_linhas_antes_merge = len(df_final)
    df_final = df_final.merge(
        df_senhas_por_senha,
        how="left",
        left_on="SENHA",
        right_on="CD_SENHA",
    )
    validar_merge_sem_duplicar_linhas(qtd_linhas_antes_merge, df_final, "SENHA")
    df_final = df_final.drop(columns=["CD_SENHA"])
    df_final["MATCH_SENHA"] = df_final["_MATCH_SENHA"].eq(True)
    df_final = df_final.drop(columns=["_MATCH_SENHA"])

    cd_pessoa_atual = df_final["CD_PESSOA"].fillna("").astype(str).str.strip()
    cd_pessoa_csv = df_final["_CD_PESSOA_SENHA"].fillna("").astype(str).str.strip()
    df_final["CD_PESSOA"] = cd_pessoa_atual.where(cd_pessoa_atual != "", cd_pessoa_csv)
    df_final = df_final.drop(columns=["_CD_PESSOA_SENHA"])

    for coluna_csv, coluna_final in COLUNAS_TELEFONE.items():
        telefone_atual = df_final[coluna_final].fillna("").astype(str).str.strip()
        telefone_csv = df_final[coluna_csv].fillna("").astype(str).str.strip()
        df_final[coluna_final] = telefone_atual.where(telefone_atual != "", telefone_csv)

    df_final = df_final.drop(columns=colunas_telefone_csv)

    df_final["MATCH_COD_USUARIO"] = False
    if coluna_usuario_csv:
        # Match temporario: usado para recuperar telefones quando a query do CSV nao traz a CD_SENHA correta.
        # Se a query SQL passar a retornar a verdadeira CD_SENHA, este fallback por COD USUARIO pode ser removido.
        df_senhas_por_usuario = (
            df_senhas[df_senhas["CD_USUARIO"] != ""]
            .groupby("CD_USUARIO", as_index=False, dropna=False)
            .agg(agregacoes)
        )
        df_senhas_por_usuario["_MATCH_COD_USUARIO"] = True
        df_senhas_por_usuario = df_senhas_por_usuario.rename(
            columns={
                "CD_PESSOA": "_CD_PESSOA_USUARIO",
                **{coluna: f"_USUARIO_{coluna}" for coluna in colunas_telefone_csv},
            }
        )

        qtd_linhas_antes_merge = len(df_final)
        df_final = df_final.merge(
            df_senhas_por_usuario,
            how="left",
            left_on="COD USUARIO",
            right_on="CD_USUARIO",
        )
        validar_merge_sem_duplicar_linhas(qtd_linhas_antes_merge, df_final, "COD USUARIO")
        df_final = df_final.drop(columns=["CD_USUARIO"])
        df_final["MATCH_COD_USUARIO"] = df_final["_MATCH_COD_USUARIO"].eq(True)
        df_final = df_final.drop(columns=["_MATCH_COD_USUARIO"])

        cd_pessoa_atual = df_final["CD_PESSOA"].fillna("").astype(str).str.strip()
        cd_pessoa_usuario = df_final["_CD_PESSOA_USUARIO"].fillna("").astype(str).str.strip()
        df_final["CD_PESSOA"] = cd_pessoa_atual.where(cd_pessoa_atual != "", cd_pessoa_usuario)
        df_final = df_final.drop(columns=["_CD_PESSOA_USUARIO"])

        for coluna_csv, coluna_final in COLUNAS_TELEFONE.items():
            coluna_usuario = f"_USUARIO_{coluna_csv}"
            telefone_atual = df_final[coluna_final].fillna("").astype(str).str.strip()
            telefone_usuario = df_final[coluna_usuario].fillna("").astype(str).str.strip()
            df_final[coluna_final] = telefone_atual.where(telefone_atual != "", telefone_usuario)

        df_final = df_final.drop(columns=[f"_USUARIO_{coluna}" for coluna in colunas_telefone_csv])

    df_final[colunas_telefone_final] = (
        df_final[colunas_telefone_final]
        .replace(["nan", "None", "<NA>"], "")
        .fillna("")
    )

    for coluna in colunas_telefone_final:
        df_final[coluna] = df_final[coluna].apply(normalizar_telefone)

    df_final["ENCONTROU"] = df_final[colunas_telefone_final].ne("").any(axis=1)
    sem_telefone = df_final[
        ((df_final["MATCH_SENHA"] == True) | (df_final["MATCH_COD_USUARIO"] == True)) &
        (df_final[colunas_telefone_final].eq("").all(axis=1))
    ]

    resumo = {
        "executado": True,
        "senhas_com_telefone": int(df_final["ENCONTROU"].sum()),
        "match_senha": int(df_final["MATCH_SENHA"].sum()),
        "match_cod_usuario": int(df_final["MATCH_COD_USUARIO"].sum()),
        "match_sem_telefone": len(sem_telefone),
    }

    return df_final, resumo


