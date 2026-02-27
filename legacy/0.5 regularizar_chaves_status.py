import pandas as pd
import re


def normalizar_tel(v):
    if pd.isna(v):
        return ""
    if isinstance(v, (int, float)):
        v = str(int(v))
    return re.sub(r"\D", "", str(v))


print("Lendo arquivos...")
df_status = pd.read_excel("status.xlsx", dtype={"Contato": str, "Telefone": str})
df_usuarios = pd.read_excel("novos_contatos.xlsx", sheet_name="usuarios")

# -----------------------------------------
# 1) Base de status (normalizacao minima)
# -----------------------------------------
df_status["Contato"] = df_status["Contato"].astype(str).str.strip()
if "nome_manipulado" not in df_status.columns:
    df_status["nome_manipulado"] = df_status["Contato"].astype(str).str.split("_").str[0]

df_status["NOME_NORM"] = df_status["nome_manipulado"].astype(str).str.strip().str.upper()
df_status["TELEFONE_NORM"] = df_status["Telefone"].apply(normalizar_tel)

# -----------------------------------------
# 2) Base usuarios (chave canonica + telefones)
# -----------------------------------------
df_usuarios["CHAVE RELATORIO"] = df_usuarios["CHAVE RELATORIO"].astype(str).str.strip()
df_usuarios["USUARIO_NORM"] = df_usuarios["USUARIO"].astype(str).str.strip().str.upper()

colunas_tel = ["TELEFONE 1", "TELEFONE 2", "TELEFONE 3", "TELEFONE 4", "TELEFONE 5"]

for c in colunas_tel:
    df_usuarios[c + "_NORM"] = df_usuarios[c].apply(normalizar_tel)

map_nome_tel = (
    df_usuarios[["CHAVE RELATORIO", "USUARIO_NORM"] + [c + "_NORM" for c in colunas_tel]]
    .melt(
        id_vars=["CHAVE RELATORIO", "USUARIO_NORM"],
        value_vars=[c + "_NORM" for c in colunas_tel],
        var_name="COLUNA_TEL",
        value_name="TELEFONE_NORM"
    )
)
map_nome_tel = map_nome_tel[map_nome_tel["TELEFONE_NORM"] != ""].copy()
map_nome_tel = map_nome_tel.drop_duplicates(subset=["USUARIO_NORM", "TELEFONE_NORM", "CHAVE RELATORIO"])

# -----------------------------------------
# 3) Classificacao dos pares nome+telefone
# -----------------------------------------
agg = (
    map_nome_tel
    .groupby(["USUARIO_NORM", "TELEFONE_NORM"])["CHAVE RELATORIO"]
    .nunique()
    .rename("QTD_CHAVES")
    .reset_index()
)

pares_unicos = agg[agg["QTD_CHAVES"] == 1][["USUARIO_NORM", "TELEFONE_NORM"]]
pares_duplicados = agg[agg["QTD_CHAVES"] > 1][["USUARIO_NORM", "TELEFONE_NORM"]]

map_unico = (
    map_nome_tel
    .merge(
        pares_unicos,
        on=["USUARIO_NORM", "TELEFONE_NORM"],
        how="inner"
    )
    .drop_duplicates(subset=["USUARIO_NORM", "TELEFONE_NORM"])
    .set_index(["USUARIO_NORM", "TELEFONE_NORM"])["CHAVE RELATORIO"]
)

set_duplicados = set(
    pares_duplicados.rename(columns={"USUARIO_NORM": "NOME_NORM"})
    .set_index(["NOME_NORM", "TELEFONE_NORM"])
    .index
)

# -----------------------------------------
# 4) Regras de correcao do Contato
# -----------------------------------------
set_chaves_validas = set(df_usuarios["CHAVE RELATORIO"].dropna().astype(str).str.strip())

df_status["Contato_original"] = df_status["Contato"]
df_status["Contato_corrigido"] = df_status["Contato"]
df_status["STATUS_CORRECAO_CHAVE"] = "NAO_AVALIADO"

mask_ok = df_status["Contato"].isin(set_chaves_validas)
df_status.loc[mask_ok, "STATUS_CORRECAO_CHAVE"] = "OK_CHAVE_EXISTENTE"

mask_restante = ~mask_ok
idx_restante = df_status.index[mask_restante]

idx_par = (
    df_status.loc[mask_restante]
    .set_index(["NOME_NORM", "TELEFONE_NORM"])
    .index
)

mask_dup_local = pd.Series([par in set_duplicados for par in idx_par], index=idx_restante)
df_status.loc[mask_dup_local[mask_dup_local].index, "STATUS_CORRECAO_CHAVE"] = "DUPLICADO_NOME_TELEFONE"

idx_nao_dup = mask_dup_local[~mask_dup_local].index
idx_nao_dup_par = (
    df_status.loc[idx_nao_dup]
    .set_index(["NOME_NORM", "TELEFONE_NORM"])
    .index
)
novas_chaves = pd.Series(idx_nao_dup_par.map(map_unico), index=idx_nao_dup) # type: ignore

mask_encontrado = novas_chaves.notna()
idx_corrigir = mask_encontrado[mask_encontrado].index
idx_nao_encontrado = mask_encontrado[~mask_encontrado].index

if len(idx_corrigir) > 0:
    df_status.loc[idx_corrigir, "Contato_corrigido"] = novas_chaves[mask_encontrado].values
    df_status.loc[idx_corrigir, "STATUS_CORRECAO_CHAVE"] = "CORRIGIDO_NOME_TELEFONE"

if len(idx_nao_encontrado) > 0:
    df_status.loc[idx_nao_encontrado, "STATUS_CORRECAO_CHAVE"] = "NAO_ENCONTRADO"

# aplica somente quando houve correcao valida
df_status.loc[df_status["STATUS_CORRECAO_CHAVE"] == "CORRIGIDO_NOME_TELEFONE", "Contato"] = \
    df_status.loc[df_status["STATUS_CORRECAO_CHAVE"] == "CORRIGIDO_NOME_TELEFONE", "Contato_corrigido"]

# mantem nome_manipulado coerente com Contato final
df_status["nome_manipulado"] = df_status["Contato"].astype(str).str.split("_").str[0]

# -----------------------------------------
# 5) Saida e arquivo somente com corrigidos
# -----------------------------------------
df_status.to_excel("status.xlsx", index=False)

resumo = df_status["STATUS_CORRECAO_CHAVE"].value_counts(dropna=False)
print("\nResumo correcao de chaves:")
for status, qtd in resumo.items():
    print(f"- {status}: {qtd}")

# somente linhas corrigidas automaticamente para revisao
df_corrigidos = df_status[
    df_status["STATUS_CORRECAO_CHAVE"] == "CORRIGIDO_NOME_TELEFONE"
].copy()
df_corrigidos.to_excel("status_chaves_corrigidas.xlsx", index=False)

print("\nArquivo atualizado: status.xlsx")
print(f"Chaves corrigidas: status_chaves_corrigidas.xlsx ({len(df_corrigidos)} linhas)")
