import pandas as pd

# =========================================================
# 1. Ler os arquivos
# =========================================================
df_main = pd.read_excel("COMPLICAÇÃO JANEIRO 27.02.xlsx")
df_senhas = pd.read_csv("telefone_janeiro_internacoes.csv")

# =========================================================
# 2. Limpar e padronizar chave SENHA
# =========================================================
df_main["SENHA"] = df_main["SENHA"].astype(str).str.strip()
df_senhas["CD_SENHA"] = df_senhas["CD_SENHA"].astype(str).str.strip()

# =========================================================
# 3. Selecionar apenas colunas necessárias do CSV
# =========================================================
telefones = ["TELEFONE_1", "TELEFONE_2", "TELEFONE_3", "TELEFONE_4", "TELEFONE_5"]

colunas_csv = ["CD_SENHA"] + telefones + ["CD_PESSOA"]
df_senhas = df_senhas[colunas_csv]

# =========================================================
# 4. Limpar telefones (preserva NaN reais)
# =========================================================
for col in telefones:
    df_senhas[col] = df_senhas[col].where(
        df_senhas[col].notna(),
        ""
    ).astype(str).str.strip()

# =========================================================
# 5. Adicionar prefixo 55 (somente quando existir)
# =========================================================
for col in telefones:
    df_senhas[col] = df_senhas[col].apply(
        lambda x: f"55{x}" if x != "" and not x.startswith("55") else x
    )

# =========================================================
# 5.1 Regra adicional para telefones com nono dígito
# =========================================================
def ajustar_nono_digito(telefone: str) -> str:
    if telefone == "":
        return telefone

    # Numero fixo: contando da direita para a esquerda, o 8o digito
    # em 2, 3, 4 ou 5 indica que nao deve inserir 9.
    if len(telefone) >= 8:
        oitavo_da_direita = telefone[-8]
        if oitavo_da_direita in {"2", "3", "4", "5"}:
            return telefone

    # Regra atual: apenas para telefones com 12 caracteres,
    # inserir 9 apos o 4o caractere.
    if len(telefone) == 12:
        return telefone[:4] + "9" + telefone[4:]

    return telefone

for col in telefones:
    df_senhas[col] = df_senhas[col].apply(ajustar_nono_digito)

# =========================================================
# 6. Merge mantendo todas as linhas da base principal
# =========================================================
df_final = df_main.merge(
    df_senhas,
    how="left",
    left_on="SENHA",
    right_on="CD_SENHA"
)

# =========================================================
# 7. Remover coluna duplicada de chave
# =========================================================
df_final = df_final.drop(columns=["CD_SENHA"])

# =========================================================
# 8. Garantir telefones vazios no resultado final
# =========================================================
df_final[telefones] = (
    df_final[telefones]
    .replace(["nan", "None", "<NA>"], "")
    .fillna("")
)

# =========================================================
# 9. Flags e contagens
# =========================================================
df_final["ENCONTROU"] = df_final[telefones].ne("").any(axis=1) # type: ignore
print("Total de SENHAS encontradas (com telefone):", df_final["ENCONTROU"].sum())

df_final["MATCH_SENHA"] = df_final["SENHA"].isin(df_senhas["CD_SENHA"])
print("Match por SENHA:", df_final["MATCH_SENHA"].sum())

sem_telefone = df_final[
    (df_final["MATCH_SENHA"] == True) &
    (df_final[telefones].eq("").all(axis=1)) # type: ignore
]
print("Senha encontrada, mas sem telefone:", len(sem_telefone))

# =========================================================
# 10. Salvar arquivo final
# =========================================================
df_final.to_excel(
    "JANEIRO_INTERNAÇÕES_COM_TELEFONES.xlsx",
    index=False
)
