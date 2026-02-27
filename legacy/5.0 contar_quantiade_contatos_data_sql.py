import pandas as pd

# 1. Ler os arquivos
df_excel = pd.read_excel("COMPLICAÇÃO JANEIRO 27.02.xlsx")
df_csv = pd.read_csv("telefone_janeiro_internacoes.csv")

# 2. Converter as colunas para string
df_excel["COD USUARIO"] = df_excel["COD USUARIO"].astype(str)
df_csv["CD_USUARIO"] = df_csv["CD_USUARIO"].astype(str)

# 3. Remover espaços (boa prática)
df_excel["COD USUARIO"] = df_excel["COD USUARIO"].str.strip()
df_csv["CD_USUARIO"] = df_csv["CD_USUARIO"].str.strip()

# 4. Pegar usuários únicos do Excel
usuarios_excel = df_excel["COD USUARIO"].dropna().unique()

# 5. Ver quais existem no CSV
usuarios_encontrados = df_csv[df_csv["CD_USUARIO"].isin(usuarios_excel)]

# 6. Contar usuários encontrados (únicos)
qtd_usuarios_encontrados = usuarios_encontrados["CD_USUARIO"].nunique()

print(f"Quantidade de usuários encontrados no CSV: {qtd_usuarios_encontrados}")
