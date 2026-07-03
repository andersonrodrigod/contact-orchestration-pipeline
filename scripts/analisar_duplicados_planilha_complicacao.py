from pathlib import Path

import pandas as pd


arquivo = Path("data/gerar_planilha_complicacao/saida/complicacao.xlsx")
arquivo_saida = Path("data/gerar_planilha_complicacao/relatorios/cod_usuario_duplicados.xlsx")

df = pd.read_excel(arquivo, sheet_name="BASE")
df["COD USUARIO"] = df["COD USUARIO"].fillna("").astype(str).str.strip()
df = df[df["COD USUARIO"] != ""]

duplicados = df[df.duplicated(subset="COD USUARIO", keep=False)]
qtd_codigos_duplicados = duplicados["COD USUARIO"].nunique()

arquivo_saida.parent.mkdir(parents=True, exist_ok=True)
duplicados.to_excel(arquivo_saida, index=False)

print("Linhas com COD USUARIO duplicado:", len(duplicados))
print("Quantidade de COD USUARIO duplicados:", qtd_codigos_duplicados)
print("Arquivo gerado:", arquivo_saida)
