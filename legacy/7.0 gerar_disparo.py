import pandas as pd

print("1️⃣ Lendo arquivo...")

arquivo = "complicacao_final.xlsx"

df_original = pd.read_excel(arquivo, sheet_name="usuarios", dtype=str)

print("Total de registros carregados:", len(df_original))

# Criamos um dataframe de trabalho
df = df_original.copy()


# -------------------------------------------------
# 2️⃣ FILTRO PAI - STATUS CHAVE
# -------------------------------------------------

print("\n2️⃣ Aplicando filtro STATUS CHAVE (OK_PRINCIPAL ou OK_FALLBACK)...")

df = df[df["STATUS CHAVE"].isin(["OK_PRINCIPAL", "OK_FALLBACK"])]

print("Registros após filtro STATUS CHAVE:", len(df))


# -------------------------------------------------
# 3️⃣ FILTRO DE PROCESSO
# -------------------------------------------------

print("\n3️⃣ Filtrando PROCESSO permitido...")

processos_validos = [
    "MUDAR_CONTATO_LIDO_NAO",
    "MUDAR_CONTATO_LIDO_SEM_RESPOSTA",
    "MUDAR_CONTATO_ENTREGUE",
    "MUDAR_CONTATO_ENVIADO",
    "MUDAR_CONTATO_NAO_ENTREGUE_META",
    "MUDAR_CONTATO_MENSAGEM_NAO_ENTREGUE",
    "MUDAR_CONTATO_EXPERIMENTO",
    "MUDAR_CONTATO_OPT_OUT",
    "DISPARAR_NOVAMENTE",
    "SEGUNDO_ENVIO",
    "MUDAR_CONTATO_DISPAROS_EXCEDENTE",
]

df = df[df["PROCESSO"].isin(processos_validos)]

print("Registros após filtro PROCESSO:", len(df))


# -------------------------------------------------
# 4️⃣ IDENTIFICAR TELEFONE DISPARO
# -------------------------------------------------

print("\n4️⃣ Determinando TELEFONE DISPARO a partir da coluna ACAO...")

def escolher_telefone(linha):

    acao = linha["ACAO"]

    if acao == "TELEFONE 1":
        return linha["TELEFONE 1"]

    elif acao == "TELEFONE 2":
        return linha["TELEFONE 2"]

    elif acao == "TELEFONE 3":
        return linha["TELEFONE 3"]

    elif acao == "TELEFONE 4":
        return linha["TELEFONE 4"]

    elif acao == "TELEFONE 5":
        return linha["TELEFONE 5"]

    else:
        return None


df["TELEFONE DISPARO"] = df.apply(escolher_telefone, axis=1)

print("Coluna TELEFONE DISPARO criada")


# -------------------------------------------------
# 5️⃣ SELECIONAR COLUNAS PARA DISPARO
# -------------------------------------------------

print("\n5️⃣ Criando dataframe da aba disparo...")

colunas_disparo = [
    "USUARIO",
    "PRESTADOR",
    "PROCEDIMENTO",
    "DT INTERNACAO",
    "TELEFONE DISPARO",
    "TP ATENDIMENTO",
    "DT ENVIO",
    "TELEFONE PRIORIDADE",
    "TELEFONE ENVIADO",
    "PROXIMO TELEFONE DISPONIVEL",
    "PROCESSO",
    "ACAO",
    "CHAVE RELATORIO",
]

df_disparo = df[colunas_disparo].copy()

print("Registros preparados para disparo:", len(df_disparo))


# -------------------------------------------------
# 6️⃣ REMOVER DUPLICIDADE DE CHAVE
# -------------------------------------------------

print("\n6️⃣ Removendo duplicidade de CHAVE RELATORIO...")

antes = len(df_disparo)

df_disparo = df_disparo.drop_duplicates(subset=["CHAVE RELATORIO"])

depois = len(df_disparo)

print("Removidos:", antes - depois)


# -------------------------------------------------
# 7️⃣ VALIDAÇÃO CHAVE
# -------------------------------------------------

print("\n7️⃣ Validando existência da CHAVE RELATORIO...")

chaves_usuarios = set(df_original["CHAVE RELATORIO"])

def validar_chave(chave):

    if chave in chaves_usuarios:
        return "OK"
    else:
        return "NAO ENCONTRADO"


df_disparo["VALIDAÇÃO CHAVE"] = df_disparo["CHAVE RELATORIO"].apply(validar_chave)

print("Validação de chave concluída")


# -------------------------------------------------
# 8️⃣ VALIDAÇÃO TELEFONE
# -------------------------------------------------

print("\n8️⃣ Validando telefone nas colunas TELEFONE 1 a 5...")

telefones_usuarios = set(
    pd.concat([
        df_original["TELEFONE 1"],
        df_original["TELEFONE 2"],
        df_original["TELEFONE 3"],
        df_original["TELEFONE 4"],
        df_original["TELEFONE 5"]
    ]).dropna()
)

def validar_telefone(tel):

    if tel in telefones_usuarios:
        return "OK"
    else:
        return "NAO ENCONTRADO"


df_disparo["VALIDAÇÃO FINAL"] = df_disparo["TELEFONE DISPARO"].apply(validar_telefone)

print("Validação de telefone concluída")


# -------------------------------------------------
# 9️⃣ EXPORTAR RESULTADO
# -------------------------------------------------

print("\n9️⃣ Exportando arquivo final...")

saida = "arquivo_saida.xlsx"

with pd.ExcelWriter(saida, engine="openpyxl") as writer:
    
    # aba original
    df_original.to_excel(writer, sheet_name="usuarios", index=False)

    # aba resultado
    df_disparo.to_excel(writer, sheet_name="disparo", index=False)

print("Arquivo criado:", saida)

print("\n✅ Processo finalizado com sucesso!")