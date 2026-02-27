import pandas as pd
import warnings
warnings.simplefilter(action="ignore", category=FutureWarning)


# ==========================================================
# 1) LEITURA DO ARQUIVO
# ==========================================================
print("📘 Lendo arquivo COMPLICAÇÃO JANEIRO 27.02.xlsx aba BASE...")
df = pd.read_excel("COMPLICAÇÃO JANEIRO 27.02.xlsx", sheet_name="BASE")

print("🧽 Normalizando colunas...")
df.columns = df.columns.str.strip()


# ==========================================================
# 2) IDENTIFICAR DUPLICADOS (ANTES DE TUDO)
# ==========================================================
print("🔍 Identificando duplicados por COD USUARIO...")

mask_duplicados = df.duplicated(subset=["COD USUARIO"], keep=False)

df_duplicados_raw = df[mask_duplicados]
df_sem_duplicados = df[~mask_duplicados]

print(f"   ➜ Total duplicados: {len(df_duplicados_raw)}")
print(f"   ➜ Total sem duplicados: {len(df_sem_duplicados)}")


# ==========================================================
# 3) COLUNAS FINAIS PADRÃO
# ==========================================================
colunas_finais = [
    'STATUS BOT', 'BASE', 'COD USUARIO', 'USUARIO',
    'TELEFONE 1', 'TELEFONE 2', 'TELEFONE 3', 'TELEFONE 4', 'TELEFONE 5',
    'PRESTADOR', 'PROCEDIMENTO', 'TP ATENDIMENTO', 'DT INTERNACAO', 'ENVIO',
    'ULTIMO STATUS DE ENVIO','IDENTIFICACAO', 'RESPOSTA', 'LIDA_REPOSTA_SIM','LIDA_REPOSTA_NAO', 'LIDA_SEM_RESPOSTA', 'LIDA', 'ENTREGUE', 'ENVIADA',
    'NAO_ENTREGUE_META', 'MENSAGEM_NAO_ENTREGUE', 'EXPERIMENTO',
    'OPT_OUT', 'TELEFONE ENVIADO', 'TELEFONE PRIORIDADE','CHAVE RELATORIO', 'CHAVE STATUS',
    'STATUS TELEFONE', 'STATUS CHAVE','PROCESSO', 'ACAO','QT LIDA', 'QT ENTREGUE', 'QT ENVIADA',
    'QT NAO_ENTREGUE_META', 'QT MENSAGEM_NAO_ENTREGUE', 'QT EXPERIMENTO',
    'QT OPT_OUT',  "QT TELEFONE", 'TELEFONE STATUS 1', 'TELEFONE STATUS 2', 'TELEFONE STATUS 3', 'TELEFONE STATUS 4', 'TELEFONE STATUS 5'
]

# ==========================================================
# 3.1) COLUNAS DA ABA DADOS_ENVIO_TELEFONICO (LOG DE DISPAROS)
# ==========================================================
colunas_envio_telefonico = [
    'BASE',
    'COD USUARIO',
    'USUARIO',
    'PRESTADOR',
    'PROCEDIMENTO',
    'TELEFONE ENVIADO',
    'TIPO TELEFONE',
    'DATA ENVIO',
    'ENVIO_ID',
    'TENTATIVA',
    'ULTIMO STATUS DE ENVIO',
    'LIDA',
    'RESPONDIDO',
    'ENTREGUE',
    'STATUS TELEFONE',
    'STATUS CHAVE',
    'SOMA STATUS',
    'CHAVE RELATORIO',
    'CHAVE STATUS',
    'PROCESSO'
]


# ==========================================================
# 4) FUNÇÃO PADRÃO PARA MONTAR DATAFRAME FINAL
# ==========================================================
def montar_df_final(df_base):
    df_final = pd.DataFrame(columns=colunas_finais)

    if 'BASE' in df_base: df_final['BASE'] = df_base['BASE']
    if 'COD USUARIO' in df_base: df_final['COD USUARIO'] = df_base['COD USUARIO']
    if 'USUARIO' in df_base: df_final['USUARIO'] = df_base['USUARIO']
    if 'TELEFONE 1' in df_base: df_final['TELEFONE 1'] = df_base['TELEFONE 1']
    if 'TELEFONE 2' in df_base: df_final['TELEFONE 2'] = df_base['TELEFONE 2']
    if 'TELEFONE 3' in df_base: df_final['TELEFONE 3'] = df_base['TELEFONE 3']
    if 'TELEFONE 4' in df_base: df_final['TELEFONE 4'] = df_base['TELEFONE 4']
    if 'TELEFONE 5' in df_base: df_final['TELEFONE 5'] = df_base['TELEFONE 5']
    if 'PRESTADOR' in df_base: df_final['PRESTADOR'] = df_base['PRESTADOR']
    if 'PROCEDIMENTO' in df_base: df_final['PROCEDIMENTO'] = df_base['PROCEDIMENTO']
    if 'TP ATENDIMENTO' in df_base: df_final['TP ATENDIMENTO'] = df_base['TP ATENDIMENTO']
    if 'DT INTERNACAO' in df_base: df_final['DT INTERNACAO'] = df_base['DT INTERNACAO']
    if 'DT ENVIO' in df_base: df_final['ENVIO'] = df_base['DT ENVIO']
    if 'CHAVE' in df_base: df_final['CHAVE RELATORIO'] = df_base['CHAVE']

    for col in colunas_finais:
        if col not in df_final.columns:
            df_final[col] = ""

    return df_final


# ==========================================================
# 5) ABA USUARIOS (BASE MESTRA — SEM DUPLICADOS)
# ==========================================================
print("📌 Criando aba usuarios (sem duplicados)...")
df_usuarios = montar_df_final(df_sem_duplicados)


# ==========================================================
# 6) ABAS DERIVADAS (SEMPRE A PARTIR DE df_sem_duplicados)
# ==========================================================

# --- usuarios_nao_lidos
filtro_nao_lidos = (
    df_sem_duplicados['STATUS'].isna() |
    (df_sem_duplicados['STATUS'].astype(str).str.strip() == "")
)
df_usuarios_nao_lidos = montar_df_final(df_sem_duplicados[filtro_nao_lidos])

# --- usuarios_lidos
status_validos = ["Lida", "Não quis", "Óbito"]
df_lidos = montar_df_final(
    df_sem_duplicados[df_sem_duplicados['STATUS'].isin(status_validos)]
)

# --- respondidos / nao respondidos P1
df_respondidos_p1 = montar_df_final(
    df[(df['P1'].notna())]
)

df_nao_respondidos_p1 = montar_df_final(
    df[(df['P1'].isna())]
)

print(len(df_respondidos_p1))
print(len(df_nao_respondidos_p1))
# ==========================================================
# 7) ABA USUARIOS DUPLICADOS (ISOLADA)
# ==========================================================
print("📌 Criando aba usuarios_duplicados...")
df_duplicados = montar_df_final(df_duplicados_raw)


# ==========================================================
# 8) ABAS VAZIAS PADRÃO
# ==========================================================
df_vazio = pd.DataFrame(columns=colunas_finais)

abas_vazias = {
    "usuarios_resolvidos": df_vazio,
}

df_dados_envio_telefonico = pd.DataFrame(columns=colunas_envio_telefonico)
# ==========================================================
# 9) SALVAR ARQUIVO FINAL
# ==========================================================
print("💾 Salvando arquivo final novos_contatos.xlsx ...")

with pd.ExcelWriter("novos_contatos.xlsx", engine="openpyxl") as writer:
    df_usuarios.to_excel(writer, sheet_name="usuarios", index=False)
    df_usuarios_nao_lidos.to_excel(writer, sheet_name="usuarios_nao_lidos", index=False)
    df_lidos.to_excel(writer, sheet_name="usuarios_lidos", index=False)
    df_respondidos_p1.to_excel(writer, sheet_name="usuarios_respondidos", index=False)
    df_nao_respondidos_p1.to_excel(writer, sheet_name="usuarios_nao_respondidos", index=False)
    df_duplicados.to_excel(writer, sheet_name="usuarios_duplicados", index=False)
    
    for aba, tabela in abas_vazias.items():
        tabela.to_excel(writer, sheet_name=aba, index=False)

print("✅ Arquivo criado com sucesso!")
