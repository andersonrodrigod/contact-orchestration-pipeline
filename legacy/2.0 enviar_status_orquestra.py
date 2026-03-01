import pandas as pd
import numpy as np
import re
import unicodedata
import warnings
from controle_usuarios import retornar_registros_para_usuarios
warnings.simplefilter(action="ignore", category=FutureWarning)

# ============================================================
# LEITURA
# ============================================================

print("📘 Lendo novos_contatos.xlsx ...")
abas = pd.read_excel("novos_contatos.xlsx", sheet_name=None)
# Leitura tipada para preservar telefones como texto e evitar notacao cientifica.
colunas_usuarios = pd.read_excel("novos_contatos.xlsx", sheet_name="usuarios", nrows=0).columns
colunas_telefone = [f"TELEFONE {i}" for i in range(1, 6)] + ["TELEFONE ENVIADO"]
dtype_usuarios = {c: str for c in colunas_telefone if c in colunas_usuarios}
abas["usuarios"] = pd.read_excel("novos_contatos.xlsx", sheet_name="usuarios", dtype=dtype_usuarios)
#abas = retornar_registros_para_usuarios(abas)


if "usuarios" not in abas:
    raise ValueError("Aba 'usuarios' não encontrada")

df_novos = abas["usuarios"].copy()
print("✔ usuarios carregado:", df_novos.shape)

print("\n📗 Lendo status.xlsx ...")
df_status = pd.read_excel("status.xlsx", dtype={"Telefone": str, "Contato": str})
print("✔ status carregado:", df_status.shape)

# ============================================================
# MAPA DE STATUS
# ============================================================

status_colunas = {
    "Lida": "LIDA",
    "Entregue": "ENTREGUE",
    "Enviada": "ENVIADA",
    "A Meta decidiu não entregar a mensagem": "NAO_ENTREGUE_META",
    "Mensagem não pode ser entregue": "MENSAGEM_NAO_ENTREGUE",
    "Número é parte de um experimento": "EXPERIMENTO",
    "Usuário decidiu não receber MKT messages": "OPT_OUT"
}

# ============================================================
# NORMALIZAÇÃO STATUS
# ============================================================

df_status["Contato"] = df_status["Contato"].astype(str).str.strip()
df_status["NOME_NORM"] = df_status["nome_manipulado"].astype(str).str.strip().str.upper()
df_status["TELEFONE_NORM"] = df_status["Telefone"].astype(str).str.replace(r"\D", "", regex=True)

def normalizar_texto(v):
    if pd.isna(v):
        return ""
    txt = str(v).strip().casefold()
    txt = unicodedata.normalize("NFKD", txt)
    return "".join(ch for ch in txt if not unicodedata.combining(ch))

if "Resposta" not in df_status.columns:
    df_status["Resposta"] = pd.NA

df_status["RESPOSTA_NORM"] = (
    df_status["Resposta"]
    .apply(normalizar_texto)
)

df_status["DATA_ENVIO"] = pd.to_datetime(
    df_status["Data de envio"],
    dayfirst=True,
    errors="coerce"
)

df_status["DATA_AGENDAMENTO"] = pd.to_datetime(
    df_status["Data agendamento"],
    dayfirst=True,
    errors="coerce"
).dt.date

df_status["DATA_EVENTO"] = df_status["DATA_ENVIO"].fillna(df_status["DATA_AGENDAMENTO"])

print("⚠️ Registros SEM DATA_EVENTO:",
      df_status["DATA_EVENTO"].isna().sum())

# ============================================================
# ESTADO ATUAL (ÚLTIMO EVENTO)
# ============================================================

df_estado_atual = (
    df_status
    .sort_values("DATA_EVENTO")
    .groupby("Contato", as_index=False)
    .last()
)

# ============================================================
# PREPARAÇÃO USUÁRIOS
# ============================================================

df_novos["CHAVE RELATORIO"] = df_novos["CHAVE RELATORIO"].astype(str).str.strip()
df_novos["NOME_NORM"] = df_novos["USUARIO"].astype(str).str.strip().str.upper()
if "LIDA_REPOSTA_SIM" not in df_novos.columns:
    df_novos["LIDA_REPOSTA_SIM"] = 0

# ============================================================
# FUNÇÃO DE NORMALIZAÇÃO DE TELEFONE (ROBUSTA)
# ============================================================

def normalizar_tel(v):
    if pd.isna(v):
        return ""
    if isinstance(v, (int, float)):
        v = str(int(v))
    return re.sub(r"\D", "", str(v))

# ============================================================
# VIA CHAVE (FLUXO ÚNICO)
# ============================================================

map_chave = df_estado_atual.set_index("Contato")
mask_chave = df_novos["CHAVE RELATORIO"].isin(map_chave.index)

df_novos.loc[mask_chave, "ULTIMO STATUS DE ENVIO"] = \
    df_novos.loc[mask_chave, "CHAVE RELATORIO"].map(map_chave["Status"])

df_novos.loc[mask_chave, "TELEFONE ENVIADO"] = \
    df_novos.loc[mask_chave, "CHAVE RELATORIO"].map(map_chave["Telefone"])

df_novos.loc[mask_chave, "IDENTIFICACAO"] = \
    df_novos.loc[mask_chave, "CHAVE RELATORIO"].map(map_chave["Respondido"])

df_novos.loc[mask_chave, "DATA_EVENTO"] = \
    df_novos.loc[mask_chave, "CHAVE RELATORIO"].map(map_chave["DATA_EVENTO"])

if "Resposta" in map_chave.columns:
    df_novos.loc[mask_chave, "RESPOSTA"] = \
        df_novos.loc[mask_chave, "CHAVE RELATORIO"].map(map_chave["Resposta"])
    
df_novos.loc[mask_chave, "CHAVE STATUS"] = \
    df_novos.loc[mask_chave, "CHAVE RELATORIO"]

print("✔ VIA CHAVE:", mask_chave.sum())

# ============================================================
# CONTAGEM DE STATUS
# ============================================================

df_status["STATUS_MAP"] = (df_status["Status"].map(status_colunas).fillna("OUTROS"))

contagem_total = (
    df_status
    .groupby(["Contato", "STATUS_MAP"])
    .size()
    .unstack(fill_value=0)
)
    
for col in status_colunas.values():
    qt_col = f"QT {col}"

    if col in contagem_total.columns:
        df_novos[qt_col] = (
            df_novos["CHAVE STATUS"]
            .map(contagem_total[col])
            .fillna(0)
            .astype(int)
        )
    else:
        df_novos[qt_col] = 0

df_novos["TELEFONE ENVIADO_NORM"] = df_novos["TELEFONE ENVIADO"].apply(normalizar_tel)

contagem_tel_nome = (
    df_status
    .dropna(subset=["NOME_NORM", "TELEFONE_NORM"])
    .groupby(["NOME_NORM", "TELEFONE_NORM", "STATUS_MAP"])
    .size()
    .unstack(fill_value=0)
)

idx_tel_nome = (
    df_novos
    .set_index(["NOME_NORM", "TELEFONE ENVIADO_NORM"])
    .index
)

for col in status_colunas.values():
    if col in contagem_tel_nome.columns:
        df_novos[col] = (
            idx_tel_nome
            .map(contagem_tel_nome[col]) # type: ignore
            .fillna(0)
            .astype(int)
        )
    else:
        df_novos[col] = 0

# indicador especifico: LIDA com Resposta == Sim (nao entra na soma de status)
mask_lida_resposta_sim = (
    (df_status["STATUS_MAP"] == "LIDA")
    & (df_status["RESPOSTA_NORM"] == "sim")
)

mask_lida_resposta_nao= (
    (df_status["STATUS_MAP"] == "LIDA")
    & (df_status["RESPOSTA_NORM"] == "nao")
)

mask_lida_sem_resposta = (
    (df_status["STATUS_MAP"] == "LIDA")
    & (df_status["RESPOSTA_NORM"] == "sem resposta")
)

contagem_lida_resposta_sim = (
    df_status[mask_lida_resposta_sim]
    .dropna(subset=["NOME_NORM", "TELEFONE_NORM"])
    .groupby(["NOME_NORM", "TELEFONE_NORM"])
    .size()
)

contagem_lida_resposta_nao = (
    df_status[mask_lida_resposta_nao]
    .dropna(subset=["NOME_NORM", "TELEFONE_NORM"])
    .groupby(["NOME_NORM", "TELEFONE_NORM"])
    .size()
)

contagem_lida_sem_resposta = (
    df_status[mask_lida_sem_resposta]
    .dropna(subset=["NOME_NORM", "TELEFONE_NORM"])
    .groupby(["NOME_NORM", "TELEFONE_NORM"])
    .size()
)

df_novos["LIDA_REPOSTA_SIM"] = (
    idx_tel_nome
    .map(contagem_lida_resposta_sim) # type: ignore
    .fillna(0)
    .astype(int)
)

df_novos["LIDA_REPOSTA_NAO"] = (
    idx_tel_nome
    .map(contagem_lida_resposta_nao) # type: ignore
    .fillna(0)
    .astype(int)
)

df_novos["LIDA_SEM_RESPOSTA"] = (
    idx_tel_nome
    .map(contagem_lida_sem_resposta) # type: ignore
    .fillna(0)
    .astype(int)
)

print("✔ Contagem aplicada")

# ============================================================
# TELEFONE PRIORIDADE
# ============================================================

colunas_tel = ["TELEFONE 1", "TELEFONE 2", "TELEFONE 3", "TELEFONE 4", "TELEFONE 5"]

for c in colunas_tel:
    df_novos[c + "_NORM"] = df_novos[c].apply(normalizar_tel)

def identificar_prioridade(row):
    tel_env = row["TELEFONE ENVIADO_NORM"]
    if not tel_env:
        return np.nan
    for c in colunas_tel:
        if row[c + "_NORM"] == tel_env:
            return c
    return "NAO_ENCONTRADO"

df_novos["TELEFONE PRIORIDADE"] = df_novos.apply(identificar_prioridade, axis=1)

# Mapa com TODOS os telefones encontrados no status por contato/chave.
telefones_por_contato = (
    df_status
    .groupby("Contato")["TELEFONE_NORM"]
    .agg(lambda s: {t for t in s if t})
    .to_dict()
)

def telefone_foi_enviado(chave_status, telefone_norm):
    if not telefone_norm:
        return False
    telefones_contato = telefones_por_contato.get(chave_status, set())
    return telefone_norm in telefones_contato

# Marca em todas as colunas de telefone onde houve envio.
for i, c in enumerate(colunas_tel, start=1):
    col_status = f"TELEFONE STATUS {i}"
    df_novos[col_status] = [
        "ENVIADO" if telefone_foi_enviado(chave, tel) else pd.NA
        for chave, tel in zip(df_novos["CHAVE STATUS"], df_novos[c + "_NORM"])
    ]

# ============================================================
# STATUS DE CONSISTÊNCIA
# ============================================================

df_novos["STATUS CHAVE"] = np.where(
    df_novos["CHAVE STATUS"] == df_novos["CHAVE RELATORIO"],
    "OK",
    "ERRO"
)

df_novos["STATUS TELEFONE"] = np.where(
    df_novos["TELEFONE PRIORIDADE"] == "NAO_ENCONTRADO",
    "ERRO",
    "OK"
)

# ============================================================
# EXPORTAÇÃO
# ============================================================

df_export = df_novos.copy()

df_export = df_export.loc[:, ~df_export.columns.str.endswith("_NORM")]
df_export = df_export.drop(columns=["DATA_EVENTO"], errors="ignore")

# colunas originais
df_export[list(status_colunas.values())] = \
    df_export[list(status_colunas.values())].replace(0, np.nan)

# colunas QT
df_export[[f"QT {c}" for c in status_colunas.values()]] = \
    df_export[[f"QT {c}" for c in status_colunas.values()]].replace(0, np.nan)

if "LIDA_REPOSTA_SIM" in df_export.columns:
    df_export["LIDA_REPOSTA_SIM"] = df_export["LIDA_REPOSTA_SIM"].replace(0, np.nan)

if "LIDA_REPOSTA_NAO" in df_export.columns:
    df_export["LIDA_REPOSTA_NAO"] = df_export["LIDA_REPOSTA_NAO"].replace(0, np.nan)

if "LIDA_SEM_RESPOSTA" in df_export.columns:
    df_export["LIDA_SEM_RESPOSTA"] = df_export["LIDA_SEM_RESPOSTA"].replace(0, np.nan)

abas["usuarios"] = df_export

with pd.ExcelWriter("novos_contatos_atualizado.xlsx") as writer:
    for nome, df in abas.items():
        df.to_excel(writer, sheet_name=nome, index=False)

print("\n💾 Arquivo salvo com estrutura preservada")
