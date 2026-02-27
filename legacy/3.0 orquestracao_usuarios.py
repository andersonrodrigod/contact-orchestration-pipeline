import pandas as pd
import numpy as np


# ==========================================================
# 1) LER ABAS PRINCIPAIS
# ==========================================================
df_abas = pd.read_excel("novos_contatos.xlsx", sheet_name=None)

if "usuarios" not in df_abas:
    raise ValueError("Aba 'usuarios' nao encontrada em novos_contatos.xlsx")

df_usuarios = df_abas["usuarios"].copy()
df_lidos = df_abas.get("usuarios_lidos", pd.DataFrame(columns=["CHAVE RELATORIO"])).copy()
df_respondidos = df_abas.get("usuarios_respondidos", pd.DataFrame(columns=["CHAVE RELATORIO"])).copy()
df_resolvidos = df_abas.get("usuarios_resolvidos", pd.DataFrame()).copy()

# garante a coluna PROCESSO para classificar sem mover para outras abas
df_usuarios["PROCESSO"] = pd.NA
df_usuarios["ACAO"] = pd.NA


# ================================================================================
# 2) REGRAS DE CLASSIFICACAO
# ================================================================================
colunas_status = [
    "LIDA",
    "ENTREGUE",
    "ENVIADA",
    "NAO_ENTREGUE_META",
    "MENSAGEM_NAO_ENTREGUE",
    "EXPERIMENTO",
    "OPT_OUT",
]
colunas_tel = [f"TELEFONE {i}" for i in range(1, 6)]
colunas_tel_status = [f"TELEFONE STATUS {i}" for i in range(1, 6)]

for c in colunas_tel + colunas_tel_status:
    if c not in df_usuarios.columns:
        df_usuarios[c] = pd.NA

ch_usuarios = df_usuarios["CHAVE RELATORIO"].astype(str).str.strip()
ch_respondidos = df_respondidos["CHAVE RELATORIO"].astype(str).str.strip()
ch_lidos = df_lidos["CHAVE RELATORIO"].astype(str).str.strip()

df_usuarios["SOMA_STATUS"] = df_usuarios[colunas_status].sum(axis=1)


def valor_vazio(v):
    return pd.isna(v) or (isinstance(v, str) and v.strip() == "")


def normalizar_telefone(v):
    if valor_vazio(v):
        return ""

    if isinstance(v, (int, np.integer)):
        return str(v)

    if isinstance(v, (float, np.floating)):
        if np.isnan(v):
            return ""
        return f"{v:.0f}"

    texto = str(v).strip()
    if texto.lower() == "nan":
        return ""

    digitos = "".join(ch for ch in texto if ch.isdigit())
    return digitos if digitos else texto


def identificar_coluna_telefone_enviado(row):
    prioridade = row.get("TELEFONE PRIORIDADE", pd.NA)
    if not valor_vazio(prioridade):
        prioridade = str(prioridade).strip()
        if prioridade in colunas_tel:
            return prioridade

    tel_enviado = normalizar_telefone(row.get("TELEFONE ENVIADO", pd.NA))
    if not tel_enviado:
        return ""

    for i in range(1, 6):
        col_tel = f"TELEFONE {i}"
        if normalizar_telefone(row.get(col_tel, pd.NA)) == tel_enviado:
            return col_tel

    return ""


def definir_acao(row):
    processo = row.get("PROCESSO", pd.NA)
    if valor_vazio(processo):
        return pd.NA

    processo = str(processo).strip()

    processos_reenvio = {
        "SEGUNDO_ENVIO_LIDOS",
        "ENVIAR_DISPARO_NOVAMENTE",
    }
    processos_troca_contato = {
        "MUDAR_CONTATO_LIDOS_NAO_IDENTIFICADOS",
        "MUDAR_CONTATO_LIDOS_REPOSTA_NAO",
        "MUDAR_CONTATO_LIDOS_SEM_RESPOSTA",
        "MUDAR_CONTATO_ENVIO",
    }

    # Processos de reenvio: manter o mesmo slot (coluna) do telefone enviado.
    if processo in processos_reenvio:
        coluna_enviado = identificar_coluna_telefone_enviado(row)
        if not coluna_enviado:
            return "SEM_TELEFONE_DISPONIVEL"
        return f"ENVIAR_{coluna_enviado.replace(' ', '_')}"

    # Troca de contato: escolher telefone elegivel com numero diferente do ja enviado.
    if processo in processos_troca_contato:
        telefone_enviado = normalizar_telefone(row.get("TELEFONE ENVIADO", pd.NA))
        coluna_enviado = identificar_coluna_telefone_enviado(row)
        telefones_vistos = set()

        for i in range(1, 6):
            coluna_i = f"TELEFONE {i}"
            status_i = row.get(f"TELEFONE STATUS {i}", pd.NA)
            tel_i = normalizar_telefone(row.get(coluna_i, pd.NA))

            if not tel_i:
                continue
            if not valor_vazio(status_i):
                continue
            if coluna_enviado and coluna_i == coluna_enviado:
                continue
            if telefone_enviado and tel_i == telefone_enviado:
                continue
            if tel_i in telefones_vistos:
                continue

            telefones_vistos.add(tel_i)
            if tel_i:
                return f"ENVIAR_TELEFONE_{i}"
    elif processo == "ENCERRAR_CONTATO_LIDOS_RESPOSTA_SIM":
        return "CONTATO_NAO_DESEJA_RECEBER"

    return "SEM_TELEFONE_DISPONIVEL"

mask_em_respondidos = ch_usuarios.isin(ch_respondidos)
mask_em_lidos = ch_usuarios.isin(ch_lidos)
mask_lida1 = df_usuarios["LIDA"] == 1

incremento = df_usuarios["SOMA_STATUS"] // 5
mask_mudar_envio_contato = df_usuarios["SOMA_STATUS"] >= 5
mask_acumalador = incremento > 0

df_usuarios.loc[mask_acumalador, "QT TELEFONE"] = (
    df_usuarios.loc[mask_acumalador, "QT TELEFONE"].fillna(0) + incremento[mask_acumalador]
)
df_usuarios.loc[mask_acumalador, colunas_status] = np.nan

mask_para_segundo_envio = (
    mask_lida1
    & ~mask_em_respondidos
)

mask_para_lidos_nao_respondidos = (
    ~mask_em_respondidos
    & mask_em_lidos
    & (df_usuarios["LIDA"] >= 3)
)

mask_lida_resposta_sim = (
    ~mask_em_respondidos
    & mask_em_lidos
    & (df_usuarios["LIDA_REPOSTA_SIM"] >= 2)                  
)

mask_lida_resposta_sim_sem_resposta = (
    ~mask_em_respondidos
    & mask_em_lidos
    & (df_usuarios["LIDA_REPOSTA_SIM"] >= 1)
    & (df_usuarios["LIDA_SEM_RESPOSTA"] >= 1)                  
)

mask_lida_sem_resposta = (
    ~mask_em_respondidos
    & mask_em_lidos
    & (df_usuarios["LIDA_SEM_RESPOSTA"] >= 2)
)

mask_lida_resposta_nao = (
    ~mask_em_respondidos
    & mask_em_lidos
    & (df_usuarios["LIDA_REPOSTA_NAO"] >= 1)
)

col_telefone_enviado = df_usuarios["TELEFONE ENVIADO"]
mask_com_envio_status = (
    col_telefone_enviado.notna()
    & col_telefone_enviado.astype(str).str.strip().ne("")
    & col_telefone_enviado.astype(str).str.lower().ne("nan")
)


# classifica em coluna unica PROCESSO
df_usuarios.loc[mask_mudar_envio_contato, "PROCESSO"] = "MUDAR_CONTATO_ENVIO"
df_usuarios.loc[mask_para_segundo_envio, "PROCESSO"] = "SEGUNDO_ENVIO_LIDOS"
df_usuarios.loc[mask_para_lidos_nao_respondidos, "PROCESSO"] = "MUDAR_CONTATO_LIDOS_NAO_IDENTIFICADOS"
df_usuarios.loc[mask_lida_resposta_sim, "PROCESSO"] = "ENCERRAR_CONTATO_LIDOS_RESPOSTA_SIM"
df_usuarios.loc[mask_lida_sem_resposta, "PROCESSO"] = "MUDAR_CONTATO_LIDOS_SEM_RESPOSTA"
df_usuarios.loc[mask_lida_resposta_nao, "PROCESSO"] = "MUDAR_CONTATO_LIDOS_REPOSTA_NAO"
df_usuarios.loc[mask_lida_resposta_sim_sem_resposta, "PROCESSO"] = "ENCERRAR_CONTATO_LIDOS_RESPOSTA_SIM"

mask_para_reenviar_disparo_novamente = (
    df_usuarios["PROCESSO"].isna()
    & ~mask_em_respondidos
    & mask_com_envio_status
    & (df_usuarios["SOMA_STATUS"] <= 4)
)
df_usuarios.loc[mask_para_reenviar_disparo_novamente, "PROCESSO"] = "ENVIAR_DISPARO_NOVAMENTE"

df_usuarios["ACAO"] = df_usuarios.apply(definir_acao, axis=1)

# resolve respondidos e remove da aba usuarios (comportamento anterior mantido)
df_novos_resolvidos = df_usuarios[mask_em_respondidos].copy()
df_resolvidos = pd.concat([df_resolvidos, df_novos_resolvidos], ignore_index=True)

# remove apenas resolvidos; processos ficam em usuarios
mask_remover = mask_em_respondidos
df_usuarios = df_usuarios[~mask_remover].copy()


# ==========================================================
# 3) ATUALIZAR ABAS E SALVAR
# ==========================================================
df_usuarios = df_usuarios.drop(columns=["SOMA_STATUS"], errors="ignore")

df_abas["usuarios"] = df_usuarios
df_abas["usuarios_resolvidos"] = df_resolvidos

print("\nSalvando arquivo final...")

with pd.ExcelWriter("novos_contatos_atualizado.xlsx", engine="openpyxl") as writer:
    for nome_aba, df in df_abas.items():
        df.to_excel(writer, sheet_name=nome_aba, index=False)

print("\nArquivo 'novos_contatos_atualizado.xlsx' salvo com sucesso!")


# ==========================================================
# 4) LOGS
# ==========================================================
print("\n================ RESULTADOS DAS DISTRIBUICOES ================\n")

print("\nApos limpeza da aba 'usuarios':")
print("Total restante:", len(df_usuarios))
print("Primeiras chaves restantes:")
print(df_usuarios["CHAVE RELATORIO"].head().to_list())

print(f"RESOLVIDOS: {len(df_resolvidos)} registros")
if len(df_resolvidos) > 0:
    print("   -> Primeiras CHAVES:")
    print(df_resolvidos["CHAVE RELATORIO"].head().to_list())
print("--------------------------------------------------------------")

qtd_lidos_nao_respondidos = int(mask_para_lidos_nao_respondidos.sum())
print(f"LIDOS NAO RESPONDIDOS (PROCESSO): {qtd_lidos_nao_respondidos} registros")
if qtd_lidos_nao_respondidos > 0:
    print("   -> Primeiras CHAVES:")
    print(
        df_usuarios.loc[
            df_usuarios["PROCESSO"] == "MUDAR_CONTATO_LIDOS_NAO_IDENTIFICADOS",
            "CHAVE RELATORIO",
        ].head().to_list()
    )
print("--------------------------------------------------------------")

qtd_segundo_envio = int(mask_para_segundo_envio.sum())
print(f"SEGUNDO ENVIO (PROCESSO): {qtd_segundo_envio} registros")
if qtd_segundo_envio > 0:
    print("   -> Primeiras CHAVES:")
    print(
        df_usuarios.loc[
            df_usuarios["PROCESSO"] == "SEGUNDO_ENVIO_LIDOS",
            "CHAVE RELATORIO",
        ].head().to_list()
    )
print("--------------------------------------------------------------")

qtd_mudar_envio_contato = int(mask_mudar_envio_contato.sum())
print(f"MUDAR ENVIO CONTATO (PROCESSO): {qtd_mudar_envio_contato} registros")
if qtd_mudar_envio_contato > 0:
    print("   -> Primeiras CHAVES:")
    print(
        df_usuarios.loc[
            df_usuarios["PROCESSO"] == "MUDAR_CONTATO_ENVIO",
            "CHAVE RELATORIO",
        ].head().to_list()
    )
print("--------------------------------------------------------------")

qtd_mudar_contato_lidos = int(mask_lida_resposta_sim.sum())
print(f"MUDAR CONTATO LIDOS (PROCESSO): {qtd_mudar_contato_lidos} registros")
if qtd_mudar_contato_lidos > 0:
    print("   -> Primeiras CHAVES:")
    print(
        df_usuarios.loc[
            df_usuarios["PROCESSO"] == "MUDAR_CONTATO_LIDOS",
            "CHAVE RELATORIO",
        ].head().to_list()
    )
print("--------------------------------------------------------------")

qtd_reenviar_disparo_novamente = int(mask_para_reenviar_disparo_novamente.sum())
print(f"ENVIAR DISPARO NOVAMENTE (PROCESSO): {qtd_reenviar_disparo_novamente} registros")
if qtd_reenviar_disparo_novamente > 0:
    print("   -> Primeiras CHAVES:")
    print(
        df_usuarios.loc[
            df_usuarios["PROCESSO"] == "ENVIAR_DISPARO_NOVAMENTE",
            "CHAVE RELATORIO",
        ].head().to_list()
    )
print("--------------------------------------------------------------")

qtd_lidos_sem_resposta = int(mask_lida_sem_resposta.sum())
print(f"LIDOS SEM RESPOSTA (PROCESSO): {qtd_lidos_sem_resposta} registros")
if qtd_lidos_sem_resposta > 0:
    print("   -> Primeiras CHAVES:")
    print(
        df_usuarios.loc[
            df_usuarios["PROCESSO"] == "LIDOS_SEM_RESPOSTA",
            "CHAVE RELATORIO",
        ].head().to_list()
    )

qtd_lida_resposta_sim_sem_resposta = int(mask_lida_resposta_sim_sem_resposta.sum())
print(f"LIDOS RESPOSTA SIM SEM RESPOSTA (PROCESSO): {qtd_lida_resposta_sim_sem_resposta} registros")
if qtd_lida_resposta_sim_sem_resposta > 0:
    print("   -> Primeiras CHAVES:")
    print(
        df_usuarios.loc[
            df_usuarios["PROCESSO"] == "ENCERRAR_CONTATO_LIDOS_RESPOSTA_SIM",
            "CHAVE RELATORIO",
        ].head().to_list()
    )
