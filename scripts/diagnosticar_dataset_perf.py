import argparse
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config.schemas import COLUNAS_FINAIS_DATASET
from src.services.dataset_metricas_service import preparar_contagens_status
from src.services.dataset_service import (
    _carregar_status_para_lookup,
    _enriquecer_dataset_com_status,
    _montar_df_final_complicacao,
    _ordenar_por_chave_principal,
)
from src.services.texto_service import (
    normalizar_texto_serie as _normalizar_texto_serie,
    simplificar_texto as _simplificar_texto,
)
from src.services.validacao_service import validar_colunas_origem_dataset_complicacao
from src.utils.arquivos import ler_arquivo_csv


def log(etapa, inicio=None, extra=""):
    agora = time.perf_counter()
    if inicio is None:
        print(f"[INICIO] {etapa} {extra}".strip(), flush=True)
    else:
        print(f"[FIM] {etapa}: {agora - inicio:.2f}s {extra}".strip(), flush=True)
    return agora


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--origem", default="src/data/COMPLICACAO MAIO.xlsx")
    parser.add_argument("--status", default="src/data/arquivo_limpo/status_complicacao.csv")
    parser.add_argument(
        "--saida",
        default="src/data/arquivo_limpo/diagnostico_complicacao_status.xlsx",
    )
    args = parser.parse_args()

    total_inicio = log("TOTAL")

    inicio = log("LEITURA_ARQUIVO_COMPLICACAO")
    df = ler_arquivo_csv(args.origem)
    df.columns = [str(col).strip() for col in df.columns]
    log("LEITURA_ARQUIVO_COMPLICACAO", inicio, f"linhas={len(df)} colunas={len(df.columns)}")

    inicio = log("VALIDACAO_COLUNAS_ORIGEM")
    validacao = validar_colunas_origem_dataset_complicacao(df.columns, contexto="complicacao")
    log("VALIDACAO_COLUNAS_ORIGEM", inicio, f"ok={validacao.get('ok')}")
    if not validacao.get("ok"):
        print(validacao, flush=True)
        return

    inicio = log("CARREGAR_STATUS_LOOKUP")
    resultado_status = _carregar_status_para_lookup(args.status)
    log(
        "CARREGAR_STATUS_LOOKUP",
        inicio,
        (
            f"ok={resultado_status.get('ok')} "
            f"status_full={len(resultado_status.get('df_status_full', []))}"
        ),
    )
    if not resultado_status.get("ok"):
        print(resultado_status, flush=True)
        return

    df_status_full = resultado_status["df_status_full"]
    df_status_por_contato = resultado_status["df_status_por_contato"]

    inicio = log("PREPARAR_CONTAGENS_STATUS")
    contagens = preparar_contagens_status(df_status_full)
    log("PREPARAR_CONTAGENS_STATUS", inicio, f"ok={contagens.get('ok')}")
    if not contagens.get("ok"):
        print(contagens, flush=True)
        return

    inicio = log("MONTAR_ABA_USUARIOS")
    df_usuarios = _montar_df_final_complicacao(df.copy())
    log("MONTAR_ABA_USUARIOS", inicio, f"linhas={len(df_usuarios)}")

    inicio = log("ENRIQUECER_ABA_PRINCIPAL")
    resultado_usuarios = _enriquecer_dataset_com_status(
        df_usuarios,
        df_status_full,
        df_status_por_contato,
        contagens_status_preparadas=contagens,
    )
    log(
        "ENRIQUECER_ABA_PRINCIPAL",
        inicio,
        (
            f"ok={resultado_usuarios.get('ok')} "
            f"match={resultado_usuarios.get('total_match')} "
            f"sem_match={resultado_usuarios.get('total_sem_match')}"
        ),
    )
    if not resultado_usuarios.get("ok"):
        print(resultado_usuarios, flush=True)
        return

    inicio = log("ORDENAR_ABA_PRINCIPAL")
    df_usuarios = _ordenar_por_chave_principal(resultado_usuarios["df_enriquecido"])
    log("ORDENAR_ABA_PRINCIPAL", inicio, f"linhas={len(df_usuarios)}")

    inicio = log("MONTAR_BASE_RESPONDIDOS")
    p1_preenchido = _normalizar_texto_serie(df["P1"]) != ""
    status_norm = df["STATUS"].apply(_simplificar_texto)
    mask_respondidos = p1_preenchido | status_norm.isin({"obito", "nao quis"})
    df_resp_base = df[mask_respondidos]
    log("MONTAR_BASE_RESPONDIDOS", inicio, f"linhas={len(df_resp_base)}")

    inicio = log("MONTAR_ABA_RESPONDIDOS")
    df_respondidos = _montar_df_final_complicacao(df_resp_base)
    log("MONTAR_ABA_RESPONDIDOS", inicio, f"linhas={len(df_respondidos)}")

    inicio = log("ENRIQUECER_ABA_RESPONDIDOS")
    resultado_respondidos = _enriquecer_dataset_com_status(
        df_respondidos,
        df_status_full,
        df_status_por_contato,
        contagens_status_preparadas=contagens,
    )
    log(
        "ENRIQUECER_ABA_RESPONDIDOS",
        inicio,
        (
            f"ok={resultado_respondidos.get('ok')} "
            f"match={resultado_respondidos.get('total_match')} "
            f"sem_match={resultado_respondidos.get('total_sem_match')}"
        ),
    )
    if not resultado_respondidos.get("ok"):
        print(resultado_respondidos, flush=True)
        return

    inicio = log("ORDENAR_ABA_RESPONDIDOS")
    df_respondidos = _ordenar_por_chave_principal(resultado_respondidos["df_enriquecido"])
    log("ORDENAR_ABA_RESPONDIDOS", inicio, f"linhas={len(df_respondidos)}")

    inicio = log("PERSISTENCIA_XLSX")
    Path(args.saida).parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(args.saida, engine="openpyxl") as writer:
        df_usuarios.to_excel(writer, sheet_name="usuarios", index=False)
        df_respondidos.to_excel(writer, sheet_name="usuarios_respondidos", index=False)
        pd.DataFrame(columns=COLUNAS_FINAIS_DATASET).to_excel(
            writer,
            sheet_name="usuarios_resolvidos",
            index=False,
        )
    log("PERSISTENCIA_XLSX", inicio, f"saida={args.saida}")

    log("TOTAL", total_inicio)


if __name__ == "__main__":
    main()
