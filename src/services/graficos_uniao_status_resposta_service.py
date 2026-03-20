from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import re
from typing import Any

import pandas as pd

from src.utils.arquivos import ler_arquivo_csv

plt: Any
try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception as exc:  # pragma: no cover - fallback de ambiente
    plt = None
    _ERRO_MATPLOTLIB = str(exc)
else:
    _ERRO_MATPLOTLIB = ""


_SPECS_GRAFICOS = [
    {
        "id": "status_distribuicao",
        "prefixo_csv": "STATUS",
        "excluir_subprefixos": ["DATA_"],
        "arquivo_png": "uniao_status_resposta_status_distribuicao_{contexto}.png",
    },
    {
        "id": "resumo_metricas",
        "prefixo_csv": "RESUMO",
        "arquivo_png": "uniao_status_resposta_resumo_metricas_{contexto}.png",
    },
    {
        "id": "resposta_lida_distribuicao",
        "prefixo_csv": "RESPOSTA_LIDA",
        "excluir_subprefixos": ["DATA_"],
        "arquivo_png": "uniao_status_resposta_resposta_lida_distribuicao_{contexto}.png",
    },
    {
        "id": "lida_data",
        "prefixo_csv": "LIDA_DATA",
        "arquivo_png": "uniao_status_resposta_lida_por_data_{contexto}.png",
    },
    {
        "id": "resposta_lida_data",
        "prefixo_csv": "RESPOSTA_LIDA_DATA",
        "arquivo_png": "uniao_status_resposta_resposta_lida_por_data_{contexto}.png",
    },
    {
        "id": "status_data",
        "prefixo_csv": "STATUS_DATA",
        "arquivo_png": "uniao_status_resposta_status_por_data_{contexto}.png",
    },
]


def _slug_coluna(texto):
    base = str(texto or "").strip().lower()
    return re.sub(r"[^a-z0-9]+", "_", base).strip("_")


def _achar_coluna(df, candidatas):
    mapa = {_slug_coluna(col): col for col in df.columns}
    for candidata in candidatas:
        achada = mapa.get(_slug_coluna(candidata))
        if achada is not None:
            return achada
    return None


def _normalizar_numerico(serie):
    base = serie.astype(str).str.strip().str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(base, errors="coerce").fillna(0)


def _normalizar_data_coluna(serie):
    texto = serie.astype(str).str.strip()
    mask_iso = texto.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    dt = pd.Series(pd.NaT, index=texto.index, dtype="datetime64[ns]")
    if mask_iso.any():
        dt.loc[mask_iso] = pd.to_datetime(texto.loc[mask_iso], errors="coerce", format="%Y-%m-%d")
    if (~mask_iso).any():
        dt.loc[~mask_iso] = pd.to_datetime(texto.loc[~mask_iso], errors="coerce", dayfirst=True)
    return dt.dt.strftime("%Y-%m-%d").fillna(texto)


def _descobrir_arquivo_csv(pasta_origem, prefixo_csv, excluir_subprefixos=None):
    excluir_subprefixos = tuple(excluir_subprefixos or [])
    candidatos = []
    for arquivo in pasta_origem.glob(f"{prefixo_csv}_*.csv"):
        sufixo = arquivo.stem[len(prefixo_csv) + 1 :]
        if any(sufixo.startswith(sub) for sub in excluir_subprefixos):
            continue
        candidatos.append(arquivo)
    arquivos = sorted(candidatos, key=lambda item: item.stat().st_mtime, reverse=True)
    if arquivos:
        return arquivos[0]

    arquivo_fixo = pasta_origem / f"{prefixo_csv}.csv"
    if arquivo_fixo.exists():
        return arquivo_fixo
    return None


def _extrair_periodo(arquivo_csv):
    nome = arquivo_csv.stem
    partes = nome.split("_")
    if len(partes) <= 1:
        return "sem periodo"
    return " ".join(partes[1:])


def _plot_status(df, arquivo_png, periodo):
    col_status = _achar_coluna(df, ["status"])
    col_total = _achar_coluna(df, ["total", "valor"])
    if not col_status or not col_total:
        raise ValueError("Colunas obrigatorias ausentes para STATUS: status/total.")

    base = df[[col_status, col_total]].copy()
    base.columns = ["status", "total"]
    base["total"] = _normalizar_numerico(base["total"])
    base = base.sort_values("total", ascending=False)
    total_geral = int(base["total"].sum())

    fig, ax = plt.subplots(figsize=(12, 6))
    barras = ax.barh(base["status"], base["total"], color="#1f77b4")
    ax.invert_yaxis()
    ax.set_xlim(left=0)
    ax.set_xlabel("Total")
    ax.set_ylabel("Status")
    ax.set_title(
        f"Distribuicao de Status das Mensagens Enviadas | Total: {total_geral} | Periodo: {periodo}"
    )
    desloc = max(base["total"].max() * 0.01, 0.2) if len(base) > 0 else 0.2
    for barra, valor in zip(barras, base["total"]):
        ax.text(barra.get_width() + desloc, barra.get_y() + barra.get_height() / 2, f"{int(valor)}", va="center")
    fig.tight_layout()
    fig.savefig(arquivo_png, dpi=150)
    plt.close(fig)


def _plot_resumo(df, arquivo_png):
    col_metrica = _achar_coluna(df, ["metrica", "metrica"])
    col_valor = _achar_coluna(df, ["valor", "total"])
    if not col_metrica or not col_valor:
        raise ValueError("Colunas obrigatorias ausentes para RESUMO: metrica/valor.")

    base = df[[col_metrica, col_valor]].copy()
    base.columns = ["metrica", "valor"]
    base["valor"] = _normalizar_numerico(base["valor"])
    base = base.sort_values("valor", ascending=False)

    fig, ax = plt.subplots(figsize=(12, 6))
    barras = ax.barh(base["metrica"], base["valor"], color="#2a9d8f")
    ax.invert_yaxis()
    ax.set_title("Resumo das Metricas de Processamento dos Dados")
    ax.set_xlabel("Valor")
    ax.set_ylabel("Metrica")
    desloc = max(base["valor"].max() * 0.01, 0.2) if len(base) > 0 else 0.2
    for barra, valor in zip(barras, base["valor"]):
        ax.text(barra.get_width() + desloc, barra.get_y() + barra.get_height() / 2, f"{int(valor)}", va="center")
    fig.tight_layout()
    fig.savefig(arquivo_png, dpi=150)
    plt.close(fig)


def _plot_resposta_lida(df, arquivo_png):
    col_resposta = _achar_coluna(df, ["resposta"])
    col_total = _achar_coluna(df, ["total", "valor"])
    if not col_resposta or not col_total:
        raise ValueError("Colunas obrigatorias ausentes para RESPOSTA_LIDA: resposta/total.")

    base = df[[col_resposta, col_total]].copy()
    base.columns = ["resposta", "total"]
    base["total"] = _normalizar_numerico(base["total"])

    fig, ax = plt.subplots(figsize=(8, 5))
    barras = ax.bar(base["resposta"], base["total"], color="#457b9d")
    ax.set_title("Distribuicao das Respostas dos Usuarios")
    ax.set_xlabel("Resposta")
    ax.set_ylabel("Total")
    desloc = max(base["total"].max() * 0.015, 0.2) if len(base) > 0 else 0.2
    for barra, valor in zip(barras, base["total"]):
        ax.text(barra.get_x() + barra.get_width() / 2, barra.get_height() + desloc, f"{int(valor)}", ha="center")
    fig.tight_layout()
    fig.savefig(arquivo_png, dpi=150)
    plt.close(fig)


def _plot_lida_data(df, arquivo_png):
    col_data = _achar_coluna(df, ["dt_envio", "data", "dt"])
    col_total = _achar_coluna(df, ["total_status_lida", "total", "valor"])
    if not col_data or not col_total:
        raise ValueError("Colunas obrigatorias ausentes para LIDA_DATA: dt_envio/total_status_lida.")

    base = df[[col_data, col_total]].copy()
    base.columns = ["dt_envio", "total"]
    base["dt_envio"] = _normalizar_data_coluna(base["dt_envio"])
    base["total"] = _normalizar_numerico(base["total"])
    base = base.sort_values("dt_envio")

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(base["dt_envio"], base["total"], marker="o", color="#e76f51")
    ax.set_title("Evolucao Diaria das Mensagens Lidas")
    ax.set_xlabel("Data")
    ax.set_ylabel('Quantidade de status "Lida"')
    desloc = max(base["total"].max() * 0.015, 0.2) if len(base) > 0 else 0.2
    for x_val, y_val in zip(base["dt_envio"], base["total"]):
        ax.text(x_val, y_val + desloc, f"{int(y_val)}", ha="center")
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    fig.tight_layout()
    fig.savefig(arquivo_png, dpi=150)
    plt.close(fig)


def _plot_resposta_lida_data(df, arquivo_png):
    col_data = _achar_coluna(df, ["dt_envio", "data", "dt"])
    col_resposta = _achar_coluna(df, ["resposta"])
    col_total = _achar_coluna(df, ["total", "valor"])
    if not col_data or not col_resposta or not col_total:
        raise ValueError("Colunas obrigatorias ausentes para RESPOSTA_LIDA_DATA: dt_envio/resposta/total.")

    base = df[[col_data, col_resposta, col_total]].copy()
    base.columns = ["dt_envio", "resposta", "total"]
    base["dt_envio"] = _normalizar_data_coluna(base["dt_envio"])
    base["total"] = _normalizar_numerico(base["total"])

    pivot = (
        base.pivot_table(index="dt_envio", columns="resposta", values="total", aggfunc="sum", fill_value=0)
        .reset_index()
        .sort_values("dt_envio")
    )
    ordem_base = ["Sim", "Nao", "Sem resposta"]
    colunas_resposta = [item for item in ordem_base if item in set(pivot.columns)]
    extras = [item for item in pivot.columns if item not in {"dt_envio", *colunas_resposta}]
    colunas_resposta.extend(sorted(extras))

    fig, ax = plt.subplots(figsize=(10, 5))
    for resposta in colunas_resposta:
        ax.plot(pivot["dt_envio"], pivot[resposta], marker="o", label=resposta)
    ax.set_title("Evolucao Diaria das Respostas dos Usuarios")
    ax.set_xlabel("Data")
    ax.set_ylabel("Total")
    ax.grid(True, alpha=0.25)
    ax.legend()
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    fig.tight_layout()
    fig.savefig(arquivo_png, dpi=150)
    plt.close(fig)


def _plot_status_data(df, arquivo_png):
    col_data = _achar_coluna(df, ["dt_envio", "data", "dt"])
    col_status = _achar_coluna(df, ["status"])
    col_total = _achar_coluna(df, ["total", "valor"])
    if not col_data or not col_status or not col_total:
        raise ValueError("Colunas obrigatorias ausentes para STATUS_DATA: dt_envio/status/total.")

    base = df[[col_data, col_status, col_total]].copy()
    base.columns = ["dt_envio", "status", "total"]
    base["dt_envio"] = _normalizar_data_coluna(base["dt_envio"])
    base["total"] = _normalizar_numerico(base["total"])

    pivot = (
        base.pivot_table(index="dt_envio", columns="status", values="total", aggfunc="sum", fill_value=0)
        .reset_index()
        .sort_values("dt_envio")
    )
    colunas_status = [item for item in pivot.columns if item != "dt_envio"]

    fig, ax = plt.subplots(figsize=(12, 6))
    for status in colunas_status:
        ax.plot(pivot["dt_envio"], pivot[status], marker="o", label=status)
    ax.set_title("Evolucao Diaria dos Status de Entrega das Mensagens")
    ax.set_xlabel("Data")
    ax.set_ylabel("Total")
    ax.grid(True, alpha=0.25)
    ax.legend()
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    fig.tight_layout()
    fig.savefig(arquivo_png, dpi=150)
    plt.close(fig)


_PLOTTERS = {
    "status_distribuicao": _plot_status,
    "resumo_metricas": _plot_resumo,
    "resposta_lida_distribuicao": _plot_resposta_lida,
    "lida_data": _plot_lida_data,
    "resposta_lida_data": _plot_resposta_lida_data,
    "status_data": _plot_status_data,
}


def gerar_graficos_uniao_status_resposta(
    contexto,
    raiz_analise_contexto="src/data/analise_dados/complicacao",
    pasta_origem_csv=None,
):
    pasta_origem = Path(pasta_origem_csv) if pasta_origem_csv else Path(raiz_analise_contexto) / "uniao_status_resposta"
    pasta_imagens = Path("src/data/analise_dados/imagens") / contexto / "uniao_status_resposta"
    pasta_imagens.mkdir(parents=True, exist_ok=True)

    manifest = {
        "etapa": "uniao_status_resposta",
        "contexto": contexto,
        "gerado_em": datetime.now().isoformat(timespec="seconds"),
        "pasta_origem_csv": str(pasta_origem),
        "pasta_saida_imagens": str(pasta_imagens),
        "graficos_gerados": [],
        "graficos_ignorados": [],
        "mensagens_validacao": [],
    }

    if plt is None:
        for spec in _SPECS_GRAFICOS:
            manifest["graficos_ignorados"].append(
                {
                    "id": spec["id"],
                    "motivo": f"Matplotlib indisponivel no ambiente: {_ERRO_MATPLOTLIB}",
                }
            )
        caminho_manifest = pasta_imagens / "manifest.json"
        caminho_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "ok": True,
            "pasta_saida": str(pasta_imagens),
            "arquivo_manifest": str(caminho_manifest),
            "graficos_gerados": [],
            "graficos_ignorados": manifest["graficos_ignorados"],
            "mensagens": ["Geracao de graficos ignorada: Matplotlib indisponivel."],
        }

    for spec in _SPECS_GRAFICOS:
        arquivo_csv = _descobrir_arquivo_csv(
            pasta_origem,
            spec["prefixo_csv"],
            excluir_subprefixos=spec.get("excluir_subprefixos"),
        )
        if arquivo_csv is None:
            manifest["graficos_ignorados"].append(
                {
                    "id": spec["id"],
                    "motivo": f"Arquivo CSV nao encontrado para prefixo {spec['prefixo_csv']}.",
                }
            )
            continue

        arquivo_png = pasta_imagens / spec["arquivo_png"].format(contexto=contexto)
        try:
            df = ler_arquivo_csv(arquivo_csv)
            if df.empty:
                raise ValueError("Arquivo CSV vazio.")
            periodo = _extrair_periodo(arquivo_csv)
            plotter = _PLOTTERS[spec["id"]]
            if spec["id"] == "status_distribuicao":
                plotter(df, arquivo_png, periodo)
            else:
                plotter(df, arquivo_png)
            manifest["graficos_gerados"].append(
                {
                    "id": spec["id"],
                    "arquivo_csv": str(arquivo_csv),
                    "arquivo_png": str(arquivo_png),
                }
            )
        except Exception as exc:
            manifest["graficos_ignorados"].append(
                {
                    "id": spec["id"],
                    "arquivo_csv": str(arquivo_csv),
                    "motivo": str(exc),
                }
            )

    manifest["mensagens_validacao"].append(
        f"Total de graficos gerados: {len(manifest['graficos_gerados'])}."
    )
    manifest["mensagens_validacao"].append(
        f"Total de graficos ignorados: {len(manifest['graficos_ignorados'])}."
    )
    caminho_manifest = pasta_imagens / "manifest.json"
    caminho_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "ok": True,
        "pasta_saida": str(pasta_imagens),
        "arquivo_manifest": str(caminho_manifest),
        "graficos_gerados": manifest["graficos_gerados"],
        "graficos_ignorados": manifest["graficos_ignorados"],
        "mensagens": manifest["mensagens_validacao"],
    }
