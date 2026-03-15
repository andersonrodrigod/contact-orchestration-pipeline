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
except Exception as exc:  # pragma: no cover
    plt = None
    _ERRO_MATPLOTLIB = str(exc)
else:
    _ERRO_MATPLOTLIB = ""


_SPECS_BARRA_HORIZONTAL = [
    (
        "QT_SOMA_COLUNA_SEM_LIDA.csv",
        "Distribuicao dos Status de Entrega e Leitura das Mensagens Sem Lida (QT)",
        "status_enviado_qt_soma_coluna_sem_lida_{contexto}.png",
    ),
    (
        "QT_SOMA_COLUNA_SEM_RESPOSTA_LIDA.csv",
        "Distribuicao dos Status de Entrega e Leitura das Mensagens Sem Resposta (QT)",
        "status_enviado_qt_soma_coluna_sem_resposta_lida_{contexto}.png",
    ),
    (
        "QT_VALIDOS.csv",
        "Quantidade de Ocorrencias de Status de Entrega e Leitura das Mensagens por Usuario (QT)",
        "status_enviado_qt_validos_{contexto}.png",
    ),
    (
        "SEM_QT_SOMA_COLUNA_SEM_LIDA.csv",
        "Distribuicao dos Status de Entrega e Leitura das Mensagens Sem Lida",
        "status_enviado_sem_qt_soma_coluna_sem_lida_{contexto}.png",
    ),
    (
        "SEM_QT_SOMA_COLUNA_SEM_RESPOSTA_LIDA.csv",
        "Distribuicao dos Status de Entrega e Leitura das Mensagens Sem Resposta",
        "status_enviado_sem_qt_soma_coluna_sem_resposta_lida_{contexto}.png",
    ),
    (
        "SEM_QT_VALIDOS.csv",
        "Quantidade de Ocorrencias de Status de Entrega e Leitura das Mensagens por Usuario",
        "status_enviado_sem_qt_validos_{contexto}.png",
    ),
]

_SPEC_QT_TELEFONES = (
    "QT_TELEFONES_OCORRENCIAS.csv",
    "Numero de Telefones Necessarios para Enviar a Pesquisa",
    "status_enviado_qt_telefones_ocorrencias_{contexto}.png",
)


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


def _plot_barra_horizontal(df, arquivo_png, titulo_base, periodo):
    col_categoria = _achar_coluna(df, ["coluna", "status", "categoria", "metrica"])
    col_total = _achar_coluna(df, ["soma_total", "total_registros_validos", "total", "valor"])
    if not col_categoria or not col_total:
        raise ValueError("Colunas obrigatorias nao encontradas para grafico de barra horizontal.")

    base = df[[col_categoria, col_total]].copy()
    base.columns = ["categoria", "total"]
    base["total"] = _normalizar_numerico(base["total"])
    base = base.sort_values("total", ascending=False)
    total_geral = int(base["total"].sum())

    fig, ax = plt.subplots(figsize=(12, 6))
    barras = ax.barh(base["categoria"], base["total"], color="#1f77b4")
    ax.invert_yaxis()
    ax.set_xlim(left=0)
    ax.set_xlabel("Total")
    ax.set_ylabel("Status")
    ax.set_title(f"{titulo_base} | Total: {total_geral} | Periodo: {periodo}")
    desloc = max(base["total"].max() * 0.01, 0.2) if len(base) > 0 else 0.2
    for barra, valor in zip(barras, base["total"]):
        ax.text(barra.get_width() + desloc, barra.get_y() + barra.get_height() / 2, f"{int(valor)}", va="center")
    fig.tight_layout()
    fig.savefig(arquivo_png, dpi=150)
    plt.close(fig)


def _plot_qt_telefones(df, arquivo_png):
    col_x = _achar_coluna(df, ["valor_qt_telefones", "qt_telefones", "valor"])
    col_y = _achar_coluna(df, ["ocorrencias", "total", "valor"])
    if not col_x or not col_y:
        raise ValueError("Colunas obrigatorias nao encontradas para QT_TELEFONES_OCORRENCIAS.")

    base = df[[col_x, col_y]].copy()
    base.columns = ["qt_telefones", "ocorrencias"]
    base["ocorrencias"] = _normalizar_numerico(base["ocorrencias"])
    base["qt_telefones_num"] = pd.to_numeric(base["qt_telefones"], errors="coerce")
    base = base.sort_values(
        ["qt_telefones_num", "qt_telefones"],
        ascending=[True, True],
        na_position="last",
    )

    fig, ax = plt.subplots(figsize=(8, 5))
    barras = ax.bar(base["qt_telefones"].astype(str), base["ocorrencias"], color="#457b9d")
    ax.set_title("Numero de Telefones Necessarios para Enviar a Pesquisa")
    ax.set_xlabel("Quantidade de telefones utilizados")
    ax.set_ylabel("Numero de ocorrencias")
    desloc = max(base["ocorrencias"].max() * 0.015, 0.2) if len(base) > 0 else 0.2
    for barra, valor in zip(barras, base["ocorrencias"]):
        ax.text(barra.get_x() + barra.get_width() / 2, barra.get_height() + desloc, f"{int(valor)}", ha="center")
    fig.tight_layout()
    fig.savefig(arquivo_png, dpi=150)
    plt.close(fig)


def _coletar_pastas_origem(raiz_analise_contexto, pastas_origem_csv):
    if pastas_origem_csv:
        if isinstance(pastas_origem_csv, (str, Path)):
            return [Path(pastas_origem_csv)]
        return [Path(item) for item in pastas_origem_csv]

    raiz = Path(raiz_analise_contexto) / "status_enviado"
    if not raiz.exists():
        return []
    return sorted(
        [p for p in raiz.iterdir() if p.is_dir() and p.name.lower() != "lixeira"],
        key=lambda p: p.name,
    )


def _registrar_ignorado(manifest, grafico_id, arquivo_csv, motivo):
    manifest["graficos_ignorados"].append(
        {
            "id": grafico_id,
            "arquivo_csv": str(arquivo_csv),
            "motivo": motivo,
        }
    )


def gerar_graficos_status_enviado(
    contexto,
    raiz_analise_contexto="src/data/analise_dados/complicacao",
    pastas_origem_csv=None,
):
    pastas_origem = _coletar_pastas_origem(raiz_analise_contexto, pastas_origem_csv)
    pasta_base_imagens = Path("src/data/analise_dados/imagens") / contexto / "status_enviado"
    pasta_base_imagens.mkdir(parents=True, exist_ok=True)

    manifests = []
    total_gerados = 0
    total_ignorados = 0

    for pasta_origem in pastas_origem:
        periodo = pasta_origem.name
        pasta_saida = pasta_base_imagens / periodo
        pasta_saida.mkdir(parents=True, exist_ok=True)
        manifest = {
            "etapa": "status_enviado",
            "contexto": contexto,
            "periodo": periodo,
            "gerado_em": datetime.now().isoformat(timespec="seconds"),
            "pasta_origem_csv": str(pasta_origem),
            "pasta_saida_imagens": str(pasta_saida),
            "graficos_gerados": [],
            "graficos_ignorados": [],
            "mensagens_validacao": [],
        }

        if plt is None:
            for nome_csv, _, _ in _SPECS_BARRA_HORIZONTAL:
                _registrar_ignorado(
                    manifest,
                    nome_csv,
                    pasta_origem / nome_csv,
                    f"Matplotlib indisponivel no ambiente: {_ERRO_MATPLOTLIB}",
                )
            _registrar_ignorado(
                manifest,
                _SPEC_QT_TELEFONES[0],
                pasta_origem / _SPEC_QT_TELEFONES[0],
                f"Matplotlib indisponivel no ambiente: {_ERRO_MATPLOTLIB}",
            )
        else:
            for nome_csv, titulo_base, nome_png in _SPECS_BARRA_HORIZONTAL:
                arquivo_csv = pasta_origem / nome_csv
                if not arquivo_csv.exists():
                    _registrar_ignorado(manifest, nome_csv, arquivo_csv, "Arquivo CSV nao encontrado.")
                    continue
                try:
                    df = ler_arquivo_csv(arquivo_csv)
                    if df.empty:
                        raise ValueError("Arquivo CSV vazio.")
                    arquivo_png = pasta_saida / nome_png.format(contexto=contexto)
                    _plot_barra_horizontal(df, arquivo_png, titulo_base=titulo_base, periodo=periodo)
                    manifest["graficos_gerados"].append(
                        {
                            "id": nome_csv,
                            "arquivo_csv": str(arquivo_csv),
                            "arquivo_png": str(arquivo_png),
                        }
                    )
                except Exception as exc:
                    _registrar_ignorado(manifest, nome_csv, arquivo_csv, str(exc))

            nome_csv, _, nome_png = _SPEC_QT_TELEFONES
            arquivo_csv = pasta_origem / nome_csv
            if not arquivo_csv.exists():
                _registrar_ignorado(manifest, nome_csv, arquivo_csv, "Arquivo CSV nao encontrado.")
            else:
                try:
                    df = ler_arquivo_csv(arquivo_csv)
                    if df.empty:
                        raise ValueError("Arquivo CSV vazio.")
                    arquivo_png = pasta_saida / nome_png.format(contexto=contexto)
                    _plot_qt_telefones(df, arquivo_png)
                    manifest["graficos_gerados"].append(
                        {
                            "id": nome_csv,
                            "arquivo_csv": str(arquivo_csv),
                            "arquivo_png": str(arquivo_png),
                        }
                    )
                except Exception as exc:
                    _registrar_ignorado(manifest, nome_csv, arquivo_csv, str(exc))

        manifest["mensagens_validacao"].append(
            f"Total de graficos gerados: {len(manifest['graficos_gerados'])}."
        )
        manifest["mensagens_validacao"].append(
            f"Total de graficos ignorados: {len(manifest['graficos_ignorados'])}."
        )
        caminho_manifest = pasta_saida / "manifest.json"
        caminho_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

        manifests.append(str(caminho_manifest))
        total_gerados += len(manifest["graficos_gerados"])
        total_ignorados += len(manifest["graficos_ignorados"])

    return {
        "ok": True,
        "pasta_base_saida": str(pasta_base_imagens),
        "pastas_origem_processadas": [str(p) for p in pastas_origem],
        "manifests": manifests,
        "total_graficos_gerados": total_gerados,
        "total_graficos_ignorados": total_ignorados,
        "mensagens": [
            f"Pastas processadas: {len(pastas_origem)}.",
            f"Total de graficos gerados: {total_gerados}.",
            f"Total de graficos ignorados: {total_ignorados}.",
        ],
    }
