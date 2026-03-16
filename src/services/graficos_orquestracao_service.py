from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import math
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


def _normalizar_numerico(serie):
    base = serie.astype(str).str.strip().str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(base, errors="coerce").fillna(0)


def _plot_horizontal_categoria(df, coluna_categoria, arquivo_png, titulo_base, periodo):
    if coluna_categoria not in df.columns or "TOTAL" not in df.columns:
        raise ValueError(f"{coluna_categoria}.csv precisa ter colunas {coluna_categoria} e TOTAL.")
    base = df[[coluna_categoria, "TOTAL"]].copy()
    base["TOTAL"] = _normalizar_numerico(base["TOTAL"])
    base = base.sort_values("TOTAL", ascending=False)
    total_geral = int(base["TOTAL"].sum())

    fig, ax = plt.subplots(figsize=(12, 6))
    barras = ax.barh(base[coluna_categoria], base["TOTAL"], color="#1f77b4")
    ax.invert_yaxis()
    ax.set_xlim(left=0)
    ax.set_xlabel("Totais")
    ax.set_ylabel("Status/Categoria")
    ax.set_title(f"{titulo_base} | Total: {total_geral} | Periodo: {periodo}")
    desloc = max(base["TOTAL"].max() * 0.01, 0.2) if len(base) > 0 else 0.2
    for barra, valor in zip(barras, base["TOTAL"]):
        ax.text(barra.get_width() + desloc, barra.get_y() + barra.get_height() / 2, f"{int(valor)}", va="center")
    fig.tight_layout()
    fig.savefig(arquivo_png, dpi=150)
    plt.close(fig)


def _plot_colunas_verticais(df, coluna_categoria, arquivo_png, titulo):
    if coluna_categoria not in df.columns or "TOTAL" not in df.columns:
        raise ValueError(f"{coluna_categoria}.csv precisa ter colunas {coluna_categoria} e TOTAL.")
    base = df[[coluna_categoria, "TOTAL"]].copy()
    base["TOTAL"] = _normalizar_numerico(base["TOTAL"])
    base = base.sort_values("TOTAL", ascending=False)

    fig, ax = plt.subplots(figsize=(8, 5))
    barras = ax.bar(base[coluna_categoria].astype(str), base["TOTAL"], color="#457b9d")
    ax.set_title(titulo)
    ax.set_xlabel("Categorias")
    ax.set_ylabel("Numero de ocorrencias")
    desloc = max(base["TOTAL"].max() * 0.015, 0.2) if len(base) > 0 else 0.2
    for barra, valor in zip(barras, base["TOTAL"]):
        ax.text(barra.get_x() + barra.get_width() / 2, barra.get_height() + desloc, f"{int(valor)}", ha="center")
    fig.tight_layout()
    fig.savefig(arquivo_png, dpi=150)
    plt.close(fig)


def _plot_acao_pizza(df, arquivo_png):
    if "ACAO" not in df.columns or "TOTAL" not in df.columns:
        raise ValueError("ACAO.csv precisa ter colunas ACAO e TOTAL.")
    base = df[["ACAO", "TOTAL"]].copy()
    base["TOTAL"] = _normalizar_numerico(base["TOTAL"])
    base = base[base["TOTAL"] > 0].sort_values("TOTAL", ascending=False)
    if base.empty:
        raise ValueError("ACAO.csv sem valores positivos para gerar pizza.")

    total_geral = float(base["TOTAL"].sum())

    fig, ax = plt.subplots(figsize=(8, 8))
    wedges, _ = ax.pie(
        base["TOTAL"],
        startangle=90,
        shadow=True,
    )

    # Rotulo externo com valor + percentual, distribuido por lado para reduzir sobreposicao.
    entradas_esquerda = []
    entradas_direita = []
    for wedge, valor in zip(wedges, base["TOTAL"]):
        angulo = (wedge.theta1 + wedge.theta2) / 2.0
        angulo_rad = math.radians(angulo)
        x = math.cos(angulo_rad)
        y = math.sin(angulo_rad)
        pct = (float(valor) / total_geral * 100.0) if total_geral > 0 else 0.0
        item = {
            "x": x,
            "y": y,
            "texto": f"{int(valor)} ({pct:.1f}%)",
            "wedge": wedge,
        }
        if x >= 0:
            entradas_direita.append(item)
        else:
            entradas_esquerda.append(item)

    def _espalhar_vertical(itens, gap=0.14, y_min=-1.15, y_max=1.15):
        if not itens:
            return itens
        itens_ordenados = sorted(itens, key=lambda i: i["y"])
        for item in itens_ordenados:
            item["y_texto"] = max(y_min, min(y_max, item["y"] * 1.22))

        # Passo 1: garante espacamento minimo subindo.
        y_atual = y_min
        for item in itens_ordenados:
            y_atual = max(y_atual + gap, item["y_texto"])
            y_atual = min(y_atual, y_max)
            item["y_texto"] = y_atual

        # Passo 2: se estourou em cima, redistribui descendo.
        y_atual = y_max
        for item in reversed(itens_ordenados):
            y_atual = min(y_atual, item["y_texto"])
            item["y_texto"] = y_atual
            y_atual -= gap

        return itens_ordenados

    entradas_esquerda = _espalhar_vertical(entradas_esquerda)
    entradas_direita = _espalhar_vertical(entradas_direita)

    for item in entradas_esquerda + entradas_direita:
        lado = 1 if item["x"] >= 0 else -1
        x_texto = 1.44 * lado
        ha = "left" if lado > 0 else "right"
        ax.annotate(
            item["texto"],
            xy=(item["x"], item["y"]),
            xytext=(x_texto, item["y_texto"]),
            ha=ha,
            va="center",
            fontsize=11,
            arrowprops={"arrowstyle": "-", "color": "#666666", "lw": 0.8},
        )

    ax.set_title("Distribuicao de Acao da Orquestracao")
    ax.axis("equal")
    fig.subplots_adjust(right=0.74)
    ax.legend(
        wedges,
        base["ACAO"],
        title="Acao",
        loc="lower left",
        bbox_to_anchor=(1.02, 0.12),
        frameon=False,
    )
    fig.tight_layout()
    fig.savefig(arquivo_png, dpi=150)
    plt.close(fig)


def _coletar_pastas_origem(raiz_analise_contexto, pastas_origem_csv):
    if pastas_origem_csv:
        if isinstance(pastas_origem_csv, (str, Path)):
            return [Path(pastas_origem_csv)]
        return [Path(item) for item in pastas_origem_csv]
    raiz = Path(raiz_analise_contexto) / "orquestracao"
    if not raiz.exists():
        return []
    return sorted(
        [p for p in raiz.iterdir() if p.is_dir() and p.name.lower() != "lixeira"],
        key=lambda p: p.name,
    )


def gerar_graficos_orquestracao(
    contexto,
    raiz_analise_contexto="src/data/analise_dados/complicacao",
    pastas_origem_csv=None,
):
    pastas_origem = _coletar_pastas_origem(raiz_analise_contexto, pastas_origem_csv)
    pasta_base_imagens = Path("src/data/analise_dados/imagens") / contexto / "orquestracao"
    pasta_base_imagens.mkdir(parents=True, exist_ok=True)

    manifests = []
    total_gerados = 0
    total_ignorados = 0

    for pasta_origem in pastas_origem:
        periodo = pasta_origem.name
        pasta_saida = pasta_base_imagens / periodo
        pasta_saida.mkdir(parents=True, exist_ok=True)

        manifest = {
            "etapa": "orquestracao",
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
            for nome in ("PROCESSO.csv", "ACAO.csv", "STATUS_CHAVE.csv"):
                manifest["graficos_ignorados"].append(
                    {
                        "id": nome,
                        "arquivo_csv": str(pasta_origem / nome),
                        "motivo": f"Matplotlib indisponivel no ambiente: {_ERRO_MATPLOTLIB}",
                    }
                )
        else:
            specs = [
                ("PROCESSO.csv", "orquestracao_processo_{contexto}.png"),
                ("ACAO.csv", "orquestracao_acao_pizza_{contexto}.png"),
                ("ACAO.csv", "orquestracao_acao_colunas_{contexto}.png"),
                ("STATUS_CHAVE.csv", "orquestracao_status_chave_{contexto}.png"),
            ]
            for nome_csv, nome_png in specs:
                arquivo_csv = pasta_origem / nome_csv
                if not arquivo_csv.exists():
                    manifest["graficos_ignorados"].append(
                        {
                            "id": nome_csv,
                            "arquivo_csv": str(arquivo_csv),
                            "motivo": "Arquivo CSV nao encontrado.",
                        }
                    )
                    continue
                try:
                    df = ler_arquivo_csv(arquivo_csv)
                    if df.empty:
                        raise ValueError("Arquivo CSV vazio.")
                    arquivo_png = pasta_saida / nome_png.format(contexto=contexto)
                    if nome_csv == "PROCESSO.csv":
                        _plot_horizontal_categoria(
                            df,
                            "PROCESSO",
                            arquivo_png,
                            "Distribuicao de Processo da Orquestracao",
                            periodo,
                        )
                    elif nome_csv == "ACAO.csv" and "pizza" in nome_png:
                        _plot_acao_pizza(df, arquivo_png)
                    elif nome_csv == "ACAO.csv":
                        _plot_horizontal_categoria(
                            df,
                            "ACAO",
                            arquivo_png,
                            "Distribuicao de Acao da Orquestracao",
                            periodo,
                        )
                    else:
                        _plot_colunas_verticais(
                            df,
                            "STATUS_CHAVE",
                            arquivo_png,
                            "Distribuicao de Status Chave da Orquestracao",
                        )
                    manifest["graficos_gerados"].append(
                        {
                            "id": nome_csv,
                            "arquivo_csv": str(arquivo_csv),
                            "arquivo_png": str(arquivo_png),
                        }
                    )
                except Exception as exc:
                    manifest["graficos_ignorados"].append(
                        {
                            "id": nome_csv,
                            "arquivo_csv": str(arquivo_csv),
                            "motivo": str(exc),
                        }
                    )

        manifest["mensagens_validacao"].append(f"Total de graficos gerados: {len(manifest['graficos_gerados'])}.")
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
