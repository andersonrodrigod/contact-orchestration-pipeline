from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
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


_MESES_PT = {
    1: "Janeiro",
    2: "Fevereiro",
    3: "Marco",
    4: "Abril",
    5: "Maio",
    6: "Junho",
    7: "Julho",
    8: "Agosto",
    9: "Setembro",
    10: "Outubro",
    11: "Novembro",
    12: "Dezembro",
}


def _normalizar_numero(valor):
    texto = str(valor if valor is not None else "").strip()
    if texto == "":
        return 0.0
    texto = texto.replace(".", "").replace(",", ".")
    try:
        return float(texto)
    except ValueError:
        return 0.0


def _formatar_percentual(numerador, denominador):
    if denominador <= 0:
        return "-"
    return f"{(numerador / denominador) * 100:.1f}%"


def _resolver_coluna(df, nome):
    alvos = {str(col).strip().upper(): col for col in df.columns}
    return alvos.get(nome.upper().strip(), "")


def _serie_data(df, coluna):
    if coluna not in df.columns:
        return pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    texto = df[coluna].astype(str).str.strip()
    mask_iso = texto.str.match(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$", na=False)
    dt = pd.Series(pd.NaT, index=texto.index, dtype="datetime64[ns]")
    if mask_iso.any():
        dt.loc[mask_iso] = pd.to_datetime(texto.loc[mask_iso], errors="coerce", dayfirst=False)
    if (~mask_iso).any():
        dt.loc[~mask_iso] = pd.to_datetime(texto.loc[~mask_iso], errors="coerce", dayfirst=True)
    return dt


def _obter_mes_dt_internacao(df_origem):
    col_dt_int = _resolver_coluna(df_origem, "DT INTERNACAO")
    if not col_dt_int:
        return "Sem mes"
    dt_int = _serie_data(df_origem, col_dt_int).dropna()
    if dt_int.empty:
        return "Sem mes"
    mes_mais_frequente = int(dt_int.dt.month.mode().iloc[0])
    return _MESES_PT.get(mes_mais_frequente, "Sem mes")


def _autopct_com_quantidade(total):
    def _formatar(pct):
        valor = int(round((pct / 100.0) * total))
        return f"{valor}\n({pct:.1f}%)"

    return _formatar


def _partes_pizza_duas_fatias(positivo, total_base, label_positivo, label_negativo):
    positivo = max(0.0, float(positivo))
    total_base = max(0.0, float(total_base))
    negativo = max(0.0, total_base - positivo)
    if total_base <= 0:
        return ["Sem dados"], [1.0]
    if positivo <= 0 and negativo <= 0:
        return ["Sem dados"], [1.0]
    return [label_positivo, label_negativo], [positivo, negativo]


def _gerar_grafico_pizza_funil_geral_internacao(total, lida, respostas, mes_titulo, pasta_saida):
    arquivo_png_pizza = pasta_saida / "grafico_pizza_funil_geral_internacao.png"
    cores = {
        "Positivo": "#2652B5",
        "Negativo": "#E64A36",
        "Sem dados": "#B0BEC5",
    }
    fig, axes = plt.subplots(1, 3, figsize=(16, 5.8))
    fig.suptitle(
        f"Internacao - Pesquisa {mes_titulo} - Resumo Geral",
        fontsize=15,
        fontweight="bold",
        y=0.98,
    )

    configuracoes = [
        ("Lidas / Nao lidas", lida, total, "Lidas", "Nao lidas"),
        ("Respondidos / Nao respondidos (Lidas)", respostas, lida, "Respondidos", "Nao respondidos"),
        ("Respondidos / Nao respondidos (Total)", respostas, total, "Respondidos", "Nao respondidos"),
    ]

    for idx, (titulo, positivo, base, label_pos, label_neg) in enumerate(configuracoes):
        labels, valores = _partes_pizza_duas_fatias(positivo, base, label_pos, label_neg)
        cores_fatias = []
        for label in labels:
            if label == "Sem dados":
                cores_fatias.append(cores["Sem dados"])
            elif label == label_pos:
                cores_fatias.append(cores["Positivo"])
            else:
                cores_fatias.append(cores["Negativo"])
        _, _, autotexts = axes[idx].pie(
            valores,
            startangle=90,
            colors=cores_fatias,
            autopct=_autopct_com_quantidade(sum(valores)),
            textprops={"fontsize": 9.5, "color": "white", "fontweight": "bold"},
        )
        for autotext in autotexts:
            autotext.set_color("white")
            autotext.set_weight("bold")
        axes[idx].set_title(titulo, fontsize=11, fontweight="bold")
        axes[idx].axis("equal")
        axes[idx].legend(labels, loc="lower center", bbox_to_anchor=(0.5, -0.25), ncol=2, fontsize=9)

    fig.tight_layout(rect=[0, 0.06, 1, 0.93])
    fig.savefig(arquivo_png_pizza, dpi=150)
    plt.close(fig)
    return str(arquivo_png_pizza)


def gerar_tabela_resumo_geral_internacao(
    arquivo_resumo_geral="src/data/analise_dados/internacao/resumo_internacao/RESUMO_GERAL_INTERNACAO.csv",
    arquivo_origem_internacao="src/data/internacao.xlsx",
    pasta_saida="src/data/analise_dados/imagens/internacao/resumo_internacao",
):
    pasta_saida = Path(pasta_saida)
    pasta_saida.mkdir(parents=True, exist_ok=True)
    arquivo_png = pasta_saida / "tabela_resumo_geral_internacao.png"
    arquivo_manifest = pasta_saida / "manifest_tabela_resumo_geral_internacao.json"

    manifest = {
        "etapa": "resumo_internacao_tabela",
        "contexto": "internacao",
        "gerado_em": datetime.now().isoformat(timespec="seconds"),
        "arquivo_origem_resumo_geral": str(arquivo_resumo_geral),
        "arquivo_origem_internacao": str(arquivo_origem_internacao),
        "arquivo_saida_imagem": str(arquivo_png),
        "arquivo_saida_grafico_pizza_funil_geral": "",
        "mensagens_validacao": [],
    }

    if plt is None:
        manifest["mensagens_validacao"].append(
            f"Tabela nao gerada: Matplotlib indisponivel no ambiente: {_ERRO_MATPLOTLIB}"
        )
        arquivo_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "ok": True,
            "arquivo_png": "",
            "arquivo_manifest": str(arquivo_manifest),
            "mensagens": manifest["mensagens_validacao"],
        }

    df_resumo_geral = ler_arquivo_csv(arquivo_resumo_geral)
    if df_resumo_geral.empty:
        raise ValueError("RESUMO_GERAL_INTERNACAO.csv vazio.")
    linha = df_resumo_geral.iloc[0]
    total = _normalizar_numero(linha.get("TOTAL", 0))
    respostas = _normalizar_numero(linha.get("RESPOSTAS", 0))
    nqa = _normalizar_numero(linha.get("NQA", 0))
    lida = _normalizar_numero(linha.get("LIDA", linha.get("LIDA_SEM_RESPOSTA", 0)))

    df_origem = ler_arquivo_csv(arquivo_origem_internacao)
    mes_titulo = _obter_mes_dt_internacao(df_origem)
    data_execucao = datetime.now().strftime("%d/%m/%y")

    titulo = f"Desempenho da pesquisa de Internacao {mes_titulo}"
    colunas = ["Categoria Metrica", "Ate o Dia", "Total", "Percentual"]
    linhas = [
        ["Lidas", data_execucao, f"{int(lida)}", _formatar_percentual(lida, total)],
        ["Respondidos / Lidas", data_execucao, f"{int(respostas)}", _formatar_percentual(respostas, lida)],
        ["Respondidos / Total", data_execucao, f"{int(respostas)}", _formatar_percentual(respostas, total)],
        ["NQA", data_execucao, f"{int(nqa)}", _formatar_percentual(nqa, total)],
        ["Total", data_execucao, f"{int(total)}", "-"],
    ]

    fig, ax = plt.subplots(figsize=(13.5, 5.2))
    ax.axis("off")
    ax.set_title(titulo, fontsize=15, fontweight="bold", pad=14)

    tabela = ax.table(
        cellText=linhas,
        colLabels=colunas,
        loc="upper center",
        cellLoc="center",
        colLoc="center",
        bbox=[0.05, 0.18, 0.9, 0.68],
    )
    tabela.auto_set_font_size(False)
    tabela.set_fontsize(11)
    tabela.scale(1, 1.45)

    for (row, col), cell in tabela.get_celld().items():
        cell.set_edgecolor("#222222")
        cell.set_linewidth(0.7)
        if row == 0:
            cell.set_facecolor("#E6E6E6")
            cell.set_text_props(weight="bold")
    for col in range(len(colunas)):
        tabela[(5, col)].set_text_props(weight="bold")

    fig.tight_layout()
    fig.savefig(arquivo_png, dpi=150)
    plt.close(fig)

    arquivo_png_pizza = _gerar_grafico_pizza_funil_geral_internacao(
        total=total,
        lida=lida,
        respostas=respostas,
        mes_titulo=mes_titulo,
        pasta_saida=pasta_saida,
    )
    manifest["arquivo_saida_grafico_pizza_funil_geral"] = arquivo_png_pizza

    manifest["mensagens_validacao"].append("Tabela Resumo - Geral Internacao gerada com sucesso.")
    manifest["mensagens_validacao"].append("Grafico pizza do funil (Geral) Internacao gerado com sucesso.")
    arquivo_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "ok": True,
        "arquivo_png": str(arquivo_png),
        "arquivo_png_pizza_funil_geral": arquivo_png_pizza,
        "arquivo_manifest": str(arquivo_manifest),
        "mensagens": manifest["mensagens_validacao"],
    }
