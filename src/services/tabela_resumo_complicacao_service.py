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


def _obter_mes_dt_internacao(df_origem):
    col_dt_int = _resolver_coluna(df_origem, "DT INTERNACAO")
    if not col_dt_int:
        return "Sem mes"
    dt_int = _serie_data(df_origem, col_dt_int).dropna()
    if dt_int.empty:
        return "Sem mes"
    mes_mais_frequente = int(dt_int.dt.month.mode().iloc[0])
    return _MESES_PT.get(mes_mais_frequente, "Sem mes")


def _obter_ate_o_dia(df_origem):
    col_dt_envio = _resolver_coluna(df_origem, "DT ENVIO")
    if not col_dt_envio:
        return ""
    dt_envio = _serie_data(df_origem, col_dt_envio)
    hoje = pd.Timestamp.now().normalize()
    dt_validas = dt_envio[(dt_envio.notna()) & (dt_envio <= hoje)]
    if dt_validas.empty:
        return ""
    data_ref = dt_validas.max() - pd.DateOffset(months=1)
    return data_ref.strftime("%d/%m/%y")


def _gerar_tabela_video_nova_pergunta(
    df_resumo_dia,
    df_resumo_geral,
    mes_titulo,
    pasta_saida,
):
    arquivo_png_video = pasta_saida / "tabela_resumo_video_nova_pergunta_complicacao.png"

    linha_dia = df_resumo_dia.iloc[0]
    linha_geral = df_resumo_geral.iloc[0]

    total_video_geral = _normalizar_numero(linha_geral.get("TOTAL_VIDEO_SIM", 0))
    respondidos_video_geral = _normalizar_numero(linha_geral.get("TOTAL_VIDEO_RESPONDIDO_SIM", 0))
    nqa_video_geral = _normalizar_numero(linha_geral.get("TOTAL_VIDEO_SIM_NQA", 0))
    sem_contato_video_geral = _normalizar_numero(linha_geral.get("TOTAL_VIDEO_SEM_CONTATO", 0))

    total_video_dia = _normalizar_numero(linha_dia.get("TOTAL_VIDEO_SIM", 0))
    respondidos_video_dia = _normalizar_numero(linha_dia.get("TOTAL_VIDEO_RESPONDIDO_SIM", 0))

    ate_hoje = datetime.now().strftime("%d/%m")
    titulo_video = (
        "Desempenho da pesquisa de Complicacoes Cirurgicas "
        f"{mes_titulo} (NOVA PERGUNTA)"
    )
    colunas_video = ["", "Quantidade", "Respondidos", "Percentual"]
    linhas_video = [
        [
            "TOTAL GERAL",
            f"{int(total_video_geral)}",
            f"{int(respondidos_video_geral)}",
            _formatar_percentual(respondidos_video_geral, total_video_geral),
        ],
        [
            f"Respondidos - Ate {ate_hoje}",
            f"{int(total_video_dia)}",
            f"{int(respondidos_video_dia)}",
            _formatar_percentual(respondidos_video_dia, total_video_dia),
        ],
        [
            "NQA",
            f"{int(nqa_video_geral)}",
            "-",
            _formatar_percentual(nqa_video_geral, total_video_geral),
        ],
        [
            "Sem contato ate o momento",
            f"{int(sem_contato_video_geral)}",
            "-",
            _formatar_percentual(sem_contato_video_geral, total_video_geral),
        ],
    ]

    fig, ax = plt.subplots(figsize=(13.5, 5.2))
    ax.axis("off")
    ax.set_title(titulo_video, fontsize=15, fontweight="bold", pad=14)

    tabela = ax.table(
        cellText=linhas_video,
        colLabels=colunas_video,
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
        elif col == 0:
            cell.set_text_props(weight="bold")

    fig.tight_layout()
    fig.savefig(arquivo_png_video, dpi=150)
    plt.close(fig)

    return str(arquivo_png_video)


def _autopct_com_quantidade(total):
    def _formatar(pct):
        valor = int(round((pct / 100.0) * total))
        return f"{valor}\n({pct:.1f}%)"

    return _formatar


def _montar_partes_pizza(total, respondidos, nqa, sem_contato):
    partes = [
        ("Respondidos", respondidos),
        ("NQA", nqa),
        ("Sem contato", sem_contato),
    ]
    restante = max(0.0, total - sum(v for _, v in partes))
    if restante > 0:
        partes.append(("Outros", restante))
    labels = [nome for nome, valor in partes if valor > 0]
    valores = [valor for _, valor in partes if valor > 0]
    if not valores:
        labels = ["Sem dados"]
        valores = [1.0]
    return labels, valores


def _gerar_grafico_pizza_video(
    df_resumo_dia,
    df_resumo_geral,
    mes_titulo,
    pasta_saida,
):
    arquivo_png_pizza = pasta_saida / "grafico_pizza_video_complicacao.png"
    linha_dia = df_resumo_dia.iloc[0]
    linha_geral = df_resumo_geral.iloc[0]

    total_dia = _normalizar_numero(linha_dia.get("TOTAL_VIDEO_SIM", 0))
    respondidos_dia = _normalizar_numero(linha_dia.get("TOTAL_VIDEO_RESPONDIDO_SIM", 0))
    nqa_dia = _normalizar_numero(linha_dia.get("TOTAL_VIDEO_SIM_NQA", 0))
    sem_contato_dia = _normalizar_numero(linha_dia.get("TOTAL_VIDEO_SEM_CONTATO", 0))

    total_geral = _normalizar_numero(linha_geral.get("TOTAL_VIDEO_SIM", 0))
    respondidos_geral = _normalizar_numero(linha_geral.get("TOTAL_VIDEO_RESPONDIDO_SIM", 0))
    nqa_geral = _normalizar_numero(linha_geral.get("TOTAL_VIDEO_SIM_NQA", 0))
    sem_contato_geral = _normalizar_numero(linha_geral.get("TOTAL_VIDEO_SEM_CONTATO", 0))

    labels_dia, valores_dia = _montar_partes_pizza(
        total=total_dia,
        respondidos=respondidos_dia,
        nqa=nqa_dia,
        sem_contato=sem_contato_dia,
    )
    labels_geral, valores_geral = _montar_partes_pizza(
        total=total_geral,
        respondidos=respondidos_geral,
        nqa=nqa_geral,
        sem_contato=sem_contato_geral,
    )

    cores = {
        "Respondidos": "#2652B5",
        "NQA": "#F5BA2B",
        "Sem contato": "#E64A36",
        "Outros": "#546E7A",
        "Sem dados": "#B0BEC5",
    }

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle(
        f"Video Abdominal - Complicacoes Cirurgicas {mes_titulo}",
        fontsize=15,
        fontweight="bold",
        y=0.98,
    )

    cores_dia = [cores.get(label, "#607D8B") for label in labels_dia]
    _, _, autotexts_dia = axes[0].pie(
        valores_dia,
        startangle=90,
        colors=cores_dia,
        autopct=_autopct_com_quantidade(sum(valores_dia)),
        textprops={"fontsize": 10, "color": "white", "fontweight": "bold"},
    )
    for autotext in autotexts_dia:
        autotext.set_color("white")
        autotext.set_weight("bold")
    axes[0].set_title("Diario (Ate o dia)", fontsize=12, fontweight="bold")
    axes[0].axis("equal")

    cores_geral = [cores.get(label, "#607D8B") for label in labels_geral]
    _, _, autotexts_geral = axes[1].pie(
        valores_geral,
        startangle=90,
        colors=cores_geral,
        autopct=_autopct_com_quantidade(sum(valores_geral)),
        textprops={"fontsize": 10, "color": "white", "fontweight": "bold"},
    )
    for autotext in autotexts_geral:
        autotext.set_color("white")
        autotext.set_weight("bold")
    axes[1].set_title("Geral (Total)", fontsize=12, fontweight="bold")
    axes[1].axis("equal")

    ordem_legenda = ["Respondidos", "NQA", "Sem contato", "Outros", "Sem dados"]
    labels_legenda = [label for label in ordem_legenda if (label in labels_dia or label in labels_geral)]
    handles = [
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=cores[label], markersize=10)
        for label in labels_legenda
    ]
    fig.legend(handles, labels_legenda, loc="lower center", ncol=max(1, len(labels_legenda)))

    fig.tight_layout(rect=[0, 0.08, 1, 0.94])
    fig.savefig(arquivo_png_pizza, dpi=150)
    plt.close(fig)
    return str(arquivo_png_pizza)


def _partes_pizza_duas_fatias(positivo, total_base, label_positivo, label_negativo):
    positivo = max(0.0, float(positivo))
    total_base = max(0.0, float(total_base))
    negativo = max(0.0, total_base - positivo)
    if total_base <= 0:
        return ["Sem dados"], [1.0]
    if positivo <= 0 and negativo <= 0:
        return ["Sem dados"], [1.0]
    return [label_positivo, label_negativo], [positivo, negativo]


def _gerar_grafico_pizza_funil(
    total,
    lida,
    respostas,
    mes_titulo,
    subtitulo,
    arquivo_saida,
):
    cores = {
        "Positivo": "#2652B5",
        "Negativo": "#E64A36",
        "Sem dados": "#B0BEC5",
    }
    fig, axes = plt.subplots(1, 3, figsize=(16, 5.8))
    fig.suptitle(
        f"Complicacoes Cirurgicas {mes_titulo} - {subtitulo}",
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
    fig.savefig(arquivo_saida, dpi=150)
    plt.close(fig)
    return str(arquivo_saida)


def gerar_tabela_resumo_dia_complicacao(
    arquivo_resumo_dia="src/data/analise_dados/complicacao/resumo_complicacao/RESUMO_DIA_COMPLICACAO.csv",
    arquivo_resumo_geral="src/data/analise_dados/complicacao/resumo_complicacao/RESUMO_GERAL_COMPLICACAO.csv",
    arquivo_origem_complicacao="src/data/complicacao.xlsx",
    pasta_saida="src/data/analise_dados/imagens/complicacao/resumo_complicacao",
):
    pasta_saida = Path(pasta_saida)
    pasta_saida.mkdir(parents=True, exist_ok=True)
    arquivo_png = pasta_saida / "tabela_resumo_dia_complicacao.png"
    arquivo_manifest = pasta_saida / "manifest_tabela_resumo_dia_complicacao.json"

    manifest = {
        "etapa": "resumo_complicacao_tabela",
        "contexto": "complicacao",
        "gerado_em": datetime.now().isoformat(timespec="seconds"),
        "arquivo_origem_resumo_dia": str(arquivo_resumo_dia),
        "arquivo_origem_resumo_geral": str(arquivo_resumo_geral),
        "arquivo_origem_complicacao": str(arquivo_origem_complicacao),
        "arquivo_saida_imagem": str(arquivo_png),
        "arquivo_saida_imagem_video_nova_pergunta": "",
        "arquivo_saida_grafico_pizza_video": "",
        "arquivo_saida_grafico_pizza_funil_dia": "",
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

    df_resumo = ler_arquivo_csv(arquivo_resumo_dia)
    if df_resumo.empty:
        raise ValueError("RESUMO_DIA_COMPLICACAO.csv vazio.")
    df_resumo_geral = ler_arquivo_csv(arquivo_resumo_geral)
    if df_resumo_geral.empty:
        raise ValueError("RESUMO_GERAL_COMPLICACAO.csv vazio.")

    linha = df_resumo.iloc[0]
    total = _normalizar_numero(linha.get("TOTAL", 0))
    respostas = _normalizar_numero(linha.get("RESPOSTAS", 0))
    lida = _normalizar_numero(linha.get("LIDA", 0))
    linha_geral = df_resumo_geral.iloc[0]
    total_geral = _normalizar_numero(linha_geral.get("TOTAL", 0))
    respostas_geral = _normalizar_numero(linha_geral.get("RESPOSTAS", 0))
    lida_geral = _normalizar_numero(linha_geral.get("LIDA", 0))

    df_origem = ler_arquivo_csv(arquivo_origem_complicacao)
    mes_titulo = _obter_mes_dt_internacao(df_origem)
    ate_o_dia = _obter_ate_o_dia(df_origem)
    ate_o_dia_geral = datetime.now().strftime("%d/%m/%y")

    titulo = f"Desempenho da pesquisa de Complicacoes Cirurgicas {mes_titulo}"
    colunas = ["Categoria Metrica", "Ate o Dia", "Total", "Percentual"]
    linhas = [
        ["Resumo - Dia de Internacao", "", "", ""],
        ["Lidas", ate_o_dia, f"{int(lida)}", _formatar_percentual(lida, total)],
        ["Respondidos / Lidas", ate_o_dia, f"{int(respostas)}", _formatar_percentual(respostas, lida)],
        ["Respondidos / Total", ate_o_dia, f"{int(respostas)}", _formatar_percentual(respostas, total)],
        ["Total", ate_o_dia, f"{int(total)}", "-"],
        ["", "", "", ""],
        ["Resumo - Geral", "", "", ""],
        ["Lidas", ate_o_dia_geral, f"{int(lida_geral)}", _formatar_percentual(lida_geral, total_geral)],
        [
            "Respondidos / Lidas",
            ate_o_dia_geral,
            f"{int(respostas_geral)}",
            _formatar_percentual(respostas_geral, lida_geral),
        ],
        [
            "Respondidos / Total",
            ate_o_dia_geral,
            f"{int(respostas_geral)}",
            _formatar_percentual(respostas_geral, total_geral),
        ],
        ["Total", ate_o_dia_geral, f"{int(total_geral)}", "-"],
    ]

    fig, ax = plt.subplots(figsize=(13.5, 7.2))
    ax.axis("off")
    ax.set_title(titulo, fontsize=15, fontweight="bold", pad=14)

    tabela = ax.table(
        cellText=linhas,
        colLabels=colunas,
        loc="upper center",
        cellLoc="center",
        colLoc="center",
        bbox=[0.05, 0.08, 0.9, 0.78],
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

    # Linha de secao "Resumo - Dia de Internacao".
    for col in range(len(colunas)):
        cell = tabela[(1, col)]
        cell.set_facecolor("#F1F1F1")
        if col == 0:
            cell.set_text_props(weight="bold")
        else:
            cell.get_text().set_text("")

    # Linha em branco separadora.
    for col in range(len(colunas)):
        cell = tabela[(6, col)]
        cell.set_facecolor("#FFFFFF")
        cell.get_text().set_text("")

    # Linha de secao "Resumo - Geral".
    for col in range(len(colunas)):
        cell = tabela[(7, col)]
        cell.set_facecolor("#F1F1F1")
        if col == 0:
            cell.set_text_props(weight="bold")
        else:
            cell.get_text().set_text("")

    # Linhas "Total" em negrito.
    for col in range(len(colunas)):
        tabela[(5, col)].set_text_props(weight="bold")
        tabela[(11, col)].set_text_props(weight="bold")

    fig.tight_layout()
    fig.savefig(arquivo_png, dpi=150)
    plt.close(fig)

    arquivo_png_video = _gerar_tabela_video_nova_pergunta(
        df_resumo_dia=df_resumo,
        df_resumo_geral=df_resumo_geral,
        mes_titulo=mes_titulo,
        pasta_saida=pasta_saida,
    )
    manifest["arquivo_saida_imagem_video_nova_pergunta"] = arquivo_png_video
    arquivo_png_pizza_video = _gerar_grafico_pizza_video(
        df_resumo_dia=df_resumo,
        df_resumo_geral=df_resumo_geral,
        mes_titulo=mes_titulo,
        pasta_saida=pasta_saida,
    )
    manifest["arquivo_saida_grafico_pizza_video"] = arquivo_png_pizza_video
    arquivo_png_pizza_funil_dia = _gerar_grafico_pizza_funil(
        total=total,
        lida=lida,
        respostas=respostas,
        mes_titulo=mes_titulo,
        subtitulo="Resumo Dia",
        arquivo_saida=pasta_saida / "grafico_pizza_funil_dia_complicacao.png",
    )
    manifest["arquivo_saida_grafico_pizza_funil_dia"] = arquivo_png_pizza_funil_dia
    arquivo_png_pizza_funil_geral = _gerar_grafico_pizza_funil(
        total=total_geral,
        lida=lida_geral,
        respostas=respostas_geral,
        mes_titulo=mes_titulo,
        subtitulo="Resumo Geral",
        arquivo_saida=pasta_saida / "grafico_pizza_funil_geral_complicacao.png",
    )
    manifest["arquivo_saida_grafico_pizza_funil_geral"] = arquivo_png_pizza_funil_geral

    manifest["mensagens_validacao"].append("Tabela Resumo - Dia e Resumo - Geral gerada com sucesso.")
    manifest["mensagens_validacao"].append("Tabela Resumo - Video (Nova Pergunta) gerada com sucesso.")
    manifest["mensagens_validacao"].append("Grafico pizza de video (Diario x Geral) gerado com sucesso.")
    manifest["mensagens_validacao"].append("Graficos pizza do funil (Dia e Geral) gerados com sucesso.")
    arquivo_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "ok": True,
        "arquivo_png": str(arquivo_png),
        "arquivo_png_video_nova_pergunta": arquivo_png_video,
        "arquivo_png_pizza_video": arquivo_png_pizza_video,
        "arquivo_png_pizza_funil_dia": arquivo_png_pizza_funil_dia,
        "arquivo_png_pizza_funil_geral": arquivo_png_pizza_funil_geral,
        "arquivo_manifest": str(arquivo_manifest),
        "mensagens": manifest["mensagens_validacao"],
    }
