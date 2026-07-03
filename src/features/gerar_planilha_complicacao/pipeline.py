from pathlib import Path

import pandas as pd

from src.features.gerar_planilha_complicacao.config import ABAS_AJUSTADAS, COLUNAS_PRINCIPAIS
from src.features.gerar_planilha_complicacao.duplicidades import remover_duplicados_por_regras, remover_parto_laqueadura
from src.features.gerar_planilha_complicacao.excel_formulas import aplicar_formulas
from src.features.gerar_planilha_complicacao.telefones import adicionar_telefones_por_senha
from src.features.gerar_planilha_complicacao.utilidades import enriquecer_com_utilidades, localizar_arquivo_utilidade
from src.features.gerar_planilha_complicacao.utils import (
    juntar_excluidos,
    normalizar_chave,
    normalizar_data_internacao,
    ordenar_por_data_internacao,
    sinalizar_duplicado_por_usuario_idade,
    sinalizar_duplicidade,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / "data" / "gerar_planilha_complicacao"
ENTRADA_DIR = DATA_DIR / "entrada"
SAIDA_DIR = DATA_DIR / "saida"
RELATORIOS_DIR = DATA_DIR / "relatorios"

ARQUIVO_ENTRADA_PADRAO = ENTRADA_DIR / "complicacao.xlsx"
ARQUIVO_TELEFONES_PADRAO = ENTRADA_DIR / "telefone_junho_internacaoes.csv"
ARQUIVO_UTILIDADE_PADRAO = localizar_arquivo_utilidade(ENTRADA_DIR)
ARQUIVO_SAIDA_PADRAO = SAIDA_DIR / "complicacao.xlsx"
ARQUIVO_EXCLUIDOS_PADRAO = RELATORIOS_DIR / "linhas_excluidas.xlsx"

COLUNAS_OBRIGATORIAS_BASE = [
    "COD USUARIO",
]


def validar_colunas_base(df):
    colunas = set(df.columns)
    colunas_obrigatorias_faltantes = [
        coluna for coluna in COLUNAS_OBRIGATORIAS_BASE if coluna not in colunas
    ]
    colunas_padrao_faltantes = [
        coluna for coluna in COLUNAS_PRINCIPAIS if coluna not in colunas
    ]
    mensagens = []

    if colunas_obrigatorias_faltantes:
        mensagens.append(
            "A planilha base nao tem as colunas obrigatorias: "
            + ", ".join(colunas_obrigatorias_faltantes)
            + "."
        )

    colunas_padrao_apenas_aviso = [
        coluna
        for coluna in colunas_padrao_faltantes
        if coluna not in set(colunas_obrigatorias_faltantes)
    ]
    if colunas_padrao_apenas_aviso:
        mensagens.append(
            "A planilha base nao tem algumas colunas padrao. "
            "Elas serao criadas vazias: "
            + ", ".join(colunas_padrao_apenas_aviso)
            + "."
        )

    return {
        "ok": not colunas_obrigatorias_faltantes,
        "mensagens": mensagens,
        "colunas_obrigatorias_faltantes": colunas_obrigatorias_faltantes,
        "colunas_padrao_faltantes": colunas_padrao_faltantes,
    }


def _resultado_falha_validacao_colunas(validacao):
    return {
        "ok": False,
        "mensagens": validacao["mensagens"],
        "colunas_obrigatorias_faltantes": validacao["colunas_obrigatorias_faltantes"],
        "colunas_padrao_faltantes": validacao["colunas_padrao_faltantes"],
    }


def _resultado_falha(mensagem):
    return {
        "ok": False,
        "mensagens": [mensagem],
    }


def executar_pipeline(
    arquivo=ARQUIVO_ENTRADA_PADRAO,
    arquivo_telefones=ARQUIVO_TELEFONES_PADRAO,
    arquivo_utilidade=ARQUIVO_UTILIDADE_PADRAO,
    arquivo_saida=ARQUIVO_SAIDA_PADRAO,
    arquivo_excluidos=ARQUIVO_EXCLUIDOS_PADRAO,
):
    arquivo = Path(arquivo)
    arquivo_telefones = Path(arquivo_telefones)
    arquivo_saida = Path(arquivo_saida)
    arquivo_excluidos = Path(arquivo_excluidos)
    arquivo_utilidade = Path(arquivo_utilidade) if arquivo_utilidade is not None else None

    if not arquivo.exists():
        return _resultado_falha(f"Arquivo base de complicacao nao encontrado: {arquivo}")

    df = pd.read_excel(arquivo, sheet_name=0, dtype={"COD USUARIO": str})
    validacao_colunas = validar_colunas_base(df)
    if not validacao_colunas["ok"]:
        return _resultado_falha_validacao_colunas(validacao_colunas)

    df["COD USUARIO"] = df["COD USUARIO"].apply(normalizar_chave)

    for coluna in COLUNAS_PRINCIPAIS:
        if coluna not in df.columns:
            df[coluna] = ""

    df, resumo_utilidades = enriquecer_com_utilidades(df, arquivo_utilidade)

    df["IDADE T"] = (
        df["IDADE"].astype("string").str.extract(r"^\s*([^Aa]*)[Aa]", expand=False).str.strip().fillna("")
    )
    df = normalizar_data_internacao(df)

    df, df_excluidos_parto_laqueadura = remover_parto_laqueadura(df)

    colunas_extras = [coluna for coluna in df.columns if coluna not in COLUNAS_PRINCIPAIS]
    df = df[COLUNAS_PRINCIPAIS + colunas_extras]

    df, df_excluidos_duplicados = remover_duplicados_por_regras(df)
    df, resumo_telefones = adicionar_telefones_por_senha(df, arquivo_telefones)
    df = ordenar_por_data_internacao(df)
    df = sinalizar_duplicado_por_usuario_idade(df)
    df = sinalizar_duplicidade(df)
    df_excluidos = juntar_excluidos(
        [df_excluidos_parto_laqueadura, df_excluidos_duplicados],
        df.columns,
    )

    arquivo_saida.parent.mkdir(parents=True, exist_ok=True)
    arquivo_excluidos.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(arquivo_saida, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="BASE", index=False)

        for nome_aba, colunas in ABAS_AJUSTADAS.items():
            df_aba = pd.DataFrame(columns=colunas)
            df_aba.to_excel(writer, sheet_name=nome_aba, index=False)

    aplicar_formulas(arquivo_saida)
    df_excluidos.to_excel(arquivo_excluidos, index=False)

    return {
        "arquivo_saida": arquivo_saida,
        "arquivo_excluidos": arquivo_excluidos,
        "linhas_excluidas_parto_laqueadura": len(df_excluidos_parto_laqueadura),
        "linhas_excluidas_duplicados": len(df_excluidos_duplicados),
        "total_linhas_excluidas": len(df_excluidos),
        "resumo_utilidades": resumo_utilidades,
        "resumo_telefones": resumo_telefones,
        "mensagens": validacao_colunas["mensagens"],
        "colunas_padrao_faltantes": validacao_colunas["colunas_padrao_faltantes"],
        "ok": True,
    }


def imprimir_resumo(resultado):
    if not resultado.get("ok", True):
        print("Nao foi possivel gerar a planilha de complicacao.")
        for mensagem in resultado.get("mensagens", []):
            print(mensagem)
        return

    print(f"Planilha organizada com sucesso em: {resultado['arquivo_saida']}")
    for mensagem in resultado.get("mensagens", []):
        print(mensagem)
    print(f"Linhas removidas com PARTO ou LAQUEADURA: {resultado['linhas_excluidas_parto_laqueadura']}")
    print(f"Linhas removidas por duplicidade: {resultado['linhas_excluidas_duplicados']}")
    print(f"Total de linhas removidas: {resultado['total_linhas_excluidas']}")
    print(f"Arquivo unico com linhas removidas: {resultado['arquivo_excluidos']}")

    resumo_utilidades = resultado["resumo_utilidades"]
    if resumo_utilidades["executado"]:
        print(f"Arquivo de utilidade usado: {resumo_utilidades['arquivo']}")
        for coluna, quantidade in resumo_utilidades["preenchidos"].items():
            print(f"Utilidade {coluna}: {quantidade} linhas com correspondencia")
    else:
        print(resumo_utilidades["motivo"])

    resumo_telefones = resultado["resumo_telefones"]
    if resumo_telefones["executado"]:
        print(f"Total de SENHAS encontradas com telefone: {resumo_telefones['senhas_com_telefone']}")
        print(f"Match por SENHA: {resumo_telefones['match_senha']}")
        print(f"Match por COD USUARIO: {resumo_telefones['match_cod_usuario']}")
        print(f"Match encontrado, mas sem telefone: {resumo_telefones['match_sem_telefone']}")
    else:
        print(resumo_telefones["motivo"])


def main():
    resultado = executar_pipeline()
    imprimir_resumo(resultado)


if __name__ == "__main__":
    main()

