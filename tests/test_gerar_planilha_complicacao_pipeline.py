import pandas as pd
from pathlib import Path
from openpyxl.utils import get_column_letter

from src.features.gerar_planilha_complicacao.config import COLUNAS_PRINCIPAIS
from src.features.gerar_planilha_complicacao.excel_formulas import FORMULAS_BASE
from src.features.gerar_planilha_complicacao.pipeline import executar_pipeline, validar_colunas_base


def test_validar_colunas_base_avisa_coluna_obrigatoria_faltante():
    df = pd.DataFrame({"PROCEDIMENTO": ["CONSULTA"]})

    resultado = validar_colunas_base(df)

    assert resultado["ok"] is False
    assert resultado["colunas_obrigatorias_faltantes"] == ["COD USUARIO"]
    assert "COD USUARIO" in resultado["mensagens"][0]


def test_validar_colunas_base_avisa_colunas_padrao_sem_bloquear():
    df = pd.DataFrame({"COD USUARIO": ["123"]})

    resultado = validar_colunas_base(df)

    assert resultado["ok"] is True
    assert "PROCEDIMENTO" in resultado["colunas_padrao_faltantes"]
    assert any("criadas vazias" in mensagem for mensagem in resultado["mensagens"])


def test_executar_pipeline_avisa_arquivo_base_ausente():
    base = Path("tests/outputs/gerar_planilha_complicacao_inexistente")
    arquivo_base = base / "nao_existe.xlsx"

    resultado = executar_pipeline(
        arquivo=arquivo_base,
        arquivo_telefones=base / "telefones.csv",
        arquivo_utilidade=None,
        arquivo_saida=base / "saida.xlsx",
    )

    assert resultado["ok"] is False
    assert "Arquivo base de complicacao nao encontrado" in resultado["mensagens"][0]


def test_ligacao_efetivada_fica_em_az_com_formula_p1():
    assert "DATA ULTIMA TENTATIVA" not in COLUNAS_PRINCIPAIS
    assert "DUPLICADO" in COLUNAS_PRINCIPAIS
    assert "DUPLICIDADE" not in COLUNAS_PRINCIPAIS
    indice_ligacao = COLUNAS_PRINCIPAIS.index("LIGACAO EFETIVADA") + 1
    assert get_column_letter(indice_ligacao) == "AZ"

    regra = next(
        regra for regra in FORMULAS_BASE if regra["coluna"] == "LIGACAO EFETIVADA"
    )
    assert regra["letra"] == "AZ"
    assert regra["formula"](2) == '=PROCX(AF2,\'P1\'!B:B,\'P1\'!C:C,"")'
