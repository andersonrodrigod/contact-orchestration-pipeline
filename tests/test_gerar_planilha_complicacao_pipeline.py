import pandas as pd
from pathlib import Path

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
        arquivo_excluidos=base / "excluidos.xlsx",
    )

    assert resultado["ok"] is False
    assert "Arquivo base de complicacao nao encontrado" in resultado["mensagens"][0]
