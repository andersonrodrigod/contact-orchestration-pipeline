from pathlib import Path

from src.ui.controllers.gerar_planilha_complicacao_controller import (
    GerarPlanilhaComplicacaoController,
)


def test_controller_exige_arquivos_obrigatorios():
    controller = GerarPlanilhaComplicacaoController()

    _, erro = controller.resolve_execution_request(
        file_values={
            "arquivo_base": "",
            "arquivo_telefones": "",
            "arquivo_utilidade": "",
            "output_dir": "",
        },
        file_labels={
            "arquivo_base": "Planilha complicacao base",
            "arquivo_telefones": "Telefones CSV",
            "arquivo_utilidade": "Utilidade",
            "output_dir": "Pasta de saida",
        },
    )

    assert erro is not None
    assert "Arquivos obrigatorios ausentes" in erro


def test_controller_monta_plano_com_caminhos_de_saida():
    base = Path("tests/outputs/ui_gerar_planilha_complicacao_controller")
    base.mkdir(parents=True, exist_ok=True)
    arquivo_base = base / "base.xlsx"
    arquivo_telefones = base / "telefones.csv"
    arquivo_utilidade = base / "utilidade.xlsx"
    for caminho in (arquivo_base, arquivo_telefones, arquivo_utilidade):
        caminho.touch()

    controller = GerarPlanilhaComplicacaoController()
    plano, erro = controller.resolve_execution_request(
        file_values={
            "arquivo_base": str(arquivo_base),
            "arquivo_telefones": str(arquivo_telefones),
            "arquivo_utilidade": str(arquivo_utilidade),
            "output_dir": str(base),
        },
        file_labels={
            "arquivo_base": "Planilha complicacao base",
            "arquivo_telefones": "Telefones CSV",
            "arquivo_utilidade": "Utilidade",
            "output_dir": "Pasta de saida",
        },
    )

    assert erro is None
    assert plano["arquivo_saida"] == base / "complicacao.xlsx"
    assert plano["arquivo_excluidos"] == base / "linhas_excluidas.xlsx"
