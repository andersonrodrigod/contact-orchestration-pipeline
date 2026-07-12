import sys
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.features.gerar_planilha_complicacao.pipeline import (
    ARQUIVO_ENTRADA_PADRAO,
    ARQUIVO_SAIDA_PADRAO,
    ARQUIVO_TELEFONES_PADRAO,
    ARQUIVO_UTILIDADE_PADRAO,
    executar_pipeline,
    imprimir_resumo,
)


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Gera a planilha base de complicacao sem abrir a UI."
    )
    parser.add_argument("--base", default=ARQUIVO_ENTRADA_PADRAO, help="Planilha base de complicacao.")
    parser.add_argument("--telefones", default=ARQUIVO_TELEFONES_PADRAO, help="CSV de telefones.")
    parser.add_argument("--utilidade", default=ARQUIVO_UTILIDADE_PADRAO, help="Planilha de utilidade.")
    parser.add_argument("--saida", default=ARQUIVO_SAIDA_PADRAO, help="Arquivo Excel de saida.")
    parser.add_argument(
        "--pasta-saida",
        default=None,
        help="Pasta de saida. Quando informada, gera complicacao.xlsx nela.",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    arquivo_saida = Path(args.saida)
    if args.pasta_saida:
        pasta_saida = Path(args.pasta_saida)
        arquivo_saida = pasta_saida / "complicacao.xlsx"

    resultado = executar_pipeline(
        arquivo=Path(args.base),
        arquivo_telefones=Path(args.telefones),
        arquivo_utilidade=Path(args.utilidade) if args.utilidade else None,
        arquivo_saida=arquivo_saida,
    )
    imprimir_resumo(resultado)


if __name__ == "__main__":
    main()
