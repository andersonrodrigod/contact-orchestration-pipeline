import argparse

from src.cli.modos_individuais import obter_modos_individuais
from src.cli.modos_principais import MODOS_AGREGADOS, MODOS_PRINCIPAIS, executar_modo_ambos
from src.utils.resumo_execucao import imprimir_resumo_execucao


ALLOW_MODOS_INDIVIDUAIS = False


def _obter_registro_modos():
    modos_individuais = obter_modos_individuais(
        permitir_execucao=ALLOW_MODOS_INDIVIDUAIS
    )
    modo_funcao = dict(MODOS_PRINCIPAIS)
    modo_funcao.update(modos_individuais)
    return modo_funcao, modos_individuais


def run_pipeline():
    modo_funcao, modos_individuais = _obter_registro_modos()
    escolhas_modo = list(MODOS_PRINCIPAIS.keys()) + MODOS_AGREGADOS + list(modos_individuais.keys())

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--modo',
        choices=escolhas_modo,
        default='ambos_com_resposta',
    )
    args = parser.parse_args()

    funcao_modo = modo_funcao.get(args.modo)
    if funcao_modo:
        return funcao_modo()

    return executar_modo_ambos(args.modo)


if __name__ == '__main__':
    resultado = run_pipeline()
    imprimir_resumo_execucao(resultado)
