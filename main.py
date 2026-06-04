import argparse

from core.error_codes import anexar_codigo_erro
from src.cli.modos_principais import (
    MODOS_PRINCIPAIS,
    obter_escolhas_modo,
    obter_registro_modos as obter_registro_modos_cli,
)
from src.services.observabilidade_service import registrar_historico_execucao
from src.utils.resumo_execucao import imprimir_resumo_execucao


def _obter_registro_modos():
    return obter_registro_modos_cli()


def run_pipeline(modo='complicacao_com_resposta'):
    modo_funcao = _obter_registro_modos()
    funcao_modo = modo_funcao.get(modo)
    if funcao_modo:
        return funcao_modo()
    return MODOS_PRINCIPAIS['complicacao_com_resposta']()


def _anexar_codigo_erro_recursivo(resultado):
    if not isinstance(resultado, dict):
        return resultado
    anexar_codigo_erro(resultado)
    filhos = resultado.get('resultados')
    if isinstance(filhos, dict):
        for res in filhos.values():
            if isinstance(res, dict):
                anexar_codigo_erro(res)
    return resultado


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--modo', choices=obter_escolhas_modo(), default='complicacao_com_resposta')
    args = parser.parse_args()
    resultado = run_pipeline(args.modo)

    resultado = _anexar_codigo_erro_recursivo(resultado)
    registrar_historico_execucao(resultado, modo=args.modo)
    imprimir_resumo_execucao(resultado)
