import argparse

from core.error_codes import anexar_codigo_erro
from src.cli.modos_individuais import obter_modos_individuais
from src.cli.modos_principais import MODOS_AGREGADOS, MODOS_PRINCIPAIS, executar_modo_ambos
from src.services.observabilidade_service import registrar_historico_execucao
from src.utils.resumo_execucao import imprimir_resumo_execucao


ALLOW_MODOS_INDIVIDUAIS = True


def _obter_registro_modos():
    modos_individuais = obter_modos_individuais(
        permitir_execucao=ALLOW_MODOS_INDIVIDUAIS
    )
    modo_funcao = dict(MODOS_PRINCIPAIS)
    modo_funcao.update(modos_individuais)
    return modo_funcao, modos_individuais


def run_pipeline(modo='ambos_com_resposta'):
    modo_funcao, _ = _obter_registro_modos()
    funcao_modo = modo_funcao.get(modo)
    if funcao_modo:
        return funcao_modo()
    return executar_modo_ambos(modo)


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
    modo_funcao, modos_individuais = _obter_registro_modos()
    escolhas_modo = list(MODOS_PRINCIPAIS.keys()) + MODOS_AGREGADOS + list(modos_individuais.keys())
    parser = argparse.ArgumentParser()
    parser.add_argument('--modo', choices=escolhas_modo, default='ambos_com_resposta')
    args = parser.parse_args()
    resultado = run_pipeline(args.modo)

    resultado = _anexar_codigo_erro_recursivo(resultado)
    registrar_historico_execucao(resultado, modo=args.modo)
    imprimir_resumo_execucao(resultado)
