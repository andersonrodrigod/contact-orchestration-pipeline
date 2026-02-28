import argparse

from src.pipelines.complicacao_pipeline import run_pipeline_complicacao
from src.pipelines.internacao_eletivo_pipeline import run_pipeline_internacao_eletivo
from src.utils.resumo_execucao import imprimir_resumo_execucao


def run_pipeline():
    parser = argparse.ArgumentParser()
    parser.add_argument('--modo', choices=['complicacao', 'internacao_eletivo', 'ambos'], default='ambos')
    args = parser.parse_args()

    if args.modo == 'complicacao':
        return run_pipeline_complicacao()

    if args.modo == 'internacao_eletivo':
        return run_pipeline_internacao_eletivo()

    resultado_complicacao = run_pipeline_complicacao()
    resultado_internacao_eletivo = run_pipeline_internacao_eletivo()
    ok_geral = resultado_complicacao.get('ok', False) and resultado_internacao_eletivo.get('ok', False)
    return {
        'ok': ok_geral,
        'resultados': {
            'complicacao': resultado_complicacao,
            'internacao_eletivo': resultado_internacao_eletivo,
        },
        'mensagens': ['Execucao em modo ambos finalizada.'],
    }


if __name__ == '__main__':
    resultado = run_pipeline()
    imprimir_resumo_execucao(resultado)
