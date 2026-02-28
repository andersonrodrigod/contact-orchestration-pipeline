import argparse

from src.pipelines.complicacao_pipeline import (
    run_pipeline_complicacao_com_resposta,
    run_pipeline_complicacao_somente_status,
)
from src.pipelines.internacao_eletivo_pipeline import (
    run_pipeline_internacao_eletivo_com_resposta,
    run_pipeline_internacao_eletivo_somente_status,
)
from src.utils.resumo_execucao import imprimir_resumo_execucao


def run_pipeline():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--modo',
        choices=[
            'complicacao_com_resposta',
            'complicacao_somente_status',
            'internacao_eletivo_com_resposta',
            'internacao_eletivo_somente_status',
            'ambos_com_resposta',
            'ambos_somente_status',
            'complicacao',
            'internacao_eletivo',
            'ambos',
        ],
        default='ambos_com_resposta',
    )
    args = parser.parse_args()

    if args.modo in ['complicacao_com_resposta', 'complicacao']:
        return run_pipeline_complicacao_com_resposta()

    if args.modo == 'complicacao_somente_status':
        return run_pipeline_complicacao_somente_status()

    if args.modo in ['internacao_eletivo_com_resposta', 'internacao_eletivo']:
        return run_pipeline_internacao_eletivo_com_resposta()

    if args.modo == 'internacao_eletivo_somente_status':
        return run_pipeline_internacao_eletivo_somente_status()

    if args.modo in ['ambos_com_resposta', 'ambos']:
        resultado_complicacao = run_pipeline_complicacao_com_resposta()
        resultado_internacao_eletivo = run_pipeline_internacao_eletivo_com_resposta()
    else:
        resultado_complicacao = run_pipeline_complicacao_somente_status()
        resultado_internacao_eletivo = run_pipeline_internacao_eletivo_somente_status()

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
