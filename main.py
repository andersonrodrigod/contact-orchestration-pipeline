import argparse
from core.pipeline_result import PipelineResult

from src.pipelines.complicacao_pipeline import (
    run_complicacao_pipeline_finalizar,
    run_complicacao_pipeline_gerar_status_dataset,
    run_pipeline_complicacao_com_resposta,
    run_pipeline_complicacao_somente_status,
)
from src.pipelines.internacao_eletivo_pipeline import (
    run_internacao_eletivo_pipeline_finalizar,
    run_internacao_eletivo_pipeline_gerar_status_dataset,
    run_pipeline_internacao_eletivo_com_resposta,
    run_pipeline_internacao_eletivo_somente_status,
)
from src.utils.resumo_execucao import imprimir_resumo_execucao


MODO_FUNCAO = {
    'complicacao_com_resposta': run_pipeline_complicacao_com_resposta,
    'complicacao': run_pipeline_complicacao_com_resposta,
    'complicacao_somente_status': run_pipeline_complicacao_somente_status,
    'complicacao_gerar_status_dataset': run_complicacao_pipeline_gerar_status_dataset,
    'complicacao_finalizar_status': run_complicacao_pipeline_finalizar,
    'internacao_eletivo_com_resposta': run_pipeline_internacao_eletivo_com_resposta,
    'internacao_eletivo': run_pipeline_internacao_eletivo_com_resposta,
    'internacao_eletivo_somente_status': run_pipeline_internacao_eletivo_somente_status,
    'internacao_eletivo_gerar_status_dataset': run_internacao_eletivo_pipeline_gerar_status_dataset,
    'internacao_eletivo_finalizar_status': run_internacao_eletivo_pipeline_finalizar,
}


def run_pipeline():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--modo',
        choices=[
            'complicacao_com_resposta',
            'complicacao_somente_status',
            'complicacao_gerar_status_dataset',
            'complicacao_finalizar_status',
            'internacao_eletivo_com_resposta',
            'internacao_eletivo_somente_status',
            'internacao_eletivo_gerar_status_dataset',
            'internacao_eletivo_finalizar_status',
            'ambos_com_resposta',
            'ambos_somente_status',
            'complicacao',
            'internacao_eletivo',
            'ambos',
        ],
        default='ambos_com_resposta',
    )
    args = parser.parse_args()

    funcao_modo = MODO_FUNCAO.get(args.modo)
    if funcao_modo:
        return funcao_modo()

    if args.modo in ['ambos_com_resposta', 'ambos']:
        resultado_complicacao = run_pipeline_complicacao_com_resposta()
        resultado_internacao_eletivo = run_pipeline_internacao_eletivo_com_resposta()
    else:
        resultado_complicacao = run_pipeline_complicacao_somente_status()
        resultado_internacao_eletivo = run_pipeline_internacao_eletivo_somente_status()

    ok_geral = resultado_complicacao.get('ok', False) and resultado_internacao_eletivo.get('ok', False)
    return PipelineResult(
        ok=ok_geral,
        mensagens=['Execucao em modo ambos finalizada.'],
        dados={
            'resultados': {
                'complicacao': resultado_complicacao,
                'internacao_eletivo': resultado_internacao_eletivo,
            },
        },
    ).to_dict()


if __name__ == '__main__':
    resultado = run_pipeline()
    imprimir_resumo_execucao(resultado)
