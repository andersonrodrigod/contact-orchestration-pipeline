import argparse
from core.pipeline_result import PipelineResult

from src.config.paths import DEFAULTS_COMPLICACAO, DEFAULTS_INTERNACAO_ELETIVO
from src.pipelines.concatenar_status_respostas_pipeline import run_unificar_status_respostas_pipeline
from src.pipelines.complicacao_pipeline import (
    run_complicacao_pipeline_finalizar,
    run_complicacao_pipeline_gerar_status_dataset,
    run_pipeline_complicacao_com_resposta,
    run_pipeline_complicacao_somente_status,
)
from src.pipelines.criacao_dataset_pipeline import run_criacao_dataset_pipeline
from src.pipelines.finalizacao_pipeline import run_finalizacao_pipeline
from src.pipelines.ingestao_pipeline import (
    run_ingestao_complicacao,
    run_ingestao_somente_status,
    run_ingestao_unificar,
)
from src.pipelines.internacao_eletivo_pipeline import (
    run_internacao_eletivo_pipeline_finalizar,
    run_internacao_eletivo_pipeline_gerar_status_dataset,
    run_pipeline_internacao_eletivo_com_resposta,
    run_pipeline_internacao_eletivo_somente_status,
)
from src.pipelines.join_status_resposta_pipeline import (
    run_status_somente_complicacao_pipeline,
    run_status_somente_internacao_eletivo_pipeline,
    run_unificar_status_resposta_complicacao_pipeline,
    run_unificar_status_resposta_internacao_eletivo_pipeline,
)
from src.utils.resumo_execucao import imprimir_resumo_execucao


ALLOW_MODOS_INDIVIDUAIS = False


def _modo_individual_bloqueado(nome_modo):
    return {
        'ok': False,
        'mensagens': [
            f'Modo individual "{nome_modo}" desabilitado. '
            'Defina ALLOW_MODOS_INDIVIDUAIS = True no main.py para executar.'
        ],
    }


def _executar_modo_individual(nome_modo, funcao_execucao):
    if not ALLOW_MODOS_INDIVIDUAIS:
        return _modo_individual_bloqueado(nome_modo)
    return funcao_execucao()


def _run_individual_unificar_status_respostas():
    return _executar_modo_individual(
        'individual_unificar_status_respostas',
        lambda: run_unificar_status_respostas_pipeline(
            arquivo_eletivo=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_eletivo'],
            arquivo_internacao=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_internacao'],
            arquivo_saida=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_unificado'],
        ),
    )


def _run_individual_ingestao_complicacao():
    return _executar_modo_individual(
        'individual_ingestao_complicacao',
        lambda: run_ingestao_complicacao(
            arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
            arquivo_status_resposta_complicacao=DEFAULTS_COMPLICACAO['arquivo_status_resposta_complicacao'],
            saida_status=DEFAULTS_COMPLICACAO['saida_status'],
            saida_status_resposta=DEFAULTS_COMPLICACAO['saida_status_resposta'],
        ),
    )


def _run_individual_ingestao_internacao_eletivo():
    return _executar_modo_individual(
        'individual_ingestao_internacao_eletivo',
        lambda: run_ingestao_unificar(
            arquivo_status=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status'],
            arquivo_status_resposta_eletivo=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_eletivo'],
            arquivo_status_resposta_internacao=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_internacao'],
            arquivo_status_resposta_unificado=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_unificado'],
            saida_status=DEFAULTS_INTERNACAO_ELETIVO['saida_status'],
            saida_status_resposta=DEFAULTS_INTERNACAO_ELETIVO['saida_status_resposta'],
        ),
    )


def _run_individual_status_somente_complicacao():
    return _executar_modo_individual(
        'individual_status_somente_complicacao',
        lambda: run_ingestao_somente_status(
            arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
            saida_status=DEFAULTS_COMPLICACAO['saida_status'],
            nome_logger='ingestao_complicacao_individual_status',
        ),
    )


def _run_individual_status_somente_internacao_eletivo():
    return _executar_modo_individual(
        'individual_status_somente_internacao_eletivo',
        lambda: run_ingestao_somente_status(
            arquivo_status=DEFAULTS_INTERNACAO_ELETIVO['arquivo_status'],
            saida_status=DEFAULTS_INTERNACAO_ELETIVO['saida_status'],
            nome_logger='ingestao_internacao_eletivo_individual_status',
        ),
    )


def _run_individual_enviar_status_complicacao():
    return _executar_modo_individual(
        'individual_enviar_status_complicacao',
        lambda: run_unificar_status_resposta_complicacao_pipeline(
            arquivo_status=DEFAULTS_COMPLICACAO['saida_status'],
            arquivo_status_resposta=DEFAULTS_COMPLICACAO['saida_status_resposta'],
            arquivo_saida=DEFAULTS_COMPLICACAO['saida_status_integrado'],
        ),
    )


def _run_individual_enviar_status_internacao_eletivo():
    return _executar_modo_individual(
        'individual_enviar_status_internacao_eletivo',
        lambda: run_unificar_status_resposta_internacao_eletivo_pipeline(
            arquivo_status=DEFAULTS_INTERNACAO_ELETIVO['saida_status'],
            arquivo_status_resposta=DEFAULTS_INTERNACAO_ELETIVO['saida_status_resposta'],
            arquivo_saida=DEFAULTS_INTERNACAO_ELETIVO['saida_status_integrado'],
        ),
    )


def _run_individual_status_filtrado_complicacao():
    return _executar_modo_individual(
        'individual_status_filtrado_complicacao',
        lambda: run_status_somente_complicacao_pipeline(
            arquivo_status=DEFAULTS_COMPLICACAO['saida_status'],
            arquivo_saida=DEFAULTS_COMPLICACAO['saida_status_integrado'],
        ),
    )


def _run_individual_status_filtrado_internacao_eletivo():
    return _executar_modo_individual(
        'individual_status_filtrado_internacao_eletivo',
        lambda: run_status_somente_internacao_eletivo_pipeline(
            arquivo_status=DEFAULTS_INTERNACAO_ELETIVO['saida_status'],
            arquivo_saida=DEFAULTS_INTERNACAO_ELETIVO['saida_status_integrado'],
        ),
    )


def _run_individual_criar_dataset_complicacao():
    return _executar_modo_individual(
        'individual_criar_dataset_complicacao',
        lambda: run_criacao_dataset_pipeline(
            arquivo_origem_dataset=DEFAULTS_COMPLICACAO['arquivo_dataset_origem_complicacao'],
            arquivo_status_integrado=DEFAULTS_COMPLICACAO['saida_status_integrado'],
            arquivo_saida_dataset=DEFAULTS_COMPLICACAO['saida_dataset_status'],
            nome_logger='criacao_dataset_complicacao_individual',
            contexto='complicacao',
        ),
    )


def _run_individual_criar_dataset_internacao_eletivo():
    return _executar_modo_individual(
        'individual_criar_dataset_internacao_eletivo',
        lambda: run_criacao_dataset_pipeline(
            arquivo_origem_dataset=DEFAULTS_INTERNACAO_ELETIVO['arquivo_dataset_origem_internacao'],
            arquivo_status_integrado=DEFAULTS_INTERNACAO_ELETIVO['saida_status_integrado'],
            arquivo_saida_dataset=DEFAULTS_INTERNACAO_ELETIVO['saida_dataset_status'],
            nome_logger='criacao_dataset_internacao_eletivo_individual',
            contexto='internacao_eletivo',
        ),
    )


def _run_individual_orquestrar_complicacao():
    return _executar_modo_individual(
        'individual_orquestrar_complicacao',
        lambda: run_finalizacao_pipeline(
            arquivo_dataset_entrada=DEFAULTS_COMPLICACAO['saida_dataset_status'],
            arquivo_dataset_saida=DEFAULTS_COMPLICACAO['saida_dataset_final'],
            nome_logger='finalizacao_complicacao_individual',
        ),
    )


def _run_individual_orquestrar_internacao_eletivo():
    return _executar_modo_individual(
        'individual_orquestrar_internacao_eletivo',
        lambda: run_finalizacao_pipeline(
            arquivo_dataset_entrada=DEFAULTS_INTERNACAO_ELETIVO['saida_dataset_status'],
            arquivo_dataset_saida=DEFAULTS_INTERNACAO_ELETIVO['saida_dataset_final'],
            nome_logger='finalizacao_internacao_eletivo_individual',
        ),
    )


MODOS_INDIVIDUAIS = {
    'individual_unificar_status_respostas': _run_individual_unificar_status_respostas,
    'individual_ingestao_complicacao': _run_individual_ingestao_complicacao,
    'individual_ingestao_internacao_eletivo': _run_individual_ingestao_internacao_eletivo,
    'individual_status_somente_complicacao': _run_individual_status_somente_complicacao,
    'individual_status_somente_internacao_eletivo': _run_individual_status_somente_internacao_eletivo,
    'individual_status_filtrado_complicacao': _run_individual_status_filtrado_complicacao,
    'individual_status_filtrado_internacao_eletivo': _run_individual_status_filtrado_internacao_eletivo,
    'individual_enviar_status_complicacao': _run_individual_enviar_status_complicacao,
    'individual_enviar_status_internacao_eletivo': _run_individual_enviar_status_internacao_eletivo,
    'individual_criar_dataset_complicacao': _run_individual_criar_dataset_complicacao,
    'individual_criar_dataset_internacao_eletivo': _run_individual_criar_dataset_internacao_eletivo,
    'individual_orquestrar_complicacao': _run_individual_orquestrar_complicacao,
    'individual_orquestrar_internacao_eletivo': _run_individual_orquestrar_internacao_eletivo,
}


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
MODO_FUNCAO.update(MODOS_INDIVIDUAIS)


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
            *MODOS_INDIVIDUAIS.keys(),
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
