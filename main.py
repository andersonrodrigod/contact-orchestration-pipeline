import argparse

from src.pipelines.complicacao_pipeline import run_complicacao_pipeline
from src.pipelines.internacao_eletivo_pipeline import run_internacao_eletivo_pipeline


DEFAULT_PATHS = {
    'arquivo_status': 'src/data/status.csv',
    'arquivo_status_resposta_complicacao': 'src/data/status_resposta_complicacao.csv',
    'arquivo_status_resposta_eletivo': 'src/data/status_respostas_eletivo.csv',
    'arquivo_status_resposta_internacao': 'src/data/status_resposta_internacao.csv',
    'arquivo_status_resposta_unificado': 'src/data/status_resposta_eletivo_internacao.csv',
    'arquivo_dataset_origem_complicacao': 'src/data/complicacao.xlsx',
    'saida_status_complicacao_limpo': 'src/data/arquivo_limpo/status_complicacao_limpo.csv',
    'saida_status_internacao_eletivo_limpo': 'src/data/arquivo_limpo/status_internacao_eletivo_limpo.csv',
    'saida_status_resposta_complicacao': 'src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    'saida_status_resposta_unificado': 'src/data/arquivo_limpo/status_resposta_eletivo_internacao_limpo.csv',
    'saida_status_complicacao': 'src/data/arquivo_limpo/status_complicacao.csv',
    'saida_status_internacao_eletivo': 'src/data/arquivo_limpo/status_internacao_eletivo.csv',
    'saida_dataset_complicacao': 'src/data/arquivo_limpo/dataset_complicacao.xlsx',
}


def _imprimir_resumo_execucao(resultado):
    if not isinstance(resultado, dict):
        print('Resultado da execucao indisponivel.')
        return

    print(f"OK: {resultado.get('ok', False)}")
    if 'arquivo_saida' in resultado:
        print(f"Arquivo final: {resultado['arquivo_saida']}")
    if 'total_status' in resultado:
        print(f"Total status: {resultado['total_status']}")
    if 'total_linhas' in resultado:
        print(f"Total dataset: {resultado['total_linhas']}")
    if 'com_match' in resultado:
        print(f"Com match: {resultado['com_match']}")
    if 'sem_match' in resultado:
        print(f"Sem match: {resultado['sem_match']}")
    if 'resultados' in resultado:
        for nome, res in resultado['resultados'].items():
            print(f"{nome}: OK={res.get('ok', False)} arquivo={res.get('arquivo_saida', '')}")
    mensagens = resultado.get('mensagens', [])
    if mensagens:
        print('Mensagens:')
        for mensagem in mensagens:
            print(f'- {mensagem}')


def _montar_resultado_ambos(resultado_complicacao, resultado_internacao_eletivo):
    ok_geral = resultado_complicacao.get('ok', False) and resultado_internacao_eletivo.get('ok', False)
    return {
        'ok': ok_geral,
        'resultados': {
            'complicacao': resultado_complicacao,
            'internacao_eletivo': resultado_internacao_eletivo,
        },
        'mensagens': ['Execucao em modo ambos finalizada.'],
    }


def _executar_complicacao(args, saida_status):
    return run_complicacao_pipeline(
        arquivo_status=args.arquivo_status,
        arquivo_status_resposta_complicacao=args.arquivo_status_resposta_complicacao,
        arquivo_dataset_origem_complicacao=args.arquivo_dataset_origem_complicacao,
        saida_status=saida_status,
        saida_status_resposta=args.saida_status_resposta_complicacao,
        saida_status_integrado=args.saida_status_complicacao,
        saida_dataset=args.saida_dataset_complicacao,
    )


def _executar_internacao_eletivo(args, saida_status):
    return run_internacao_eletivo_pipeline(
        arquivo_status=args.arquivo_status,
        arquivo_status_resposta_eletivo=args.arquivo_status_resposta_eletivo,
        arquivo_status_resposta_internacao=args.arquivo_status_resposta_internacao,
        arquivo_status_resposta_unificado=args.arquivo_status_resposta_unificado,
        saida_status=saida_status,
        saida_status_resposta=args.saida_status_resposta_unificado,
        saida_status_integrado=args.saida_status_internacao_eletivo,
    )


def run_pipeline():
    parser = argparse.ArgumentParser()
    parser.add_argument('--modo', choices=['complicacao', 'internacao_eletivo', 'ambos'], default='ambos')
    parser.add_argument('--arquivo-status', default=DEFAULT_PATHS['arquivo_status'])
    parser.add_argument('--arquivo-status-resposta-complicacao', default=DEFAULT_PATHS['arquivo_status_resposta_complicacao'])
    parser.add_argument('--arquivo-status-resposta-eletivo', default=DEFAULT_PATHS['arquivo_status_resposta_eletivo'])
    parser.add_argument('--arquivo-status-resposta-internacao', default=DEFAULT_PATHS['arquivo_status_resposta_internacao'])
    parser.add_argument('--arquivo-status-resposta-unificado', default=DEFAULT_PATHS['arquivo_status_resposta_unificado'])
    parser.add_argument('--arquivo-dataset-origem-complicacao', default=DEFAULT_PATHS['arquivo_dataset_origem_complicacao'])
    parser.add_argument('--saida-status-complicacao-limpo', default=DEFAULT_PATHS['saida_status_complicacao_limpo'])
    parser.add_argument('--saida-status-internacao-eletivo-limpo', default=DEFAULT_PATHS['saida_status_internacao_eletivo_limpo'])
    parser.add_argument('--saida-status-resposta-complicacao', default=DEFAULT_PATHS['saida_status_resposta_complicacao'])
    parser.add_argument('--saida-status-resposta-unificado', default=DEFAULT_PATHS['saida_status_resposta_unificado'])
    parser.add_argument('--saida-status-complicacao', default=DEFAULT_PATHS['saida_status_complicacao'])
    parser.add_argument('--saida-status-internacao-eletivo', default=DEFAULT_PATHS['saida_status_internacao_eletivo'])
    parser.add_argument('--saida-dataset-complicacao', default=DEFAULT_PATHS['saida_dataset_complicacao'])
    args = parser.parse_args()

    if args.modo == 'complicacao':
        return _executar_complicacao(args, saida_status=args.saida_status_complicacao_limpo)

    if args.modo == 'internacao_eletivo':
        return _executar_internacao_eletivo(args, saida_status=args.saida_status_internacao_eletivo_limpo)

    resultado_complicacao = _executar_complicacao(args, saida_status=args.saida_status_complicacao_limpo)
    resultado_internacao_eletivo = _executar_internacao_eletivo(
        args,
        saida_status=args.saida_status_internacao_eletivo_limpo,
    )
    return _montar_resultado_ambos(resultado_complicacao, resultado_internacao_eletivo)


if __name__ == '__main__':
    resultado = run_pipeline()
    _imprimir_resumo_execucao(resultado)
