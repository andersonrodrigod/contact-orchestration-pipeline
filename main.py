import argparse
from pathlib import Path

from src.pipelines.ingestao_pipeline import (
    run_ingestao_complicacao,
    run_ingestao_unificar,
)
from src.pipelines.integracao_pipeline import (
    integrar_dados_status_complicacao,
    integrar_dados_status_unificar,
)


DEFAULT_PATHS = {
    'arquivo_status': 'src/data/status.csv',
    'arquivo_status_resposta_complicacao': 'src/data/status_resposta_complicacao.csv',
    'arquivo_status_resposta_eletivo': 'src/data/status_respostas_eletivo.csv',
    'arquivo_status_resposta_internacao': 'src/data/status_resposta_internacao.csv',
    'arquivo_status_resposta_unificado': 'src/data/status_resposta_eletivo_internacao.csv',
    'saida_status': 'src/data/arquivo_limpo/status_limpo.csv',
    'saida_status_resposta_complicacao': 'src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    'saida_status_resposta_unificado': 'src/data/arquivo_limpo/status_resposta_eletivo_internacao_limpo.csv',
    'saida_status_integrado_complicacao': 'src/data/arquivo_limpo/status_complicacao_integrado.csv',
    'saida_status_integrado_unificado': 'src/data/arquivo_limpo/status_unificado_integrado.csv',
}


def _arquivo_existe_e_tem_dados(caminho):
    arquivo = Path(caminho)
    return arquivo.exists() and arquivo.is_file() and arquivo.stat().st_size > 0


def _resolver_modo_execucao(args):
    if args.modo in ['complicacao', 'unificar']:
        return args.modo

    if _arquivo_existe_e_tem_dados(args.arquivo_status_resposta_complicacao):
        return 'complicacao'

    if (
        _arquivo_existe_e_tem_dados(args.arquivo_status_resposta_eletivo)
        and _arquivo_existe_e_tem_dados(args.arquivo_status_resposta_internacao)
    ):
        return 'unificar'

    return 'complicacao'


def _imprimir_resumo_execucao(resultado):
    if not isinstance(resultado, dict):
        print('Resultado da execucao indisponivel.')
        return

    print(f"OK: {resultado.get('ok', False)}")
    if 'arquivo_saida' in resultado:
        print(f"Arquivo final: {resultado['arquivo_saida']}")
    if 'total_status' in resultado:
        print(f"Total status: {resultado['total_status']}")
    if 'com_match' in resultado:
        print(f"Com match: {resultado['com_match']}")
    if 'sem_match' in resultado:
        print(f"Sem match: {resultado['sem_match']}")
    mensagens = resultado.get('mensagens', [])
    if mensagens:
        print('Mensagens:')
        for mensagem in mensagens:
            print(f'- {mensagem}')


def run_pipeline():
    parser = argparse.ArgumentParser()
    parser.add_argument('--modo', choices=['auto', 'complicacao', 'unificar'], default='auto')
    parser.add_argument('--arquivo-status', default=DEFAULT_PATHS['arquivo_status'])
    parser.add_argument('--arquivo-status-resposta-complicacao', default=DEFAULT_PATHS['arquivo_status_resposta_complicacao'])
    parser.add_argument('--arquivo-status-resposta-eletivo', default=DEFAULT_PATHS['arquivo_status_resposta_eletivo'])
    parser.add_argument('--arquivo-status-resposta-internacao', default=DEFAULT_PATHS['arquivo_status_resposta_internacao'])
    parser.add_argument('--arquivo-status-resposta-unificado', default=DEFAULT_PATHS['arquivo_status_resposta_unificado'])
    parser.add_argument('--saida-status', default=DEFAULT_PATHS['saida_status'])
    parser.add_argument('--saida-status-resposta-complicacao', default=DEFAULT_PATHS['saida_status_resposta_complicacao'])
    parser.add_argument('--saida-status-resposta-unificado', default=DEFAULT_PATHS['saida_status_resposta_unificado'])
    parser.add_argument('--saida-status-integrado-complicacao', default=DEFAULT_PATHS['saida_status_integrado_complicacao'])
    parser.add_argument('--saida-status-integrado-unificado', default=DEFAULT_PATHS['saida_status_integrado_unificado'])

    args = parser.parse_args()
    modo = _resolver_modo_execucao(args)

    if modo == 'complicacao':
        resultado_ingestao = run_ingestao_complicacao(
            arquivo_status=args.arquivo_status,
            arquivo_status_resposta_complicacao=args.arquivo_status_resposta_complicacao,
            saida_status=args.saida_status,
            saida_status_resposta=args.saida_status_resposta_complicacao,
        )
        if not resultado_ingestao.get('ok'):
            return resultado_ingestao

        return integrar_dados_status_complicacao(
            arquivo_status=args.saida_status,
            arquivo_status_resposta=args.saida_status_resposta_complicacao,
            arquivo_saida=args.saida_status_integrado_complicacao,
        )

    resultado_ingestao = run_ingestao_unificar(
        arquivo_status=args.arquivo_status,
        arquivo_status_resposta_eletivo=args.arquivo_status_resposta_eletivo,
        arquivo_status_resposta_internacao=args.arquivo_status_resposta_internacao,
        arquivo_status_resposta_unificado=args.arquivo_status_resposta_unificado,
        saida_status=args.saida_status,
        saida_status_resposta=args.saida_status_resposta_unificado,
    )
    if not resultado_ingestao.get('ok'):
        return resultado_ingestao

    return integrar_dados_status_unificar(
        arquivo_status=args.saida_status,
        arquivo_status_resposta=args.saida_status_resposta_unificado,
        arquivo_saida=args.saida_status_integrado_unificado,
    )


if __name__ == '__main__':
    resultado = run_pipeline()
    _imprimir_resumo_execucao(resultado)
