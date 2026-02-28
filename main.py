import argparse

from src.pipelines.ingestao_pipeline import (
    run_ingestao_complicacao,
    run_ingestao_unificar,
)


def run_pipeline():
    parser = argparse.ArgumentParser()
    parser.add_argument('--modo', choices=['complicacao', 'unificar'], default='complicacao')
    parser.add_argument('--arquivo-status', default='src/data/status.csv')
    parser.add_argument('--arquivo-status-resposta-complicacao', default='src/data/status_resposta_complicacao.csv')
    parser.add_argument('--arquivo-status-resposta-eletivo', default='src/data/status_respostas_eletivo.csv')
    parser.add_argument('--arquivo-status-resposta-internacao', default='src/data/status_resposta_internacao.csv')
    parser.add_argument('--arquivo-status-resposta-unificado', default='src/data/status_resposta_eletivo_internacao.csv')
    parser.add_argument('--saida-status', default='src/data/arquivo_limpo/status_limpo.csv')
    parser.add_argument('--saida-status-resposta', default='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv')
    parser.add_argument('--output-validacao', default='src/data/arquivo_limpo/output_validacao_datas.txt')

    args = parser.parse_args()

    if args.modo == 'complicacao':
        return run_ingestao_complicacao(
            arquivo_status=args.arquivo_status,
            arquivo_status_resposta_complicacao=args.arquivo_status_resposta_complicacao,
            saida_status=args.saida_status,
            saida_status_resposta=args.saida_status_resposta,
            output_validacao=args.output_validacao,
        )

    return run_ingestao_unificar(
        arquivo_status=args.arquivo_status,
        arquivo_status_resposta_eletivo=args.arquivo_status_resposta_eletivo,
        arquivo_status_resposta_internacao=args.arquivo_status_resposta_internacao,
        arquivo_status_resposta_unificado=args.arquivo_status_resposta_unificado,
        saida_status=args.saida_status,
        saida_status_resposta=args.saida_status_resposta,
        output_validacao=args.output_validacao,
    )


if __name__ == '__main__':
    run_pipeline()
