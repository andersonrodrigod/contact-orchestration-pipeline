from src.services.ingestao_service import (
    executar_ingestao_complicacao,
    executar_ingestao_somente_status,
    executar_ingestao_unificar,
)


def run_ingestao_complicacao(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_complicacao='src/data/status_resposta_complicacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
):
    return executar_ingestao_complicacao(
        arquivo_status=arquivo_status,
        arquivo_status_resposta_complicacao=arquivo_status_resposta_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
    )


def run_ingestao_somente_status(
    arquivo_status='src/data/status.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    nome_logger='ingestao_somente_status',
):
    return executar_ingestao_somente_status(
        arquivo_status=arquivo_status,
        saida_status=saida_status,
        nome_logger=nome_logger,
    )


def run_ingestao_unificar(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_eletivo='src/data/status_respostas_eletivo.csv',
    arquivo_status_resposta_internacao='src/data/status_resposta_internacao.csv',
    arquivo_status_resposta_unificado='src/data/status_resposta_eletivo_internacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_eletivo_internacao_limpo.csv',
):
    return executar_ingestao_unificar(
        arquivo_status=arquivo_status,
        arquivo_status_resposta_eletivo=arquivo_status_resposta_eletivo,
        arquivo_status_resposta_internacao=arquivo_status_resposta_internacao,
        arquivo_status_resposta_unificado=arquivo_status_resposta_unificado,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
    )
