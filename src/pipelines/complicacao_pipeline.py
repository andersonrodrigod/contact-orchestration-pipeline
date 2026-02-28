from src.pipelines.criacao_dataset_pipeline import run_criacao_dataset_pipeline
from src.pipelines.ingestao_pipeline import run_ingestao_complicacao
from src.pipelines.integracao_pipeline import integrar_dados_status_complicacao


def run_complicacao_pipeline(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_complicacao='src/data/status_resposta_complicacao.csv',
    arquivo_dataset_origem_complicacao='src/data/complicacao.xlsx',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    saida_status_integrado='src/data/arquivo_limpo/status_complicacao.csv',
    saida_dataset='src/data/arquivo_limpo/dataset_complicacao.xlsx',
):
    resultado_ingestao = run_ingestao_complicacao(
        arquivo_status=arquivo_status,
        arquivo_status_resposta_complicacao=arquivo_status_resposta_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
    )
    if not resultado_ingestao.get('ok'):
        return resultado_ingestao

    resultado_integracao = integrar_dados_status_complicacao(
        arquivo_status=saida_status,
        arquivo_status_resposta=saida_status_resposta,
        arquivo_saida=saida_status_integrado,
    )
    if not resultado_integracao.get('ok'):
        return resultado_integracao

    resultado_dataset = run_criacao_dataset_pipeline(
        arquivo_complicacao=arquivo_dataset_origem_complicacao,
        arquivo_saida_dataset=saida_dataset,
        nome_logger='criacao_dataset_complicacao',
    )
    if not resultado_dataset.get('ok'):
        return resultado_dataset

    return {
        **resultado_integracao,
        **resultado_dataset,
        'arquivo_saida': resultado_dataset.get('arquivo_saida'),
    }
