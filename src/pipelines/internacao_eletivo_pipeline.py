from src.pipelines.ingestao_pipeline import run_ingestao_unificar
from src.pipelines.integracao_pipeline import integrar_dados_status_unificar


def run_internacao_eletivo_pipeline(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_eletivo='src/data/status_respostas_eletivo.csv',
    arquivo_status_resposta_internacao='src/data/status_resposta_internacao.csv',
    arquivo_status_resposta_unificado='src/data/status_resposta_eletivo_internacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_eletivo_internacao_limpo.csv',
    saida_status_integrado='src/data/arquivo_limpo/status_internacao_eletivo.csv',
):
    resultado_ingestao = run_ingestao_unificar(
        arquivo_status=arquivo_status,
        arquivo_status_resposta_eletivo=arquivo_status_resposta_eletivo,
        arquivo_status_resposta_internacao=arquivo_status_resposta_internacao,
        arquivo_status_resposta_unificado=arquivo_status_resposta_unificado,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
    )
    if not resultado_ingestao.get('ok'):
        return resultado_ingestao

    return integrar_dados_status_unificar(
        arquivo_status=saida_status,
        arquivo_status_resposta=saida_status_resposta,
        arquivo_saida=saida_status_integrado,
    )
