from src.utils.arquivos import ler_arquivo_csv, salvar_output_validacao
from src.services.normalizacao_services import (
    limpar_texto_exceto_colunas,
    normalizar_tipos_dataframe,
)
from src.services.schema_service import (
    padronizar_colunas_status,
    padronizar_colunas_status_resposta,
    validar_colunas_origem_para_padronizacao,
    validar_padronizacao_colunas_data,
)

def run_ingestao_pipeline(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta='src/data/status_resposta_complicacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    output_validacao='src/data/arquivo_limpo/output_validacao_datas.txt',
):
    df_status = ler_arquivo_csv(arquivo_status)
    df_status_resposta = ler_arquivo_csv(arquivo_status_resposta)

    resultado_colunas_origem = validar_colunas_origem_para_padronizacao(
        df_status, df_status_resposta
    )
    if not resultado_colunas_origem['ok']:
        salvar_output_validacao(resultado_colunas_origem, output_validacao)
        return resultado_colunas_origem

    df_status = padronizar_colunas_status(df_status)
    df_status_resposta = padronizar_colunas_status_resposta(df_status_resposta)

    df_status = normalizar_tipos_dataframe(df_status, colunas_data=['DT_ENVIO'])
    df_status_resposta = normalizar_tipos_dataframe(df_status_resposta, colunas_data=['DT_ATENDIMENTO'])

    df_status = limpar_texto_exceto_colunas(df_status, colunas_ignorar=['DT_ENVIO'])
    df_status_resposta = limpar_texto_exceto_colunas(df_status_resposta, colunas_ignorar=['DT_ATENDIMENTO'])

    resultado_validacao = validar_padronizacao_colunas_data(df_status, df_status_resposta)
    resultado_final = {
        'ok': resultado_colunas_origem['ok'] and resultado_validacao['ok'],
        'mensagens': resultado_colunas_origem['mensagens'] + resultado_validacao['mensagens'],
    }
    salvar_output_validacao(resultado_final, output_validacao)

    df_status.to_csv(saida_status, sep=';', index=False, encoding='utf-8-sig')
    df_status_resposta.to_csv(saida_status_resposta, sep=';', index=False, encoding='utf-8-sig')

    return resultado_final
