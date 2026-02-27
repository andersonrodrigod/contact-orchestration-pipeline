from src.utils.arquivos import ler_arquivo_csv
from src.services.normalizacao_services import (
    limpar_texto_exceto_colunas,
    normalizar_tipos_dataframe,
)


def executar_leitura_limpeza_status(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta='src/data/status_resposta_complicacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
):
    df_status = ler_arquivo_csv(arquivo_status)
    df_status_resposta = ler_arquivo_csv(arquivo_status_resposta)

    df_status = normalizar_tipos_dataframe(df_status, colunas_data=['Data agendamento'])
    df_status_resposta = normalizar_tipos_dataframe(df_status_resposta, colunas_data=['dat_atendimento'])

    df_status = limpar_texto_exceto_colunas(df_status, colunas_ignorar=['Data agendamento'])
    df_status_resposta = limpar_texto_exceto_colunas(df_status_resposta, colunas_ignorar=['dat_atendimento'])

    df_status.to_csv(saida_status, sep=';', index=False, encoding='utf-8-sig')
    df_status_resposta.to_csv(saida_status_resposta, sep=';', index=False, encoding='utf-8-sig')
