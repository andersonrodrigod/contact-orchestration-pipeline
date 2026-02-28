import pandas as pd

from src.utils.arquivos import ler_arquivo_csv
from src.services.validacao_service import validar_colunas_identicas


def concatenar_status_resposta_eletivo_internacao(
    arquivo_eletivo='src/data/status_respostas_eletivo.csv',
    arquivo_internacao='src/data/status_resposta_internacao.csv',
    arquivo_saida='src/data/status_resposta_eletivo_internacao.csv',
):
    df_eletivo = ler_arquivo_csv(arquivo_eletivo)
    df_internacao = ler_arquivo_csv(arquivo_internacao)

    validacao_colunas = validar_colunas_identicas(df_eletivo, df_internacao)
    if not validacao_colunas['ok']:
        return validacao_colunas

    df_concatenado = pd.concat([df_eletivo, df_internacao], ignore_index=True)
    df_concatenado.to_csv(arquivo_saida, sep=';', index=False, encoding='utf-8-sig')

    return {
        'ok': True,
        'mensagens': ['Concatenacao status_resposta_eletivo_internacao executada com sucesso.'],
    }
