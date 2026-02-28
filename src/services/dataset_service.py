import pandas as pd

from src.utils.arquivos import ler_arquivo_csv
from src.services.schema_service import padronizar_colunas_status_resposta
from src.services.validacao_service import validar_colunas_minimas_status_resposta


def concatenar_status_resposta_eletivo_internacao(
    arquivo_eletivo='src/data/status_respostas_eletivo.csv',
    arquivo_internacao='src/data/status_resposta_internacao.csv',
    arquivo_saida='src/data/status_resposta_eletivo_internacao.csv',
):
    df_eletivo = ler_arquivo_csv(arquivo_eletivo)
    df_internacao = ler_arquivo_csv(arquivo_internacao)
    total_eletivo = len(df_eletivo)
    total_internacao = len(df_internacao)

    validacao_colunas = validar_colunas_minimas_status_resposta(df_eletivo, df_internacao)
    if not validacao_colunas['ok']:
        return {
            'ok': False,
            'mensagens': validacao_colunas['mensagens'],
            'total_eletivo': total_eletivo,
            'total_internacao': total_internacao,
            'total_concatenado': 0,
        }

    colunas_unificadas = sorted(set(df_eletivo.columns).union(set(df_internacao.columns)))
    df_eletivo = df_eletivo.reindex(columns=colunas_unificadas, fill_value='')
    df_internacao = df_internacao.reindex(columns=colunas_unificadas, fill_value='')

    df_concatenado = pd.concat([df_eletivo, df_internacao], ignore_index=True)
    df_concatenado = padronizar_colunas_status_resposta(df_concatenado)
    df_concatenado.to_csv(arquivo_saida, sep=';', index=False, encoding='utf-8-sig')
    total_concatenado = len(df_concatenado)

    return {
        'ok': True,
        'mensagens': ['Concatenacao status_resposta_eletivo_internacao executada com sucesso.'],
        'total_eletivo': total_eletivo,
        'total_internacao': total_internacao,
        'total_concatenado': total_concatenado,
    }
