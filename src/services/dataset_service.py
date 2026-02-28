import pandas as pd

from src.services.normalizacao_services import (
    normalizar_colunas_telefone_dataframe,
    normalizar_tipos_dataframe,
)
from src.services.schema_service import padronizar_colunas_status_resposta
from src.services.validacao_service import (
    validar_colunas_minimas_status_resposta,
    validar_colunas_origem_dataset_complicacao,
)
from src.utils.arquivos import ler_arquivo_csv


COLUNAS_FINAIS_DATASET = [
    'BASE', 'COD USUARIO', 'USUARIO',
    'TELEFONE 1', 'TELEFONE 2', 'TELEFONE 3', 'TELEFONE 4', 'TELEFONE 5',
    'PRESTADOR', 'PROCEDIMENTO', 'TP ATENDIMENTO', 'DT INTERNACAO', 'DT ENVIO',
    'ULTIMO STATUS DE ENVIO', 'IDENTIFICACAO', 'RESPOSTA', 'LIDA_REPOSTA_SIM', 'LIDA_REPOSTA_NAO',
    'LIDA_SEM_RESPOSTA', 'LIDA', 'ENTREGUE', 'ENVIADA', 'NAO_ENTREGUE_META', 'MENSAGEM_NAO_ENTREGUE',
    'EXPERIMENTO', 'OPT_OUT', 'TELEFONE ENVIADO', 'TELEFONE PRIORIDADE', 'CHAVE RELATORIO', 'CHAVE STATUS',
    'STATUS TELEFONE', 'STATUS CHAVE', 'PROCESSO', 'ACAO', 'QT LIDA', 'QT ENTREGUE', 'QT ENVIADA',
    'QT NAO_ENTREGUE_META', 'QT MENSAGEM_NAO_ENTREGUE', 'QT EXPERIMENTO', 'QT OPT_OUT', 'QT TELEFONE',
    'TELEFONE STATUS 1', 'TELEFONE STATUS 2', 'TELEFONE STATUS 3', 'TELEFONE STATUS 4', 'TELEFONE STATUS 5',
]


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


def _montar_df_final_complicacao(df_base):
    df_final = pd.DataFrame(index=df_base.index)

    mapeamento = {
        'BASE': 'BASE',
        'COD USUARIO': 'COD USUARIO',
        'USUARIO': 'USUARIO',
        'TELEFONE 1': 'TELEFONE 1',
        'TELEFONE 2': 'TELEFONE 2',
        'TELEFONE 3': 'TELEFONE 3',
        'TELEFONE 4': 'TELEFONE 4',
        'TELEFONE 5': 'TELEFONE 5',
        'PRESTADOR': 'PRESTADOR',
        'PROCEDIMENTO': 'PROCEDIMENTO',
        'TP ATENDIMENTO': 'TP ATENDIMENTO',
        'DT INTERNACAO': 'DT INTERNACAO',
        'DT ENVIO': 'DT ENVIO',
        'CHAVE': 'CHAVE RELATORIO',
        'STATUS': 'ULTIMO STATUS DE ENVIO',
    }

    for coluna_origem, coluna_destino in mapeamento.items():
        if coluna_origem in df_base.columns:
            df_final[coluna_destino] = df_base[coluna_origem]

    for coluna in COLUNAS_FINAIS_DATASET:
        if coluna not in df_final.columns:
            df_final[coluna] = ''

    df_final = df_final[COLUNAS_FINAIS_DATASET].copy()

    colunas_data = [col for col in ['DT INTERNACAO', 'DT ENVIO'] if col in df_final.columns]
    df_final = normalizar_tipos_dataframe(df_final, colunas_data=colunas_data)
    colunas_telefone = [col for col in df_final.columns if 'TELEFONE' in col]
    df_final = normalizar_colunas_telefone_dataframe(df_final, colunas_telefone)

    return df_final


def criar_dataset_complicacao(
    arquivo_complicacao,
    arquivo_saida_dataset,
):
    df = ler_arquivo_csv(arquivo_complicacao)
    df.columns = [str(col).strip() for col in df.columns]

    validacao_colunas = validar_colunas_origem_dataset_complicacao(df.columns)
    if not validacao_colunas['ok']:
        return {
            'ok': False,
            'mensagens': validacao_colunas['mensagens'],
            'colunas_arquivo': list(df.columns),
            'colunas_faltando': validacao_colunas['colunas_faltando'],
        }

    mask_duplicados = df.duplicated(subset=['COD USUARIO'], keep=False)
    df_duplicados = df[mask_duplicados].copy()
    df_sem_duplicados = df[~mask_duplicados].copy()

    df_usuarios = _montar_df_final_complicacao(df_sem_duplicados)

    status_lidos = ['Lida', 'Não quis', 'Obito', 'Óbito']
    if 'STATUS' in df_sem_duplicados.columns:
        df_lidos_base = df_sem_duplicados[df_sem_duplicados['STATUS'].isin(status_lidos)]
    else:
        df_lidos_base = df_sem_duplicados.iloc[0:0]
    df_usuarios_lidos = _montar_df_final_complicacao(df_lidos_base)

    status_respondidos = ['Obito', 'Óbito', 'Não quis']
    if 'STATUS' in df_sem_duplicados.columns and 'P1' in df_sem_duplicados.columns:
        df_resp_base = df_sem_duplicados[
            df_sem_duplicados['STATUS'].isin(status_respondidos)
            & df_sem_duplicados['P1'].notna()
        ]
    else:
        df_resp_base = df_sem_duplicados.iloc[0:0]
    df_usuarios_respondidos = _montar_df_final_complicacao(df_resp_base)

    df_usuarios_duplicados = _montar_df_final_complicacao(df_duplicados)
    df_usuarios_resolvidos = pd.DataFrame(columns=COLUNAS_FINAIS_DATASET)

    with pd.ExcelWriter(arquivo_saida_dataset, engine='openpyxl') as writer:
        df_usuarios.to_excel(writer, sheet_name='usuarios', index=False)
        df_usuarios_lidos.to_excel(writer, sheet_name='usuarios_lidos', index=False)
        df_usuarios_respondidos.to_excel(writer, sheet_name='usuarios_respondidos', index=False)
        df_usuarios_duplicados.to_excel(writer, sheet_name='usuarios_duplicados', index=False)
        df_usuarios_resolvidos.to_excel(writer, sheet_name='usuarios_resolvidos', index=False)

    return {
        'ok': True,
        'arquivo_saida': arquivo_saida_dataset,
        'total_linhas': len(df_usuarios),
        'mensagens': validacao_colunas['mensagens'] + ['Dataset de complicacao criado com sucesso.'],
        'colunas_arquivo': list(df.columns),
        'colunas_faltando': [],
    }
