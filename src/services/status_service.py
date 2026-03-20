import pandas as pd

from src.services.schema_resposta_service import (
    garantir_contrato_resposta_canonica,
    normalizar_coluna_resposta,
)
from src.utils.arquivos import ler_arquivo_csv, salvar_dataframe


def _preservar_status_e_derivar_chave_data(df, coluna_original, coluna_chave):
    if coluna_original not in df.columns:
        raise ValueError(f'Coluna obrigatoria nao encontrada: {coluna_original}')

    df_resultado = df.copy()
    serie_original = df_resultado[coluna_original].astype(str).str.strip()
    serie_data = pd.to_datetime(serie_original, errors='coerce', dayfirst=True)
    mask_valida = serie_data.notna()
    qtd_invalidos = int((~mask_valida).sum())

    # Mantem as linhas de status no dataset final; datas invalidas apenas perdem a chave
    # de integracao e ficam vazias no arquivo salvo.
    df_resultado.loc[~mask_valida, coluna_original] = ''
    df_resultado[coluna_chave] = serie_data.dt.date
    return df_resultado, qtd_invalidos


def _filtrar_e_derivar_chave_data(df, coluna_original, coluna_chave):
    if coluna_original not in df.columns:
        raise ValueError(f'Coluna obrigatoria nao encontrada: {coluna_original}')

    serie_data = pd.to_datetime(df[coluna_original].astype(str).str.strip(), errors='coerce', dayfirst=True)
    mask_valida = serie_data.notna()
    qtd_invalidos = int((~mask_valida).sum())

    df_filtrado = df.loc[mask_valida].copy()
    df_filtrado[coluna_chave] = serie_data.loc[mask_valida].dt.date
    return df_filtrado, qtd_invalidos


def _falhar_se_todas_datas_invalidas(total_linhas, descartados, coluna):
    if total_linhas > 0 and descartados == total_linhas:
        raise ValueError(
            f'Coluna {coluna} possui 100% de datas invalidas ({descartados}/{total_linhas}).'
        )


def integrar_status_com_resposta(
    arquivo_status='src/data/arquivo_limpo/status_limpo.csv',
    arquivo_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    arquivo_saida='src/data/arquivo_limpo/status.csv',
    colunas_limpar=None,
):
    df_status = ler_arquivo_csv(arquivo_status)
    df_resposta = ler_arquivo_csv(arquivo_status_resposta)
    df_resposta = normalizar_coluna_resposta(
        df_resposta,
        criar_vazia=True,
        remover_alias=True,
    )
    garantir_contrato_resposta_canonica(
        df_resposta,
        contexto='status.integracao_resposta_pos_padronizacao',
    )

    colunas_status_obrigatorias = ['Contato', 'DT ENVIO']
    colunas_resposta_obrigatorias = ['nom_contato', 'DT_ATENDIMENTO', 'resposta']

    faltando_status = [c for c in colunas_status_obrigatorias if c not in df_status.columns]
    faltando_resposta = [c for c in colunas_resposta_obrigatorias if c not in df_resposta.columns]
    if faltando_status or faltando_resposta:
        raise ValueError(
            f'Colunas faltando para integracao. status={faltando_status} resposta={faltando_resposta}'
        )

    df_status['Contato'] = df_status['Contato'].astype(str).str.strip()
    df_resposta['nom_contato'] = df_resposta['nom_contato'].astype(str).str.strip()

    # Validacao de contrato da integracao: as datas devem estar parseaveis.
    # A integracao nao normaliza dados novamente; ela apenas valida e deriva chave temporaria.
    total_status_entrada = len(df_status)
    total_resposta_entrada = len(df_resposta)

    df_status, descartados_status_data = _preservar_status_e_derivar_chave_data(
        df_status, 'DT ENVIO', '__CHAVE_DATA'
    )
    df_resposta, descartados_resposta_data = _filtrar_e_derivar_chave_data(
        df_resposta, 'DT_ATENDIMENTO', '__CHAVE_DATA'
    )
    _falhar_se_todas_datas_invalidas(total_resposta_entrada, descartados_resposta_data, 'DT_ATENDIMENTO')

    df_resposta = (
        df_resposta.sort_values('DT_ATENDIMENTO')
        .drop_duplicates(subset=['nom_contato', '__CHAVE_DATA'], keep='last')
    )

    df_merge = df_status.merge(
        df_resposta[['nom_contato', '__CHAVE_DATA', 'resposta']],
        left_on=['Contato', '__CHAVE_DATA'],
        right_on=['nom_contato', '__CHAVE_DATA'],
        how='left',
    )

    resposta_tratada = df_merge['resposta'].fillna('').astype(str).str.strip()
    df_merge['RESPOSTA'] = resposta_tratada.mask(resposta_tratada == '', 'Sem resposta')
    df_merge['NOME_MANIPULADO'] = (
        df_merge['Contato'].astype(str).str.split('_', n=1).str[0].str.strip()
    )

    if colunas_limpar is None:
        colunas_limpar = []
    for coluna in colunas_limpar:
        if coluna in df_merge.columns:
            df_merge[coluna] = ''

    match = int(df_merge['resposta'].notna().sum())
    sem_match = int(df_merge['resposta'].isna().sum())

    df_merge = df_merge.drop(
        columns=['nom_contato', '__CHAVE_DATA', 'resposta'],
        errors='ignore',
    )

    salvar_dataframe(df_merge, arquivo_saida)

    return {
        'ok': True,
        'arquivo_saida': arquivo_saida,
        'total_status': len(df_merge),
        'com_match': match,
        'sem_match': sem_match,
        'descartados_status_data_invalida': descartados_status_data,
        'descartados_resposta_data_invalida': descartados_resposta_data,
    }
