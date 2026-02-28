import pandas as pd

from src.utils.arquivos import ler_arquivo_csv


def _validar_e_derivar_chave_data(df, coluna_original, coluna_chave):
    if coluna_original not in df.columns:
        raise ValueError(f'Coluna obrigatoria nao encontrada: {coluna_original}')

    serie_data = pd.to_datetime(df[coluna_original].astype(str).str.strip(), errors='coerce', dayfirst=True)
    qtd_invalidos = int(serie_data.isna().sum())
    if qtd_invalidos > 0:
        raise ValueError(
            f'Coluna {coluna_original} possui {qtd_invalidos} valores de data invalidos para integracao.'
        )

    df[coluna_chave] = serie_data.dt.date
    return df


def integrar_status_com_resposta(
    arquivo_status='src/data/arquivo_limpo/status_limpo.csv',
    arquivo_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    arquivo_saida='src/data/arquivo_limpo/status.csv',
):
    df_status = ler_arquivo_csv(arquivo_status)
    df_resposta = ler_arquivo_csv(arquivo_status_resposta)

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
    df_status = _validar_e_derivar_chave_data(df_status, 'DT ENVIO', '__CHAVE_DATA')
    df_resposta = _validar_e_derivar_chave_data(
        df_resposta, 'DT_ATENDIMENTO', '__CHAVE_DATA'
    )

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

    match = int((df_merge['RESPOSTA'] != 'Sem resposta').sum())
    sem_match = int((df_merge['RESPOSTA'] == 'Sem resposta').sum())

    df_merge = df_merge.drop(
        columns=['nom_contato', '__CHAVE_DATA', 'resposta'],
        errors='ignore',
    )

    df_merge.to_csv(arquivo_saida, sep=';', index=False, encoding='utf-8-sig')

    return {
        'ok': True,
        'arquivo_saida': arquivo_saida,
        'total_status': len(df_merge),
        'com_match': match,
        'sem_match': sem_match,
    }
