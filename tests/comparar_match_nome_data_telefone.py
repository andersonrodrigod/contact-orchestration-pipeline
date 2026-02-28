import pandas as pd
import re
from pathlib import Path


def normalizar_telefone(valor):
    if pd.isna(valor):
        return ''
    return re.sub(r'\D', '', str(valor))


def criar_nome_manipulado(serie):
    return serie.astype(str).str.strip().str.split('_', n=1).str[0].str.strip()


def chave_data_sem_hora(serie):
    return pd.to_datetime(serie, errors='coerce', dayfirst=True).dt.date


def executar_comparacao_matchs():
    arquivo_status = 'src/data/arquivo_limpo/status_limpo.csv'
    arquivo_resposta = 'src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv'

    df_status = pd.read_csv(arquivo_status, sep=';', dtype=str, encoding='utf-8-sig', keep_default_na=False)
    df_resposta = pd.read_csv(arquivo_resposta, sep=';', dtype=str, encoding='utf-8-sig', keep_default_na=False)

    df_status['NOME_MANIPULADO'] = criar_nome_manipulado(df_status['Contato'])
    df_resposta['NOME_MANIPULADO'] = criar_nome_manipulado(df_resposta['nom_contato'])

    # Comparacao por data somente (hora/minuto/segundo sao ignorados).
    df_status['DATA_CHAVE'] = chave_data_sem_hora(df_status['DT ENVIO'])
    df_resposta['DATA_CHAVE'] = chave_data_sem_hora(df_resposta['DT_ATENDIMENTO'])

    df_status['TELEFONE_CHAVE'] = df_status['Telefone'].apply(normalizar_telefone)
    df_resposta['TELEFONE_CHAVE'] = df_resposta['num_telefone'].apply(normalizar_telefone)

    df_status['CONTATO_CHAVE'] = df_status['Contato'].astype(str).str.strip()
    df_resposta['CONTATO_CHAVE'] = df_resposta['nom_contato'].astype(str).str.strip()
    df_resposta_contato_data = (
        df_resposta.sort_values('DT_ATENDIMENTO')
        .drop_duplicates(subset=['CONTATO_CHAVE', 'DATA_CHAVE'], keep='last')
        [['CONTATO_CHAVE', 'DATA_CHAVE', 'nom_contato', 'DT_ATENDIMENTO', 'num_telefone']]
        .copy()
    )

    merge_primeiro = df_status.merge(
        df_resposta_contato_data,
        on=['CONTATO_CHAVE', 'DATA_CHAVE'],
        how='left',
    )

    mask_match_primeiro = merge_primeiro['nom_contato'].notna()
    matches_primeiro = merge_primeiro[mask_match_primeiro].copy()

    sem_match_primeiro = merge_primeiro[~mask_match_primeiro].copy()

    df_resposta_nome_tel = (
        df_resposta.sort_values('DT_ATENDIMENTO')
        .drop_duplicates(subset=['NOME_MANIPULADO', 'TELEFONE_CHAVE'], keep='last')
        [['NOME_MANIPULADO', 'TELEFONE_CHAVE', 'nom_contato', 'DT_ATENDIMENTO', 'num_telefone']]
        .copy()
    )

    merge_fallback = sem_match_primeiro.merge(
        df_resposta_nome_tel,
        on=['NOME_MANIPULADO', 'TELEFONE_CHAVE'],
        how='left',
    )
    mask_match_fallback = merge_fallback['nom_contato_y'].notna()
    matches_fallback = merge_fallback[mask_match_fallback].copy()

    match_contato_data = len(matches_primeiro)
    match_nome_tel = len(matches_fallback)

    total_status = len(df_status)

    print('=== COMPARACAO DE MATCHS ===')
    print(f'Total de registros no status_limpo: {total_status}')
    print('')
    print('Estrategia 1 - CONTATO + DATA:')
    print(f'Com match: {match_contato_data}')
    print(f'Sem match: {total_status - match_contato_data}')
    print('')
    print('Estrategia 2 - NOME_MANIPULADO + TELEFONE:')
    print(f'Com match: {match_nome_tel}')
    print(f'Sem match: {len(sem_match_primeiro) - match_nome_tel}')

    pasta_saida = Path('tests/outputs')
    pasta_saida.mkdir(parents=True, exist_ok=True)

    arquivo_primeiro = pasta_saida / 'matches_primeiro_contato_data.xlsx'
    arquivo_fallback = pasta_saida / 'matches_fallback_nome_telefone.xlsx'

    saida_primeiro = matches_primeiro[
        ['Contato', 'nom_contato', 'DT_ATENDIMENTO', 'DT ENVIO', 'Telefone', 'num_telefone']
    ].copy()

    saida_fallback = matches_fallback[
        ['Contato', 'nom_contato_y', 'DT_ATENDIMENTO_y', 'DT ENVIO', 'Telefone', 'num_telefone_y']
    ].copy()
    saida_fallback = saida_fallback.rename(
        columns={
            'nom_contato_y': 'nom_contato',
            'DT_ATENDIMENTO_y': 'DT_ATENDIMENTO',
            'num_telefone_y': 'num_telefone',
        }
    )

    saida_primeiro.to_excel(arquivo_primeiro, index=False)
    saida_fallback.to_excel(arquivo_fallback, index=False)

    print('')
    print(f'Arquivo gerado (primeiro match): {arquivo_primeiro}')
    print(f'Arquivo gerado (fallback): {arquivo_fallback}')


if __name__ == '__main__':
    executar_comparacao_matchs()
