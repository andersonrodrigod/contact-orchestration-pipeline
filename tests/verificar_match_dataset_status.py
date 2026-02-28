import re
from pathlib import Path

import pandas as pd


ARQUIVO_DATASET = 'src/data/arquivo_limpo/dataset_complicacao.xlsx'
ARQUIVO_STATUS_INTEGRADO = 'src/data/arquivo_limpo/status_complicacao_integrado.csv'
ARQUIVO_RELATORIO = 'tests/outputs/relatorio_fallback_chave_contato.xlsx'


def normalizar_texto(valor):
    return str(valor).strip().upper()


def normalizar_telefone(valor):
    texto = str(valor).strip()
    if texto.endswith('.0'):
        texto = texto[:-2]
    return re.sub(r'\D', '', texto)


def executar_teste_match():
    df_dataset = pd.read_excel(ARQUIVO_DATASET, dtype=str).fillna('')
    df_status = pd.read_csv(
        ARQUIVO_STATUS_INTEGRADO,
        sep=';',
        dtype=str,
        keep_default_na=False,
        encoding='utf-8-sig',
    )

    df_dataset['CHAVE RELATORIO'] = df_dataset['CHAVE RELATORIO'].astype(str).str.strip()
    df_status['Contato'] = df_status['Contato'].astype(str).str.strip()

    # 1) Match principal: CHAVE RELATORIO (dataset) x Contato (status integrado)
    chaves_status = set(df_status['Contato'])
    mask_match_principal = df_dataset['CHAVE RELATORIO'].isin(chaves_status)
    df_match_principal = df_dataset[mask_match_principal].copy()
    df_sem_match_principal = df_dataset[~mask_match_principal].copy()

    # 2) Fallback: USUARIO + qualquer TELEFONE 1..5 x NOME_MANIPULADO + Telefone
    df_status['NOME_MANIPULADO_NORM'] = df_status['NOME_MANIPULADO'].apply(normalizar_texto)
    df_status['TELEFONE_NORM'] = df_status['Telefone'].apply(normalizar_telefone)
    pares_status = set(zip(df_status['NOME_MANIPULADO_NORM'], df_status['TELEFONE_NORM']))

    df_sem_match_principal['USUARIO_NORM'] = df_sem_match_principal['USUARIO'].apply(normalizar_texto)
    colunas_telefone = ['TELEFONE 1', 'TELEFONE 2', 'TELEFONE 3', 'TELEFONE 4', 'TELEFONE 5']
    for col in colunas_telefone:
        df_sem_match_principal[f'{col}_NORM'] = df_sem_match_principal[col].apply(normalizar_telefone)

    def tem_match_fallback(linha):
        usuario = linha['USUARIO_NORM']
        for col in colunas_telefone:
            tel = linha[f'{col}_NORM']
            if tel and (usuario, tel) in pares_status:
                return True
        return False

    mask_fallback = df_sem_match_principal.apply(tem_match_fallback, axis=1)
    df_match_fallback = df_sem_match_principal[mask_fallback].copy()
    df_sem_match_total = df_sem_match_principal[~mask_fallback].copy()

    # Relatorio do fallback: tabela pareada dataset x status
    pares_relatorio = []
    for _, linha_ds in df_match_fallback.iterrows():
        usuario = linha_ds['USUARIO_NORM']
        chave_relatorio = linha_ds['CHAVE RELATORIO']

        for col in colunas_telefone:
            telefone = linha_ds[f'{col}_NORM']
            if not telefone:
                continue

            df_status_match = df_status[
                (df_status['NOME_MANIPULADO_NORM'] == usuario)
                & (df_status['TELEFONE_NORM'] == telefone)
            ]

            if df_status_match.empty:
                continue

            for _, linha_st in df_status_match.iterrows():
                pares_relatorio.append({
                    'CHAVE RELATORIO': chave_relatorio,
                    'USUARIO': linha_ds['USUARIO'],
                    'TELEFONE DATASET MATCH': telefone,
                    'Contato': linha_st['Contato'],
                    'NOME_MANIPULADO': linha_st['NOME_MANIPULADO'],
                    'Telefone STATUS': linha_st['Telefone'],
                })

    df_pares_fallback = pd.DataFrame(pares_relatorio).drop_duplicates()
    contatos_fallback = set(df_pares_fallback['Contato']) if not df_pares_fallback.empty else set()

    df_fallback_dataset = df_match_fallback[
        ['CHAVE RELATORIO', 'USUARIO', 'TELEFONE 1', 'TELEFONE 2', 'TELEFONE 3', 'TELEFONE 4', 'TELEFONE 5']
    ].copy()

    df_fallback_status = df_status[df_status['Contato'].isin(contatos_fallback)][
        ['Contato', 'NOME_MANIPULADO', 'Telefone', 'DT ENVIO', 'RESPOSTA']
    ].copy()

    Path('tests/outputs').mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(ARQUIVO_RELATORIO, engine='openpyxl') as writer:
        df_fallback_dataset.to_excel(writer, sheet_name='fallback_dataset', index=False)
        df_fallback_status.to_excel(writer, sheet_name='fallback_status', index=False)
        df_pares_fallback.to_excel(writer, sheet_name='fallback_pares', index=False)

    total_dataset = len(df_dataset)
    total_match_principal = len(df_match_principal)
    total_match_fallback = len(df_match_fallback)
    total_match_geral = total_match_principal + total_match_fallback
    total_sem_match = len(df_sem_match_total)

    print('=== RESUMO DE MATCH ===')
    print(f'Total dataset: {total_dataset}')
    print(f'Match principal (CHAVE RELATORIO x Contato): {total_match_principal}')
    print(f'Match fallback (USUARIO+TELEFONE x NOME_MANIPULADO+Telefone): {total_match_fallback}')
    print(f'Total com match (principal + fallback): {total_match_geral}')
    print(f'Total sem match: {total_sem_match}')
    print(f'Relatorio fallback gerado em: {ARQUIVO_RELATORIO}')


if __name__ == '__main__':
    executar_teste_match()
