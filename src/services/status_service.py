from src.services.schema_resposta_service import (
    garantir_contrato_resposta_canonica,
    normalizar_coluna_resposta,
)
from src.services.schema_chave_service import (
    COLUNA_CHAVE_PRINCIPAL,
    adicionar_chave_principal,
)
from src.utils.arquivos import ler_arquivo_csv, salvar_dataframe


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

    colunas_status_obrigatorias = ['Contato']
    colunas_resposta_obrigatorias = ['nom_contato', 'resposta']

    faltando_status = [c for c in colunas_status_obrigatorias if c not in df_status.columns]
    faltando_resposta = [c for c in colunas_resposta_obrigatorias if c not in df_resposta.columns]
    if faltando_status or faltando_resposta:
        raise ValueError(
            f'Colunas faltando para integracao. status={faltando_status} resposta={faltando_resposta}'
        )

    df_status['Contato'] = df_status['Contato'].astype(str).str.strip()
    df_resposta['nom_contato'] = df_resposta['nom_contato'].astype(str).str.strip()

    df_status = adicionar_chave_principal(df_status, ['CHAVE', 'Contato'])
    df_resposta = adicionar_chave_principal(df_resposta, ['CHAVE', 'nom_contato'])
    df_resposta = df_resposta[df_resposta[COLUNA_CHAVE_PRINCIPAL] != ''].copy()

    df_resposta = (
        df_resposta.sort_values(COLUNA_CHAVE_PRINCIPAL)
        .drop_duplicates(subset=[COLUNA_CHAVE_PRINCIPAL], keep='last')
    )

    df_merge = df_status.merge(
        df_resposta[['nom_contato', COLUNA_CHAVE_PRINCIPAL, 'resposta']],
        on=COLUNA_CHAVE_PRINCIPAL,
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
        columns=['nom_contato', 'resposta'],
        errors='ignore',
    )

    salvar_dataframe(df_merge, arquivo_saida)

    return {
        'ok': True,
        'arquivo_saida': arquivo_saida,
        'total_status': len(df_merge),
        'com_match': match,
        'sem_match': sem_match,
        'descartados_status_data_invalida': 0,
        'descartados_resposta_data_invalida': 0,
    }
