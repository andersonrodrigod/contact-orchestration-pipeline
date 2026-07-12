from src.services.schema_resposta_service import (
    garantir_contrato_resposta_canonica,
    normalizar_coluna_resposta,
)
from src.services.schema_chave_service import (
    COLUNA_CHAVE_SENHA,
    adicionar_chave_senha,
)
from src.utils.arquivos import ler_arquivo_csv, salvar_dataframe


COLUNA_REFERENCIA_ORDEM_STATUS = 'ID_Mailing'
COLUNAS_APOS_ID_MAILING_STATUS = [
    'DT ENVIO',
    'RESPOSTA',
    'NOME_MANIPULADO',
    'SENHA',
]


def _ordenar_colunas_status_integrado(df):
    for coluna in COLUNAS_APOS_ID_MAILING_STATUS:
        if coluna not in df.columns:
            df[coluna] = ''

    if COLUNA_REFERENCIA_ORDEM_STATUS not in df.columns:
        return df

    colunas_atuais = list(df.columns)
    indice_referencia = colunas_atuais.index(COLUNA_REFERENCIA_ORDEM_STATUS)
    colunas_ate_referencia = colunas_atuais[: indice_referencia + 1]
    colunas_restantes = [
        coluna
        for coluna in colunas_atuais
        if coluna not in colunas_ate_referencia
        and coluna not in COLUNAS_APOS_ID_MAILING_STATUS
    ]
    return df[colunas_ate_referencia + COLUNAS_APOS_ID_MAILING_STATUS + colunas_restantes].copy()


def _preparar_senha_saida_status(df):
    if COLUNA_CHAVE_SENHA in df.columns:
        df['SENHA'] = df[COLUNA_CHAVE_SENHA].fillna('').astype(str).str.strip()
    elif 'SENHA' not in df.columns:
        df['SENHA'] = ''
    return df.drop(columns=[COLUNA_CHAVE_SENHA], errors='ignore')


def integrar_status_com_resposta(
    arquivo_status='data/disparo_complicacao/saida/status_limpo.csv',
    arquivo_status_resposta='data/disparo_complicacao/saida/status_resposta_limpo.csv',
    arquivo_saida='data/disparo_complicacao/saida/status.csv',
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

    df_status = adicionar_chave_senha(df_status, ['SENHA', COLUNA_CHAVE_SENHA, 'Contato'])
    df_resposta = adicionar_chave_senha(df_resposta, ['SENHA', COLUNA_CHAVE_SENHA, 'nom_contato'])
    df_resposta = df_resposta[df_resposta[COLUNA_CHAVE_SENHA] != ''].copy()

    df_resposta = (
        df_resposta.sort_values(COLUNA_CHAVE_SENHA)
        .drop_duplicates(subset=[COLUNA_CHAVE_SENHA], keep='last')
    )

    df_merge = df_status.merge(
        df_resposta[['nom_contato', COLUNA_CHAVE_SENHA, 'resposta']],
        on=COLUNA_CHAVE_SENHA,
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

    df_merge = df_merge.drop(
        columns=['nom_contato', 'resposta'],
        errors='ignore',
    )
    df_merge = _preparar_senha_saida_status(df_merge)
    df_merge = _ordenar_colunas_status_integrado(df_merge)

    salvar_dataframe(df_merge, arquivo_saida)

    return {
        'ok': True,
        'arquivo_saida': arquivo_saida,
    }
