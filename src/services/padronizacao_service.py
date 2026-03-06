"""Regras simples de padronizacao de nomes de colunas entre fontes."""


ORDEM_CANONICA_STATUS = [
    'Conta',
    'HSM',
    'Mensagem',
    'Categoria',
    'Template',
    'Data do envio',
    'Status',
    'Respondido',
    'Protocolo',
    'Agendamento',
    'Data agendamento',
    'Status agendamento',
    'Campanha',
    'Agente',
    'Contato',
    'Telefone',
    'ID_Mailing',
]


def padronizar_colunas_status(df):
    # Mantem ordem canonica para garantir saidas consistentes entre CSV e XLSX.
    colunas_fixas = [col for col in ORDEM_CANONICA_STATUS if col in df.columns]
    colunas_restantes = [col for col in df.columns if col not in colunas_fixas]
    if colunas_fixas:
        return df[colunas_fixas + colunas_restantes].copy()
    return df


def padronizar_colunas_status_resposta(df):
    mapa_colunas = {
        'dat_atendimento': 'DT_ATENDIMENTO',
        'Resposta': 'resposta',
        'RESPOSTA': 'resposta',
    }
    df = df.rename(columns=mapa_colunas)
    if 'resposta' not in df.columns:
        df['resposta'] = ''
    return df
