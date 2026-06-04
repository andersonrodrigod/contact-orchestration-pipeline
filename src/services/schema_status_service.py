"""Contrato interno do arquivo de status."""


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


def normalizar_schema_status(df):
    colunas_fixas = [col for col in ORDEM_CANONICA_STATUS if col in df.columns]
    colunas_restantes = [col for col in df.columns if col not in colunas_fixas]
    if colunas_fixas:
        return df[colunas_fixas + colunas_restantes].copy()
    return df
