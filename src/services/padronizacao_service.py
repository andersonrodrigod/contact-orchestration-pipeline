"""Regras simples de padronizacao de nomes de colunas entre fontes."""

from src.services.schema_resposta_service import (
    normalizar_coluna_data_atendimento,
    normalizar_coluna_resposta,
)


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
    df = normalizar_coluna_data_atendimento(df, remover_alias=True)
    return normalizar_coluna_resposta(df, criar_vazia=True, remover_alias=True)
