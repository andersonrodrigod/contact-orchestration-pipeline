"""Compatibilidade retroativa (DEPRECADO).

Este modulo foi substituido por ``src.services.orquestracao_service``.
Ele e mantido apenas como ponte de migracao e deve ser removido apos a
janela de compatibilidade.
"""

from src.services.orquestracao_service import (
    ABAS_OBRIGATORIAS_FINALIZACAO,
    COLUNAS_DISPARO,
    ERRO_ORQUESTRACAO,
    MARCADOR_ACAO_PRIORIDADE,
    MARCADOR_ACAO_PROXIMO,
    PROCESSOS_PERMITIDOS_DISPARO,
    PROCESSO_RESOLVIDO,
    VALOR_SEM_TELEFONE_DISPONIVEL,
    aplicar_classificacao_processo_acao,
    gerar_dataset_final,
    normalizar_telefone,
    normalizar_texto_serie,
    orquestrar_usuarios_respondidos,
    pd,
    warnings,
)

__all__ = [
    "ABAS_OBRIGATORIAS_FINALIZACAO",
    "COLUNAS_DISPARO",
    "ERRO_ORQUESTRACAO",
    "MARCADOR_ACAO_PRIORIDADE",
    "MARCADOR_ACAO_PROXIMO",
    "PROCESSOS_PERMITIDOS_DISPARO",
    "PROCESSO_RESOLVIDO",
    "VALOR_SEM_TELEFONE_DISPONIVEL",
    "aplicar_classificacao_processo_acao",
    "gerar_dataset_final",
    "normalizar_telefone",
    "normalizar_texto_serie",
    "orquestrar_usuarios_respondidos",
    "pd",
    "warnings",
]
