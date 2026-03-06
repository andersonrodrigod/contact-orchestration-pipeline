from dataclasses import dataclass


@dataclass(frozen=True)
class IntegracaoContexto:
    nome: str
    hsms_permitidos: tuple[str, ...]
    colunas_limpar: tuple[str, ...]


COLUNAS_LIMPAR_PADRAO = (
    'Conta',
    'Mensagem',
    'Categoria',
    'Template',
    'Protocolo',
    'Agendamento',
    'Status agendamento',
    'Campanha',
    'Agente',
)


CONTEXTO_INTEGRACAO_COMPLICACAO = IntegracaoContexto(
    nome='complicacao',
    hsms_permitidos=('Pesquisa Complicacoes Cirurgicas', 'Pesquisa ComplicaÃ§Ãµes Cirurgicas'),
    colunas_limpar=COLUNAS_LIMPAR_PADRAO,
)

CONTEXTO_INTEGRACAO_INTERNACAO_ELETIVO = IntegracaoContexto(
    nome='internacao_eletivo',
    hsms_permitidos=('Pesquisa_Pos_cir_urg_intern', 'Pesquisa_Pos_cir_eletivo'),
    colunas_limpar=COLUNAS_LIMPAR_PADRAO,
)
