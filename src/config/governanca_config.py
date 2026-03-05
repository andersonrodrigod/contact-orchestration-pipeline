import os


LIMIAR_NAT_DATA_PADRAO = 30.0
ENV_LIMIAR_NAT_DATA = 'LIMIAR_NAT_DATA_PERCENT'
ENV_LIMIAR_NAT_DATA_COMPLICACAO = 'LIMIAR_NAT_DATA_PERCENT_COMPLICACAO'
ENV_LIMIAR_NAT_DATA_INTERNACAO_ELETIVO = 'LIMIAR_NAT_DATA_PERCENT_INTERNACAO_ELETIVO'
ENV_PERMITIR_OVERRIDE_LIMIAR = 'PERMITIR_OVERRIDE_LIMIAR_NAT_DATA'

LIMIAR_NAT_DATA_PADRAO_POR_CONTEXTO = {
    'complicacao': 30.0,
    'internacao_eletivo': 30.0,
}

ENV_LIMIAR_POR_CONTEXTO = {
    'complicacao': ENV_LIMIAR_NAT_DATA_COMPLICACAO,
    'internacao_eletivo': ENV_LIMIAR_NAT_DATA_INTERNACAO_ELETIVO,
}


def _aplicar_limites_limiar(limiar):
    if limiar < 0:
        limiar = 0.0
    if limiar > 100:
        limiar = 100.0
    return limiar


def _parse_bool_env(nome_env, default=True):
    valor = os.getenv(nome_env)
    if valor is None:
        return default
    valor_norm = str(valor).strip().lower()
    if valor_norm in ['1', 'true', 't', 'yes', 'y', 'sim', 's']:
        return True
    if valor_norm in ['0', 'false', 'f', 'no', 'n', 'nao']:
        return False
    return default


def _resolver_limiar_por_env(nome_env):
    valor_env = os.getenv(nome_env)
    if valor_env is None or str(valor_env).strip() == '':
        return None
    try:
        return _aplicar_limites_limiar(float(valor_env))
    except ValueError:
        return None


def resolver_limiar_nat_data(limiar_nat_data=None, contexto=None, permitir_override_limiar=None):
    contexto_norm = str(contexto).strip().lower() if contexto else None
    if permitir_override_limiar is None:
        permitir_override_limiar = _parse_bool_env(ENV_PERMITIR_OVERRIDE_LIMIAR, default=True)

    if limiar_nat_data is not None and permitir_override_limiar:
        return _aplicar_limites_limiar(float(limiar_nat_data)), 'parametro'

    limiar_default_contexto = LIMIAR_NAT_DATA_PADRAO_POR_CONTEXTO.get(
        contexto_norm, LIMIAR_NAT_DATA_PADRAO
    )

    if contexto_norm in ENV_LIMIAR_POR_CONTEXTO:
        limiar_env_contexto = _resolver_limiar_por_env(ENV_LIMIAR_POR_CONTEXTO[contexto_norm])
        if limiar_env_contexto is not None:
            return limiar_env_contexto, 'env_contexto'

    limiar_env_global = _resolver_limiar_por_env(ENV_LIMIAR_NAT_DATA)
    if limiar_env_global is not None:
        return limiar_env_global, 'env_global'

    if limiar_nat_data is not None and not permitir_override_limiar:
        return limiar_default_contexto, 'override_parametro_bloqueado'

    if contexto_norm in LIMIAR_NAT_DATA_PADRAO_POR_CONTEXTO:
        return limiar_default_contexto, 'default_contexto'
    return limiar_default_contexto, 'default'
