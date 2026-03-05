import os


LIMIAR_NAT_DATA_PADRAO = 30.0
ENV_LIMIAR_NAT_DATA = 'LIMIAR_NAT_DATA_PERCENT'


def resolver_limiar_nat_data(limiar_nat_data=None):
    if limiar_nat_data is not None:
        return float(limiar_nat_data), 'parametro'

    valor_env = os.getenv(ENV_LIMIAR_NAT_DATA)
    if valor_env is None or str(valor_env).strip() == '':
        return LIMIAR_NAT_DATA_PADRAO, 'default'

    try:
        limiar = float(valor_env)
    except ValueError:
        return LIMIAR_NAT_DATA_PADRAO, 'default_env_invalido'

    if limiar < 0:
        limiar = 0.0
    if limiar > 100:
        limiar = 100.0
    return limiar, 'env'
