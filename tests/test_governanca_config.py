import os
import unittest
from unittest.mock import patch

from src.config.governanca_config import resolver_limiar_nat_data


class GovernancaConfigTests(unittest.TestCase):
    def test_resolver_limiar_nat_data_com_parametro(self):
        limiar, origem = resolver_limiar_nat_data(42.5, contexto='complicacao')
        self.assertEqual(limiar, 42.5)
        self.assertEqual(origem, 'parametro')

    @patch.dict(os.environ, {'LIMIAR_NAT_DATA_PERCENT_COMPLICACAO': '22.0'}, clear=False)
    def test_resolver_limiar_nat_data_por_env_contexto(self):
        limiar, origem = resolver_limiar_nat_data(None, contexto='complicacao')
        self.assertEqual(limiar, 22.0)
        self.assertEqual(origem, 'env_contexto')

    @patch.dict(os.environ, {'PERMITIR_OVERRIDE_LIMIAR_NAT_DATA': '0'}, clear=False)
    def test_resolver_limiar_nat_data_bloqueia_override_quando_parametro_desabilitado(self):
        limiar, origem = resolver_limiar_nat_data(
            55.0,
            contexto='internacao_eletivo',
            permitir_override_limiar=False,
        )
        self.assertEqual(limiar, 30.0)
        self.assertEqual(origem, 'override_parametro_bloqueado')


if __name__ == '__main__':
    unittest.main()
