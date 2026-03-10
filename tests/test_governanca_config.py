import os
import unittest
from unittest.mock import patch

from src.config.governanca_config import (
    resolver_janela_corte_alias_resposta,
    resolver_limiar_nat_data,
    resolver_modo_estrito_data_atendimento,
    resolver_modo_estrito_alias_resposta,
)


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

    @patch.dict(os.environ, {'MODO_ESTRITO_ALIAS_RESPOSTA': '1'}, clear=False)
    def test_resolver_modo_estrito_alias_resposta_por_env(self):
        modo, origem = resolver_modo_estrito_alias_resposta()
        self.assertTrue(modo)
        self.assertEqual(origem, 'env_ou_default')

    def test_resolver_modo_estrito_alias_resposta_por_parametro(self):
        modo, origem = resolver_modo_estrito_alias_resposta(False)
        self.assertFalse(modo)
        self.assertEqual(origem, 'parametro')

    @patch.dict(os.environ, {'JANELA_CORTE_ALIAS_RESPOSTA_CICLOS': '5'}, clear=False)
    def test_resolver_janela_corte_alias_resposta_por_env(self):
        janela, origem = resolver_janela_corte_alias_resposta()
        self.assertEqual(janela, 5)
        self.assertEqual(origem, 'env')

    @patch.dict(os.environ, {'MODO_ESTRITO_DATA_ATENDIMENTO': '1'}, clear=False)
    def test_resolver_modo_estrito_data_atendimento_por_env(self):
        modo, origem = resolver_modo_estrito_data_atendimento()
        self.assertTrue(modo)
        self.assertEqual(origem, 'env_ou_default')


if __name__ == '__main__':
    unittest.main()
