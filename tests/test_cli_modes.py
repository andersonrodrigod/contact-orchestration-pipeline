import unittest

import main
from src.cli.modos_principais import obter_registro_modos


class CliModesTests(unittest.TestCase):
    def test_registro_modos_tem_fluxos_principais_e_etapas(self):
        modos = obter_registro_modos()

        for modo in [
            'complicacao_com_resposta',
            'complicacao',
            'complicacao_gerar_status_dataset',
            'complicacao_orquestracao',
            'preflight_complicacao',
            'complicacao_ingestao',
            'complicacao_integrar_status_resposta',
            'complicacao_criar_dataset_status',
            'complicacao_gerar_dataset_status',
            'complicacao_orquestrar',
        ]:
            self.assertIn(modo, modos)
            self.assertTrue(callable(modos[modo]))

    def test_aliases_individuais_nao_estao_no_contrato_cli(self):
        modos = obter_registro_modos()

        for modo in [
            'individual_ingestao_complicacao',
            'individual_enviar_status_complicacao',
            'individual_criar_dataset_complicacao',
            'individual_gerar_dataset_complicacao_com_resposta',
            'individual_orquestrar_complicacao',
        ]:
            self.assertNotIn(modo, modos)

    def test_somente_status_e_allow_nao_estao_no_contrato_cli(self):
        modos = obter_registro_modos()

        self.assertNotIn('complicacao_somente_status', modos)
        self.assertFalse(hasattr(main, 'ALLOW_MODOS_INDIVIDUAIS'))


if __name__ == '__main__':
    unittest.main()
