import unittest

import pandas as pd

from src.services.dataset_metricas_service import aplicar_contagens_status


class DatasetMetricasServiceTests(unittest.TestCase):
    def test_aplicar_contagens_status_com_chave_canonica_no_join(self):
        df_saida = pd.DataFrame(
            [
                {
                    'CHAVE STATUS': '  JOSÉ   DA SILVA  ',
                }
            ]
        )
        df_status_full = pd.DataFrame(
            [
                {
                    'Contato': 'jose da silva',
                    'Telefone': '11 9999-0001',
                    'Status': 'Lida',
                    'RESPOSTA': 'Sim',
                },
                {
                    'Contato': 'José da Silva',
                    'Telefone': '11 9999-0002',
                    'Status': 'LIDA',
                    'RESPOSTA': '',
                },
                {
                    'Contato': 'JOSE DA SILVA',
                    'Telefone': '(11) 9999-0002',
                    'Status': 'Enviada',
                    'RESPOSTA': 'n/a',
                },
            ]
        )

        resultado = aplicar_contagens_status(df_saida, df_status_full)

        self.assertTrue(resultado['ok'])
        self.assertEqual(resultado['mensagens'], [])
        self.assertEqual(df_saida.loc[0, 'LIDA'], 2)
        self.assertEqual(df_saida.loc[0, 'ENVIADA'], 1)
        self.assertEqual(df_saida.loc[0, 'LIDA_RESPOSTA_SIM'], 1)
        self.assertEqual(df_saida.loc[0, 'LIDA_SEM_RESPOSTA'], 1)
        self.assertEqual(df_saida.loc[0, 'QT LIDA'], 2)
        self.assertEqual(df_saida.loc[0, 'QT ENVIADA'], 1)
        self.assertEqual(df_saida.loc[0, 'QT LIDA_RESPOSTA_SIM'], 1)
        self.assertEqual(df_saida.loc[0, 'QT LIDA_SEM_RESPOSTA'], 1)
        self.assertEqual(df_saida.loc[0, 'QT TELEFONES'], 2)


if __name__ == '__main__':
    unittest.main()
