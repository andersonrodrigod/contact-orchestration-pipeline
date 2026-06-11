import unittest

import pandas as pd

from src.services.dataset_metricas_service import aplicar_contagens_status
from src.services.schema_chave_service import COLUNA_CHAVE_SENHA


class DatasetMetricasServiceTests(unittest.TestCase):
    def test_aplicar_contagens_status_com_chave_principal_no_join(self):
        df_saida = pd.DataFrame([{COLUNA_CHAVE_SENHA: 'SENHA001'}])
        df_status_full = pd.DataFrame(
            [
                {
                    COLUNA_CHAVE_SENHA: 'SENHA001',
                    'Contato': 'jose da silva',
                    'Telefone': '11 9999-0001',
                    'Status': 'Lida',
                    'RESPOSTA': 'Sim',
                },
                {
                    COLUNA_CHAVE_SENHA: 'SENHA001',
                    'Contato': 'Jose da Silva',
                    'Telefone': '11 9999-0002',
                    'Status': 'LIDA',
                    'RESPOSTA': '',
                },
                {
                    COLUNA_CHAVE_SENHA: 'SENHA001',
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
