import unittest

import pandas as pd

from src.services.normalizacao_services import limpar_texto_colunas_alvo


class NormalizacaoColunasAlvoTests(unittest.TestCase):
    def test_aplica_somente_nas_colunas_alvo(self):
        df = pd.DataFrame(
            {
                'HSM': ['Pesquisa Complica\u00ef\u00bf\u00bd\u00ef\u00bf\u00bdes Cirurgicas'],
                'Status': ['Mensagem n\u03c0o entregue'],
                'Respondido': ['N\u03c0o'],
                'resposta': ['N\u03c0o'],
                'Outra': ['Usu\u00df\u00e1rio'],
            }
        )

        saida = limpar_texto_colunas_alvo(
            df.copy(),
            colunas_alvo=['HSM', 'Status', 'Respondido', 'RESPOSTA'],
        )

        self.assertNotEqual(saida.loc[0, 'Status'], df.loc[0, 'Status'])
        self.assertNotEqual(saida.loc[0, 'Respondido'], df.loc[0, 'Respondido'])
        self.assertNotEqual(saida.loc[0, 'resposta'], df.loc[0, 'resposta'])
        self.assertEqual(saida.loc[0, 'Outra'], df.loc[0, 'Outra'])

    def test_trata_alias_resposta_case_insensitive(self):
        df = pd.DataFrame(
            {
                'RESPOSTA': ['N\u03c0o'],
                'resposta': ['N\u03c0o'],
            }
        )

        saida = limpar_texto_colunas_alvo(
            df.copy(),
            colunas_alvo=['resposta'],
        )

        self.assertEqual(saida.loc[0, 'RESPOSTA'], 'N\u00e3o')
        self.assertEqual(saida.loc[0, 'resposta'], 'N\u00e3o')

    def test_preserva_nan_nas_colunas_alvo(self):
        df = pd.DataFrame(
            {
                'Status': [None, 'N\u03c0o'],
            }
        )

        saida = limpar_texto_colunas_alvo(
            df.copy(),
            colunas_alvo=['Status'],
        )

        self.assertTrue(pd.isna(saida.loc[0, 'Status']))
        self.assertEqual(saida.loc[1, 'Status'], 'N\u00e3o')


if __name__ == '__main__':
    unittest.main()
