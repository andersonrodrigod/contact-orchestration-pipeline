import unittest

import pandas as pd

from src.services.normalizacao_services import limpar_texto_colunas_alvo


class NormalizacaoColunasAlvoTests(unittest.TestCase):
    def test_aplica_somente_nas_colunas_alvo(self):
        df = pd.DataFrame(
            {
                'HSM': ['Pesquisa Complica├º├╡es Cirurgicas'],
                'Status': ['Mensagem nÏ€o entregue'],
                'Respondido': ['NÏ€o'],
                'resposta': ['NÏ€o'],
                'Outra': ['UsuÃŸrio'],
            }
        )

        saida = limpar_texto_colunas_alvo(
            df.copy(),
            colunas_alvo=['HSM', 'Status', 'Respondido', 'RESPOSTA'],
        )

        self.assertNotEqual(saida.loc[0, 'HSM'], df.loc[0, 'HSM'])
        self.assertNotEqual(saida.loc[0, 'Status'], df.loc[0, 'Status'])
        self.assertNotEqual(saida.loc[0, 'Respondido'], df.loc[0, 'Respondido'])
        self.assertNotEqual(saida.loc[0, 'resposta'], df.loc[0, 'resposta'])
        self.assertEqual(saida.loc[0, 'Outra'], df.loc[0, 'Outra'])

    def test_trata_alias_resposta_case_insensitive(self):
        df = pd.DataFrame(
            {
                'RESPOSTA': ['NÏ€o'],
                'resposta': ['NÏ€o'],
            }
        )

        saida = limpar_texto_colunas_alvo(
            df.copy(),
            colunas_alvo=['resposta'],
        )

        self.assertEqual(saida.loc[0, 'RESPOSTA'], 'Não')
        self.assertEqual(saida.loc[0, 'resposta'], 'Não')


if __name__ == '__main__':
    unittest.main()
