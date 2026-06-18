import unittest

import pandas as pd

from src.utils.arquivos import _limpar_linhas_excel


class ArquivosUtilsTests(unittest.TestCase):
    def test_ler_excel_remove_linhas_totalmente_vazias(self):
        df = pd.DataFrame(
            [
                {'USUARIO': 'Ana', 'SENHA': 'SENHA001'},
                {'USUARIO': '', 'SENHA': ''},
                {'USUARIO': 'Bruno', 'SENHA': 'SENHA002'},
            ]
        )

        df = _limpar_linhas_excel(df)

        self.assertEqual(len(df), 2)
        self.assertEqual(list(df['USUARIO']), ['Ana', 'Bruno'])


if __name__ == '__main__':
    unittest.main()
