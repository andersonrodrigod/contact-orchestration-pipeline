import unittest

import pandas as pd

from src.services.schema_chave_service import (
    COLUNA_CHAVE_PRINCIPAL,
    adicionar_chave_principal,
    extrair_chave_principal_valor,
)


class SchemaChaveServiceTests(unittest.TestCase):
    def test_extrai_chave_principal_do_ultimo_underscore(self):
        self.assertEqual(
            extrair_chave_principal_valor('ANDERSON_HOSPITAL_PROCEDIMENTO_SENHA'),
            'SENHA',
        )

    def test_adiciona_chave_principal_usando_primeira_coluna_disponivel(self):
        df = pd.DataFrame(
            {
                'Contato': ['ANDERSON_HOSPITAL_PROCEDIMENTO_SENHA'],
            }
        )

        saida = adicionar_chave_principal(df, ['CHAVE', 'Contato'])

        self.assertEqual(saida.loc[0, COLUNA_CHAVE_PRINCIPAL], 'SENHA')


if __name__ == '__main__':
    unittest.main()
