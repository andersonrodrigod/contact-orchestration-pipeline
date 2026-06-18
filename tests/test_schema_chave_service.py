import unittest

import pandas as pd

from src.services.schema_chave_service import (
    COLUNA_CHAVE_SENHA,
    adicionar_chave_senha,
    extrair_chave_principal_valor,
)


class SchemaChaveServiceTests(unittest.TestCase):
    def test_extrai_chave_principal_do_ultimo_underscore(self):
        self.assertEqual(
            extrair_chave_principal_valor('ANDERSON_HOSPITAL_PROCEDIMENTO_SENHA'),
            'SENHA',
        )

    def test_adiciona_chave_senha_usando_primeira_coluna_disponivel(self):
        df = pd.DataFrame(
            {
                'Contato': ['ANDERSON_HOSPITAL_PROCEDIMENTO_SENHA'],
            }
        )

        saida = adicionar_chave_senha(df, ['SENHA', 'Contato'])

        self.assertEqual(saida.loc[0, COLUNA_CHAVE_SENHA], 'SENHA')

    def test_adiciona_chave_senha_em_coluna_propria(self):
        df = pd.DataFrame(
            {
                'Contato': ['ANDERSON_HOSPITAL_PROCEDIMENTO_SENHA123'],
            }
        )

        saida = adicionar_chave_senha(df, ['SENHA', 'Contato'])

        self.assertEqual(saida.loc[0, COLUNA_CHAVE_SENHA], 'SENHA123')

    def test_preenche_chave_senha_vazia_com_proxima_origem(self):
        df = pd.DataFrame(
            {
                COLUNA_CHAVE_SENHA: [''],
                'Contato': ['ANDERSON_HOSPITAL_PROCEDIMENTO_SENHA456'],
            }
        )

        saida = adicionar_chave_senha(df, [COLUNA_CHAVE_SENHA, 'Contato'])

        self.assertEqual(saida.loc[0, COLUNA_CHAVE_SENHA], 'SENHA456')

    def test_preserva_chave_senha_existente(self):
        df = pd.DataFrame(
            {
                COLUNA_CHAVE_SENHA: ['SENHA_EXISTENTE'],
                'Contato': ['ANDERSON_HOSPITAL_PROCEDIMENTO_SENHA456'],
            }
        )

        saida = adicionar_chave_senha(df, [COLUNA_CHAVE_SENHA, 'Contato'])

        self.assertEqual(saida.loc[0, COLUNA_CHAVE_SENHA], 'SENHA_EXISTENTE')


if __name__ == '__main__':
    unittest.main()
