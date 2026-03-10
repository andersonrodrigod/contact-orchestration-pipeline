import unittest

import pandas as pd

from src.services.schema_resposta_service import (
    COLUNA_RESPOSTA_CANONICA,
    diagnosticar_coluna_resposta,
    normalizar_coluna_resposta,
    tem_coluna_resposta,
)


class SchemaRespostaServiceTests(unittest.TestCase):
    def test_tem_coluna_resposta_com_alias(self):
        df = pd.DataFrame({'RESPOSTA': ['Sim']})
        self.assertTrue(tem_coluna_resposta(df))

    def test_normaliza_alias_para_canonica(self):
        df = pd.DataFrame({'Resposta': ['Não']})
        saida = normalizar_coluna_resposta(df.copy(), criar_vazia=True, remover_alias=True)

        self.assertIn(COLUNA_RESPOSTA_CANONICA, saida.columns)
        self.assertNotIn('Resposta', saida.columns)
        self.assertEqual(saida.loc[0, COLUNA_RESPOSTA_CANONICA], 'Não')

    def test_coalesce_resposta_prioriza_valor_nao_vazio(self):
        df = pd.DataFrame(
            {
                'resposta': [''],
                'Resposta': [''],
                'RESPOSTA': ['Sim'],
            }
        )
        saida = normalizar_coluna_resposta(df.copy(), criar_vazia=True, remover_alias=True)
        self.assertEqual(saida.loc[0, COLUNA_RESPOSTA_CANONICA], 'Sim')

    def test_diagnostico_coluna_resposta_detecta_conflito_entre_aliases(self):
        df = pd.DataFrame(
            {
                'resposta': ['Sim', 'Talvez', ''],
                'Resposta': ['Sim', 'Nao', ''],
                'RESPOSTA': ['', '', ''],
            }
        )
        diagnostico = diagnosticar_coluna_resposta(df)

        self.assertEqual(diagnostico['aliases_presentes'], ['resposta', 'Resposta', 'RESPOSTA'])
        self.assertEqual(diagnostico['qtd_aliases_presentes'], 3)
        self.assertEqual(diagnostico['qtd_linhas_conflito'], 1)


if __name__ == '__main__':
    unittest.main()
