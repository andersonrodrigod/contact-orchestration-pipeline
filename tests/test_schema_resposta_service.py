import unittest

import pandas as pd

from src.services.schema_resposta_service import (
    COLUNA_DATA_ATENDIMENTO_CANONICA,
    COLUNA_RESPOSTA_CANONICA,
    colunas_data_atendimento_presentes,
    diagnosticar_coluna_resposta,
    garantir_contrato_resposta_canonica,
    normalizar_coluna_data_atendimento,
    normalizar_coluna_resposta,
    tem_coluna_data_atendimento,
    tem_coluna_resposta,
)


class SchemaRespostaServiceTests(unittest.TestCase):
    def test_tem_coluna_data_atendimento_com_alias(self):
        df = pd.DataFrame({'dat_atendimento': ['01/01/2026']})
        self.assertTrue(tem_coluna_data_atendimento(df))

    def test_normaliza_alias_data_atendimento_para_canonica(self):
        df = pd.DataFrame({'dat_atendimento': ['01/01/2026']})
        saida = normalizar_coluna_data_atendimento(df.copy(), remover_alias=True)
        self.assertIn(COLUNA_DATA_ATENDIMENTO_CANONICA, saida.columns)
        self.assertNotIn('dat_atendimento', saida.columns)
        self.assertEqual(colunas_data_atendimento_presentes(saida), [COLUNA_DATA_ATENDIMENTO_CANONICA])

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

    def test_contrato_canonico_falha_quando_alias_legado_esta_presente(self):
        df = pd.DataFrame({'resposta': ['Sim'], 'RESPOSTA': ['Nao']})
        with self.assertRaisesRegex(ValueError, 'aliases legados presentes'):
            garantir_contrato_resposta_canonica(df, contexto='teste')

    def test_contrato_canonico_falha_quando_coluna_canonica_ausente(self):
        df = pd.DataFrame({'RESPOSTA': ['Sim']})
        with self.assertRaisesRegex(ValueError, 'coluna obrigatoria ausente'):
            garantir_contrato_resposta_canonica(df, contexto='teste')


if __name__ == '__main__':
    unittest.main()
