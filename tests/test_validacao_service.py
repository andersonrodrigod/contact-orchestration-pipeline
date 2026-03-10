import unittest

import pandas as pd

from src.services.validacao_service import validar_colunas_origem_para_padronizacao


class ValidacaoServiceTests(unittest.TestCase):
    def _df_status_minimo(self):
        return pd.DataFrame(
            [
                {
                    'Data agendamento': '01/01/2026',
                    'HSM': 'Pesquisa Complicacoes Cirurgicas',
                    'Status': 'ENVIADA',
                    'Respondido': 'NAO',
                    'Contato': 'usuario_1',
                    'Telefone': '11999999999',
                }
            ]
        )

    def test_validacao_registra_diagnostico_alias_resposta(self):
        df_status = self._df_status_minimo()
        df_status_resposta = pd.DataFrame(
            [
                {
                    'nom_contato': 'usuario_1',
                    'DT_ATENDIMENTO': '01/01/2026',
                    'RESPOSTA': 'Sim',
                }
            ]
        )

        resultado = validar_colunas_origem_para_padronizacao(df_status, df_status_resposta)
        mensagens = '\n'.join(resultado['mensagens'])

        self.assertTrue(resultado['ok'])
        self.assertIn('Diagnostico coluna resposta no status_resposta', mensagens)
        self.assertIn("aliases_presentes=['RESPOSTA']", mensagens)
        self.assertIn("Diagnostico data atendimento no status_resposta: aliases_presentes=['DT_ATENDIMENTO']", mensagens)
        self.assertEqual(resultado.get('warnings_alias_resposta_legado'), 1)

    def test_validacao_registra_aviso_quando_aliases_conflitam(self):
        df_status = self._df_status_minimo()
        df_status_resposta = pd.DataFrame(
            [
                {
                    'nom_contato': 'usuario_1',
                    'DT_ATENDIMENTO': '01/01/2026',
                    'resposta': 'Sim',
                    'Resposta': 'Nao',
                },
                {
                    'nom_contato': 'usuario_2',
                    'DT_ATENDIMENTO': '01/01/2026',
                    'resposta': 'Sim',
                    'Resposta': 'Sim',
                },
            ]
        )

        resultado = validar_colunas_origem_para_padronizacao(df_status, df_status_resposta)
        mensagens = '\n'.join(resultado['mensagens'])

        self.assertTrue(resultado['ok'])
        self.assertIn('Aviso: conflito detectado entre aliases de resposta no status_resposta.', mensagens)
        self.assertIn('linhas_com_valores_distintos=1', mensagens)

    def test_validacao_falha_em_modo_estrito_quando_alias_legado_presente(self):
        df_status = self._df_status_minimo()
        df_status_resposta = pd.DataFrame(
            [
                {
                    'nom_contato': 'usuario_1',
                    'DT_ATENDIMENTO': '01/01/2026',
                    'RESPOSTA': 'Sim',
                }
            ]
        )

        resultado = validar_colunas_origem_para_padronizacao(
            df_status,
            df_status_resposta,
            modo_estrito_alias_resposta=True,
            janela_corte_alias_resposta_ciclos=4,
        )
        mensagens = '\n'.join(resultado['mensagens'])

        self.assertFalse(resultado['ok'])
        self.assertEqual(resultado.get('warnings_alias_resposta_legado'), 1)
        self.assertIn('Modo estrito de alias de resposta habilitado', mensagens)
        self.assertIn('0 warning por 4 ciclos consecutivos', mensagens)

    def test_validacao_falha_em_modo_estrito_data_atendimento_quando_alias_legado_presente(self):
        df_status = self._df_status_minimo()
        df_status_resposta = pd.DataFrame(
            [
                {
                    'nom_contato': 'usuario_1',
                    'dat_atendimento': '01/01/2026',
                    'resposta': 'Sim',
                }
            ]
        )

        resultado = validar_colunas_origem_para_padronizacao(
            df_status,
            df_status_resposta,
            modo_estrito_data_atendimento=True,
        )
        mensagens = '\n'.join(resultado['mensagens'])

        self.assertFalse(resultado['ok'])
        self.assertIn('Modo estrito de data de atendimento habilitado', mensagens)
        self.assertIn("aliases_legados=['dat_atendimento']", mensagens)


if __name__ == '__main__':
    unittest.main()
