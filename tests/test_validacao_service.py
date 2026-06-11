import unittest

import pandas as pd

from src.services.validacao_service import (
    validar_colunas_origem_dataset_complicacao,
    validar_colunas_origem_para_padronizacao,
)


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

    def test_validacao_aceita_alias_resposta_legado(self):
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
        self.assertNotIn('warnings_alias_resposta_legado', resultado)

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

    def test_validacao_aceita_alias_data_atendimento_legado(self):
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

        resultado = validar_colunas_origem_para_padronizacao(df_status, df_status_resposta)
        mensagens = '\n'.join(resultado['mensagens'])

        self.assertTrue(resultado['ok'])
        self.assertIn("aliases_presentes=['dat_atendimento']", mensagens)
        self.assertNotIn('modo_estrito_data_atendimento', resultado)

    def test_validacao_falha_quando_resposta_ausente_no_status_resposta(self):
        df_status = self._df_status_minimo()
        df_status_resposta = pd.DataFrame(
            [
                {
                    'nom_contato': 'usuario_1',
                    'DT_ATENDIMENTO': '01/01/2026',
                }
            ]
        )

        resultado = validar_colunas_origem_para_padronizacao(df_status, df_status_resposta)
        mensagens = '\n'.join(resultado['mensagens'])

        self.assertFalse(resultado['ok'])
        self.assertIn('Arquivo status_resposta com estrutura alterada.', mensagens)
        self.assertIn('resposta (ou alias legado Resposta/RESPOSTA)', mensagens)

    def test_validacao_dataset_exige_senha_como_chave(self):
        colunas = [
            'BASE',
            'COD USUARIO',
            'USUARIO',
            'TELEFONE 1',
            'TELEFONE 2',
            'TELEFONE 3',
            'TELEFONE 4',
            'TELEFONE 5',
            'PRESTADOR',
            'PROCEDIMENTO',
            'TP ATENDIMENTO',
            'DT INTERNACAO',
            'DT ENVIO',
            'SENHA',
            'STATUS',
            'P1',
        ]

        resultado = validar_colunas_origem_dataset_complicacao(colunas, contexto='complicacao')

        self.assertTrue(resultado['ok'])
        self.assertEqual(resultado['colunas_faltando'], [])

    def test_validacao_dataset_nao_aceita_chave_no_lugar_de_senha(self):
        colunas = [
            'BASE',
            'COD USUARIO',
            'USUARIO',
            'TELEFONE 1',
            'TELEFONE 2',
            'TELEFONE 3',
            'TELEFONE 4',
            'TELEFONE 5',
            'PRESTADOR',
            'PROCEDIMENTO',
            'TP ATENDIMENTO',
            'DT INTERNACAO',
            'DT ENVIO',
            'CHAVE',
            'STATUS',
            'P1',
        ]

        resultado = validar_colunas_origem_dataset_complicacao(colunas, contexto='complicacao')
        mensagens = '\n'.join(resultado['mensagens'])

        self.assertFalse(resultado['ok'])
        self.assertIn('SENHA', resultado['colunas_faltando'])
        self.assertIn('Colunas faltando', mensagens)


if __name__ == '__main__':
    unittest.main()
