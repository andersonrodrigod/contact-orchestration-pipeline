import shutil
import unittest
import uuid
from pathlib import Path

import pandas as pd

from src.services.status_service import integrar_status_com_resposta
from src.utils.arquivos import ler_arquivo_csv


def _salvar_csv(df, caminho):
    df.to_csv(caminho, sep=';', index=False, encoding='utf-8-sig')


class StatusServiceTests(unittest.TestCase):
    def _criar_pasta_tmp_teste(self):
        base = Path('tests/outputs/tmp')
        base.mkdir(parents=True, exist_ok=True)
        pasta = base / f'status_service_{uuid.uuid4().hex}'
        pasta.mkdir(parents=True, exist_ok=True)
        return pasta

    def _limpar_pasta_tmp_teste(self, pasta):
        if pasta and pasta.exists():
            shutil.rmtree(pasta, ignore_errors=True)

    def test_integracao_usa_chave_parametro_em_vez_de_data(self):
        base = self._criar_pasta_tmp_teste()
        try:
            arq_status = base / 'status.csv'
            arq_resposta = base / 'status_resposta.csv'
            arq_saida = base / 'status_integrado.csv'

            df_status = pd.DataFrame(
                [
                    {
                        'Contato': 'ana_hospital_procedimento_46114_SENHA001',
                        'DT ENVIO': 'data ruim',
                        'Status': 'ENVIADA',
                    },
                    {
                        'Contato': 'bruno_hospital_procedimento_46114_SENHA002',
                        'DT ENVIO': 'outra data ruim',
                        'Status': 'ENVIADA',
                    },
                ]
            )
            df_resposta = pd.DataFrame(
                [
                    {
                        'nom_contato': 'ana_hospital_procedimento_46112_SENHA001',
                        'DT_ATENDIMENTO': '01/01/2026',
                        'resposta': 'Sim',
                    }
                ]
            )

            _salvar_csv(df_status, arq_status)
            _salvar_csv(df_resposta, arq_resposta)

            resultado = integrar_status_com_resposta(
                arquivo_status=str(arq_status),
                arquivo_status_resposta=str(arq_resposta),
                arquivo_saida=str(arq_saida),
            )

            self.assertTrue(resultado['ok'])
            self.assertEqual(resultado['arquivo_saida'], str(arq_saida))

            df_saida = ler_arquivo_csv(str(arq_saida))
            self.assertEqual(len(df_saida), 2)
            self.assertEqual(df_saida.loc[0, 'RESPOSTA'], 'Sim')
            self.assertEqual(df_saida.loc[0, 'SENHA'], 'SENHA001')
            self.assertNotIn('CHAVE_SENHA', df_saida.columns)
            self.assertEqual(df_saida.loc[1, 'Contato'], 'bruno_hospital_procedimento_46114_SENHA002')
            self.assertEqual(df_saida.loc[1, 'DT ENVIO'], 'outra data ruim')
            self.assertEqual(df_saida.loc[1, 'RESPOSTA'], 'Sem resposta')
            self.assertEqual(df_saida.loc[1, 'SENHA'], 'SENHA002')
        finally:
            self._limpar_pasta_tmp_teste(base)

    def test_status_integrado_ordena_colunas_pos_id_mailing(self):
        base = self._criar_pasta_tmp_teste()
        try:
            arq_status = base / 'status.csv'
            arq_resposta = base / 'status_resposta.csv'
            arq_saida = base / 'status_integrado.csv'

            df_status = pd.DataFrame(
                [
                    {
                        'Conta': '',
                        'HSM': 'Pesquisa Complicacoes Cirurgicas',
                        'Mensagem': '',
                        'Categoria': '',
                        'Template': '',
                        'Data do envio': '',
                        'Status': 'ENVIADA',
                        'Respondido': '',
                        'Protocolo': '',
                        'Agendamento': '',
                        'Data agendamento': '',
                        'Status agendamento': '',
                        'Campanha': '',
                        'Agente': '',
                        'Contato': 'ana_hospital_procedimento_46114_SENHA001',
                        'Telefone': '11999990000',
                        'ID_Mailing': 'mailing-1',
                        'SENHA': 'SENHA001',
                        'CHAVE_SENHA': 'SENHA001',
                        'DT ENVIO': '01/01/2026',
                    }
                ]
            )
            df_resposta = pd.DataFrame(
                [
                    {
                        'nom_contato': 'ana_hospital_procedimento_46114_SENHA001',
                        'resposta': 'Sim',
                    }
                ]
            )

            _salvar_csv(df_status, arq_status)
            _salvar_csv(df_resposta, arq_resposta)

            resultado = integrar_status_com_resposta(
                arquivo_status=str(arq_status),
                arquivo_status_resposta=str(arq_resposta),
                arquivo_saida=str(arq_saida),
            )

            self.assertTrue(resultado['ok'])
            df_saida = ler_arquivo_csv(str(arq_saida))
            colunas = list(df_saida.columns)
            indice_id_mailing = colunas.index('ID_Mailing')
            self.assertEqual(
                colunas[indice_id_mailing + 1: indice_id_mailing + 5],
                ['DT ENVIO', 'RESPOSTA', 'NOME_MANIPULADO', 'SENHA'],
            )
            self.assertNotIn('CHAVE_SENHA', colunas)
            self.assertEqual(df_saida.loc[0, 'SENHA'], 'SENHA001')
        finally:
            self._limpar_pasta_tmp_teste(base)


if __name__ == '__main__':
    unittest.main()
