import shutil
import unittest
import uuid
from pathlib import Path

import pandas as pd

from src.services.ingestao_service import executar_ingestao_complicacao
from src.services.schema_chave_service import COLUNA_CHAVE_SENHA
from src.utils.arquivos import ler_arquivo_csv


def _salvar_csv(df, caminho):
    df.to_csv(caminho, sep=';', index=False, encoding='utf-8-sig')


class IngestaoServiceTests(unittest.TestCase):
    def _criar_pasta_tmp_teste(self):
        base = Path('tests/outputs/tmp')
        base.mkdir(parents=True, exist_ok=True)
        pasta = base / f'ingestao_service_{uuid.uuid4().hex}'
        pasta.mkdir(parents=True, exist_ok=True)
        return pasta

    def _limpar_pasta_tmp_teste(self, pasta):
        if pasta and pasta.exists():
            shutil.rmtree(pasta, ignore_errors=True)

    def test_ingestao_cria_chave_senha_no_status_e_status_resposta(self):
        base = self._criar_pasta_tmp_teste()
        try:
            arq_status = base / 'status.csv'
            arq_resposta = base / 'status_resposta.csv'
            saida_status = base / 'status_limpo.csv'
            saida_resposta = base / 'status_resposta_limpo.csv'

            df_status = pd.DataFrame(
                [
                    {
                        'Data agendamento': '01/01/2026',
                        'HSM': 'Pesquisa Complicacoes Cirurgicas',
                        'Status': 'ENVIADA',
                        'Respondido': 'NAO',
                        'Contato': 'ana_hospital_procedimento_46114_SENHA001',
                        'Telefone': '11999999999',
                        'Mensagem': 'texto sensivel status',
                        'Campanha': 'campanha sensivel',
                    }
                ]
            )
            df_resposta = pd.DataFrame(
                [
                    {
                        'nom_contato': 'ana_hospital_procedimento_46112_SENHA001',
                        'DT_ATENDIMENTO': '01/01/2026',
                        'resposta': 'Sim',
                        'mensagem': 'texto sensivel resposta',
                        'telefone': '11888888888',
                    }
                ]
            )

            _salvar_csv(df_status, arq_status)
            _salvar_csv(df_resposta, arq_resposta)

            resultado = executar_ingestao_complicacao(
                arquivo_status=str(arq_status),
                arquivo_status_resposta_complicacao=str(arq_resposta),
                saida_status=str(saida_status),
                saida_status_resposta=str(saida_resposta),
            )

            self.assertTrue(resultado['ok'])
            df_status_saida = ler_arquivo_csv(str(saida_status))
            df_resposta_saida = ler_arquivo_csv(str(saida_resposta))
            self.assertEqual(df_status_saida.loc[0, COLUNA_CHAVE_SENHA], 'SENHA001')
            self.assertEqual(df_resposta_saida.loc[0, COLUNA_CHAVE_SENHA], 'SENHA001')
            self.assertNotIn('CHAVE PRINCIPAL', df_status_saida.columns)
            self.assertNotIn('CHAVE PRINCIPAL', df_resposta_saida.columns)
            self.assertEqual(df_status_saida.loc[0, 'Mensagem'], '')
            self.assertEqual(df_status_saida.loc[0, 'Campanha'], '')
            self.assertEqual(df_resposta_saida.loc[0, 'mensagem'], '')
            self.assertEqual(df_resposta_saida.loc[0, 'telefone'], '')
            self.assertEqual(df_status_saida.loc[0, 'Contato'], 'ana_hospital_procedimento_46114_SENHA001')
            self.assertEqual(df_resposta_saida.loc[0, 'nom_contato'], 'ana_hospital_procedimento_46112_SENHA001')
        finally:
            self._limpar_pasta_tmp_teste(base)


if __name__ == '__main__':
    unittest.main()
