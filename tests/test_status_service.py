import shutil
import unittest
import uuid
from pathlib import Path

import pandas as pd

from src.services.status_service import integrar_status_com_resposta
from src.services.schema_chave_service import COLUNA_CHAVE_SENHA
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
            self.assertEqual(resultado['total_status'], 2)
            self.assertEqual(resultado['com_match'], 1)
            self.assertEqual(resultado['sem_match'], 1)
            self.assertEqual(resultado['descartados_status_data_invalida'], 0)
            self.assertEqual(resultado['descartados_resposta_data_invalida'], 0)

            df_saida = ler_arquivo_csv(str(arq_saida))
            self.assertEqual(len(df_saida), 2)
            self.assertEqual(df_saida.loc[0, 'RESPOSTA'], 'Sim')
            self.assertEqual(df_saida.loc[0, COLUNA_CHAVE_SENHA], 'SENHA001')
            self.assertEqual(df_saida.loc[1, 'Contato'], 'bruno_hospital_procedimento_46114_SENHA002')
            self.assertEqual(df_saida.loc[1, 'DT ENVIO'], 'outra data ruim')
            self.assertEqual(df_saida.loc[1, 'RESPOSTA'], 'Sem resposta')
        finally:
            self._limpar_pasta_tmp_teste(base)


if __name__ == '__main__':
    unittest.main()
