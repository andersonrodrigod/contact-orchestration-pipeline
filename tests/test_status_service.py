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

    def test_integracao_preserva_linha_status_com_data_invalida(self):
        base = self._criar_pasta_tmp_teste()
        try:
            arq_status = base / 'status.csv'
            arq_resposta = base / 'status_resposta.csv'
            arq_saida = base / 'status_integrado.csv'

            df_status = pd.DataFrame(
                [
                    {
                        'Contato': 'ana',
                        'DT ENVIO': '10/03/2026',
                        'Status': 'ENVIADA',
                    },
                    {
                        'Contato': 'bruno',
                        'DT ENVIO': 'data ruim',
                        'Status': 'ENVIADA',
                    },
                ]
            )
            df_resposta = pd.DataFrame(
                [
                    {
                        'nom_contato': 'ana',
                        'DT_ATENDIMENTO': '10/03/2026',
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
            self.assertEqual(resultado['descartados_status_data_invalida'], 1)

            df_saida = ler_arquivo_csv(str(arq_saida))
            self.assertEqual(len(df_saida), 2)
            self.assertEqual(df_saida.loc[1, 'Contato'], 'bruno')
            self.assertEqual(df_saida.loc[1, 'DT ENVIO'], '')
            self.assertEqual(df_saida.loc[1, 'RESPOSTA'], 'Sem resposta')
        finally:
            self._limpar_pasta_tmp_teste(base)


if __name__ == '__main__':
    unittest.main()
