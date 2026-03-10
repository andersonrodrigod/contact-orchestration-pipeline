import shutil
import unittest
import uuid
from pathlib import Path

import pandas as pd

from src.services.dataset_service import (
    _carregar_status_para_lookup,
    concatenar_status_resposta_eletivo_internacao,
)
from src.utils.arquivos import ler_arquivo_csv


def _salvar_csv(df, caminho):
    df.to_csv(caminho, sep=';', index=False, encoding='utf-8-sig')


class DatasetServiceConcatenacaoStatusRespostaTests(unittest.TestCase):
    def _criar_pasta_tmp_teste(self):
        base = Path('tests/outputs/tmp')
        base.mkdir(parents=True, exist_ok=True)
        pasta = base / f'dataset_concat_{uuid.uuid4().hex}'
        pasta.mkdir(parents=True, exist_ok=True)
        return pasta

    def _limpar_pasta_tmp_teste(self, pasta):
        if pasta and pasta.exists():
            shutil.rmtree(pasta, ignore_errors=True)

    def test_concatenacao_status_resposta_aplica_contrato_canonico(self):
        base = self._criar_pasta_tmp_teste()
        try:
            arq_eletivo = base / 'status_resposta_eletivo.csv'
            arq_internacao = base / 'status_resposta_internacao.csv'
            arq_saida = base / 'status_resposta_unificado.csv'

            df_eletivo = pd.DataFrame(
                [
                    {
                        'nom_contato': 'usuario_1',
                        'dat_atendimento': '01/01/2026',
                        'resposta': 'Sim',
                        'RESPOSTA': 'Sim',
                    }
                ]
            )
            df_internacao = pd.DataFrame(
                [
                    {
                        'nom_contato': 'usuario_2',
                        'DT_ATENDIMENTO': '02/01/2026',
                        'resposta': 'Nao',
                        'Resposta': 'Nao',
                    }
                ]
            )

            _salvar_csv(df_eletivo, arq_eletivo)
            _salvar_csv(df_internacao, arq_internacao)

            resultado = concatenar_status_resposta_eletivo_internacao(
                arquivo_eletivo=str(arq_eletivo),
                arquivo_internacao=str(arq_internacao),
                arquivo_saida=str(arq_saida),
            )

            self.assertTrue(resultado['ok'])
            df_saida = ler_arquivo_csv(str(arq_saida))
            self.assertIn('resposta', df_saida.columns)
            self.assertNotIn('RESPOSTA', df_saida.columns)
            self.assertNotIn('Resposta', df_saida.columns)
        finally:
            self._limpar_pasta_tmp_teste(base)

    def test_lookup_status_integrado_canoniza_resposta_interna(self):
        base = self._criar_pasta_tmp_teste()
        try:
            arq_status = base / 'status_integrado.csv'
            df_status = pd.DataFrame(
                [
                    {
                        'Contato': 'usuario_1',
                        'DT ENVIO': '01/01/2026',
                        'Status': 'Enviada',
                        'Respondido': 'Sim',
                        'RESPOSTA': 'Nao',
                        'Telefone': '11999999999',
                    }
                ]
            )

            _salvar_csv(df_status, arq_status)

            resultado = _carregar_status_para_lookup(str(arq_status))

            self.assertTrue(resultado['ok'])
            self.assertIn('resposta', resultado['df_status_full'].columns)
            self.assertNotIn('RESPOSTA', resultado['df_status_full'].columns)
            self.assertEqual(resultado['df_status_full'].loc[0, 'resposta'], 'Não')
        finally:
            self._limpar_pasta_tmp_teste(base)


if __name__ == '__main__':
    unittest.main()
