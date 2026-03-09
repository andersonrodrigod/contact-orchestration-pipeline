import unittest
import uuid
import shutil
from pathlib import Path

import pandas as pd

from core.error_codes import ERRO_QUALIDADE_DATA, ERRO_VALIDACAO_ARQUIVOS
from src.pipelines.preflight_pipeline import run_preflight_pipeline


def _salvar_csv(df, caminho):
    df.to_csv(caminho, sep=';', index=False, encoding='utf-8-sig')


def _dataset_origem_minimo():
    return pd.DataFrame(
        [
            {
                'BASE': 'A',
                'COD USUARIO': '1',
                'USUARIO': 'USUARIO TESTE',
                'TELEFONE 1': '11999999999',
                'TELEFONE 2': '',
                'TELEFONE 3': '',
                'TELEFONE 4': '',
                'TELEFONE 5': '',
                'PRESTADOR': 'HOSPITAL',
                'PROCEDIMENTO': 'PROC',
                'TP ATENDIMENTO': 'TIPO',
                'DT INTERNACAO': '01/01/2026',
                'DT ENVIO': '01/01/2026',
                'CHAVE': 'CH-1',
                'STATUS': 'ENVIADA',
                'P1': '',
            }
        ]
    )


class PreflightPipelineTests(unittest.TestCase):
    def _criar_pasta_tmp_teste(self):
        base = Path('tests/outputs/tmp')
        base.mkdir(parents=True, exist_ok=True)
        pasta = base / f'preflight_{uuid.uuid4().hex}'
        pasta.mkdir(parents=True, exist_ok=True)
        return pasta

    def _limpar_pasta_tmp_teste(self, pasta):
        if pasta and pasta.exists():
            shutil.rmtree(pasta, ignore_errors=True)

    def test_preflight_retorna_erro_quando_arquivos_nao_existem(self):
        resultado = run_preflight_pipeline(
            contexto='complicacao',
            arquivo_status='nao_existe_status.csv',
            arquivo_status_resposta='nao_existe_resposta.csv',
            arquivo_dataset_origem='nao_existe_dataset.csv',
            nome_logger='test_preflight_missing_files',
        )
        self.assertFalse(resultado.get('ok'))
        self.assertEqual(resultado.get('codigo_erro'), ERRO_VALIDACAO_ARQUIVOS)
        self.assertIn('bloqueios', resultado)
        self.assertGreater(len(resultado.get('bloqueios', [])), 0)

    def test_preflight_bloqueia_quando_qualidade_de_data_ultrapassa_limiar(self):
        base = self._criar_pasta_tmp_teste()
        try:
            arq_status = base / 'status.csv'
            arq_resposta = base / 'status_resposta.csv'
            arq_dataset = base / 'dataset.csv'

            df_status = pd.DataFrame(
                [
                    {
                        'Data agendamento': 'data-invalida',
                        'HSM': 'Pesquisa Complicacoes Cirurgicas',
                        'Status': 'ENVIADA',
                        'Respondido': 'NAO',
                        'Contato': 'usuario_1',
                        'Telefone': '11999999999',
                    }
                ]
            )
            df_resposta = pd.DataFrame(
                [
                    {
                        'nom_contato': 'usuario_1',
                        'DT_ATENDIMENTO': 'data-invalida',
                        'resposta': '',
                    }
                ]
            )
            df_dataset = _dataset_origem_minimo()

            _salvar_csv(df_status, arq_status)
            _salvar_csv(df_resposta, arq_resposta)
            _salvar_csv(df_dataset, arq_dataset)

            resultado = run_preflight_pipeline(
                contexto='complicacao',
                arquivo_status=str(arq_status),
                arquivo_status_resposta=str(arq_resposta),
                arquivo_dataset_origem=str(arq_dataset),
                limiar_nat_data=30.0,
                nome_logger='test_preflight_quality_block',
            )

            self.assertFalse(resultado.get('ok'))
            self.assertEqual(resultado.get('codigo_erro'), ERRO_QUALIDADE_DATA)
            self.assertIn('bloqueios', resultado)
            self.assertGreater(len(resultado.get('bloqueios', [])), 0)
        finally:
            self._limpar_pasta_tmp_teste(base)

    def test_preflight_ok_com_entradas_minimas_validas(self):
        base = self._criar_pasta_tmp_teste()
        try:
            arq_status = base / 'status.csv'
            arq_resposta = base / 'status_resposta.csv'
            arq_dataset = base / 'dataset.csv'

            df_status = pd.DataFrame(
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
            df_resposta = pd.DataFrame(
                [
                    {
                        'nom_contato': 'usuario_1',
                        'DT_ATENDIMENTO': '01/01/2026',
                        'resposta': '',
                    }
                ]
            )
            df_dataset = _dataset_origem_minimo()

            _salvar_csv(df_status, arq_status)
            _salvar_csv(df_resposta, arq_resposta)
            _salvar_csv(df_dataset, arq_dataset)

            resultado = run_preflight_pipeline(
                contexto='complicacao',
                arquivo_status=str(arq_status),
                arquivo_status_resposta=str(arq_resposta),
                arquivo_dataset_origem=str(arq_dataset),
                limiar_nat_data=30.0,
                nome_logger='test_preflight_ok',
            )

            self.assertTrue(resultado.get('ok'))
            self.assertIn('mensagens', resultado)
            self.assertIn('contexto', resultado)
            self.assertEqual(resultado.get('contexto'), 'complicacao')
        finally:
            self._limpar_pasta_tmp_teste(base)


if __name__ == '__main__':
    unittest.main()
