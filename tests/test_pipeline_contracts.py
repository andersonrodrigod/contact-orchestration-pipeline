import unittest
from unittest.mock import patch

from core.pipeline_result import error_result, ok_result
from src.pipelines.complicacao_pipeline import run_complicacao_pipeline


class PipelineContractsTests(unittest.TestCase):
    def test_ok_result_contrato_minimo(self):
        resultado = ok_result(mensagens=['ok'])
        self.assertIsInstance(resultado, dict)
        self.assertTrue(resultado.get('ok'))
        self.assertIn('mensagens', resultado)
        self.assertEqual(resultado['mensagens'], ['ok'])

    def test_error_result_contrato_minimo(self):
        resultado = error_result(mensagens=['falhou'], codigo_erro='E999')
        self.assertIsInstance(resultado, dict)
        self.assertFalse(resultado.get('ok'))
        self.assertIn('mensagens', resultado)
        self.assertEqual(resultado.get('codigo_erro'), 'E999')

    def test_complicacao_pipeline_agrega_apenas_contrato_operacional(self):
        chamadas = []

        def _status_dataset(**kwargs):
            chamadas.append(('status_dataset', kwargs))
            return {
                'ok': True,
                'mensagens': ['status ok'],
                'arquivo_status_dataset': 'status_dataset.csv',
            }

        def _orquestracao(**kwargs):
            chamadas.append(('orquestracao', kwargs))
            return {
                'ok': True,
                'mensagens': ['orquestracao ok'],
                'arquivo_saida': 'saida.csv',
            }

        with patch(
            'src.pipelines.complicacao_pipeline.run_complicacao_pipeline_gerar_status_dataset',
            side_effect=_status_dataset,
        ), patch(
            'src.pipelines.complicacao_pipeline.run_complicacao_pipeline_orquestrar',
            side_effect=_orquestracao,
        ):
            resultado = run_complicacao_pipeline()

        self.assertTrue(resultado.get('ok'))
        self.assertEqual(
            [nome for nome, _kwargs in chamadas],
            ['status_dataset', 'orquestracao'],
        )
        self.assertEqual(resultado.get('mensagens'), ['status ok', 'orquestracao ok'])
        self.assertEqual(resultado.get('arquivo_status_dataset'), 'status_dataset.csv')
        self.assertEqual(resultado.get('arquivo_saida'), 'saida.csv')
        self.assertEqual(set(resultado), {'ok', 'mensagens', 'arquivo_status_dataset', 'arquivo_saida'})


if __name__ == '__main__':
    unittest.main()

