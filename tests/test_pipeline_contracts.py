import unittest
from unittest.mock import patch

from core.pipeline_result import error_result, ok_result
from src.contracts.preflight_contracts import build_preflight_result
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

    def test_build_preflight_result_contrato(self):
        resultado = build_preflight_result(
            ok=False,
            contexto='complicacao',
            bloqueios=['arquivo ausente'],
            avisos=['coluna opcional ausente'],
            metricas={'linhas_status': 10},
            detalhes={'origem': 'teste'},
            codigo_erro='E101',
        )
        self.assertFalse(resultado.get('ok'))
        self.assertEqual(resultado.get('codigo_erro'), 'E101')
        self.assertIn('mensagens', resultado)
        self.assertIn('bloqueios', resultado)
        self.assertIn('avisos', resultado)
        self.assertIn('detalhes', resultado)
        self.assertEqual(resultado.get('contexto'), 'complicacao')

    def test_complicacao_pipeline_nao_agrega_metricas_antigas(self):
        chamadas = []

        def _status_dataset(**kwargs):
            chamadas.append(('status_dataset', kwargs))
            return {
                'ok': True,
                'mensagens': ['status ok'],
                'arquivo_status_dataset': 'status_dataset.csv',
                'total_status': 10,
                'com_match': 7,
                'sem_match': 3,
                'metricas_por_etapa': {'status': {'total': 10}},
            }

        def _orquestracao(**kwargs):
            chamadas.append(('orquestracao', kwargs))
            return {
                'ok': True,
                'mensagens': ['orquestracao ok'],
                'arquivo_saida': 'saida.csv',
                'total_usuarios': 5,
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
        self.assertNotIn('total_status', resultado)
        self.assertNotIn('com_match', resultado)
        self.assertNotIn('sem_match', resultado)
        self.assertNotIn('metricas_por_etapa', resultado)


if __name__ == '__main__':
    unittest.main()

