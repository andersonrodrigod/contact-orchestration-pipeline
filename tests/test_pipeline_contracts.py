import unittest

from core.pipeline_result import error_result, ok_result
from src.contracts.preflight_contracts import build_preflight_result


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


if __name__ == '__main__':
    unittest.main()

