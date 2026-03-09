import unittest

from core.error_codes import (
    ERRO_QUALIDADE_DATA,
    ERRO_VALIDACAO_ARQUIVOS,
    anexar_codigo_erro,
    inferir_codigo_erro_por_mensagens,
)


class ErrorCodesTests(unittest.TestCase):
    def test_inferir_codigo_erro_por_mensagens_para_arquivo_faltando(self):
        codigo = inferir_codigo_erro_por_mensagens(
            ['Arquivo status nao encontrado no caminho informado.']
        )
        self.assertEqual(codigo, ERRO_VALIDACAO_ARQUIVOS)

    def test_inferir_codigo_erro_por_mensagens_para_qualidade_data(self):
        codigo = inferir_codigo_erro_por_mensagens(
            ['Qualidade de data abaixo do esperado: 45% NaT.']
        )
        self.assertEqual(codigo, ERRO_QUALIDADE_DATA)

    def test_anexar_codigo_erro_preenche_resultado_sem_codigo(self):
        resultado = {'ok': False, 'mensagens': ['arquivo faltando']}
        saida = anexar_codigo_erro(resultado)
        self.assertEqual(saida.get('codigo_erro'), ERRO_VALIDACAO_ARQUIVOS)


if __name__ == '__main__':
    unittest.main()
