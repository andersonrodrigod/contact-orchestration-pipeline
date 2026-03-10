import unittest

from src.services.normalizacao_services import corrigir_texto_bugado, normalizar_chave_texto


class NormalizacaoFrasesTests(unittest.TestCase):
    def test_normaliza_hsm_complicacoes_com_caractere_quebrado(self):
        valor = 'Pesquisa Complica��es Cirurgicas'
        self.assertEqual(
            corrigir_texto_bugado(valor),
            'Pesquisa Complicações Cirurgicas',
        )

    def test_normaliza_frase_numero_experimento(self):
        valor = 'N�mero � parte de um experimento'
        self.assertEqual(
            corrigir_texto_bugado(valor),
            'Número é parte de um experimento',
        )

    def test_normaliza_frase_usuario_optout(self):
        valor = 'Usu�rio decidiu não receber MKT messages'
        self.assertEqual(
            corrigir_texto_bugado(valor),
            'Usuário decidiu não receber MKT messages',
        )

    def test_normaliza_nao_isolado(self):
        self.assertEqual(corrigir_texto_bugado('N�o'), 'Não')

    def test_normalizar_chave_texto_fragmentada(self):
        self.assertEqual(
            normalizar_chave_texto('N�mero � parte de um experimento'),
            'numero e parte de um experimento',
        )
        self.assertEqual(
            normalizar_chave_texto('Usu�rio decidiu não receber MKT messages'),
            'usuario decidiu nao receber mkt messages',
        )


if __name__ == '__main__':
    unittest.main()
