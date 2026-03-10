import unittest

from src.services.normalizacao_services import corrigir_texto_bugado, normalizar_chave_texto


class NormalizacaoFrasesTests(unittest.TestCase):
    def test_aceita_entrada_legada_e_entrega_hsm_canonico(self):
        valor = 'Pesquisa Complicaï¿½ï¿½es Cirurgicas'
        self.assertEqual(
            corrigir_texto_bugado(valor),
            'Pesquisa Complicações Cirurgicas',
        )

    def test_aceita_entrada_legada_e_entrega_frase_numero_canonica(self):
        valor = 'Nï¿½mero ï¿½ parte de um experimento'
        self.assertEqual(
            corrigir_texto_bugado(valor),
            'Número é parte de um experimento',
        )

    def test_aceita_entrada_legada_e_entrega_frase_optout_canonica(self):
        valor = 'Usuï¿½rio decidiu nÃ£o receber MKT messages'
        self.assertEqual(
            corrigir_texto_bugado(valor),
            'Usuário decidiu não receber MKT messages',
        )

    def test_aceita_nao_legado_e_entrega_texto_canonico(self):
        self.assertEqual(corrigir_texto_bugado('Nï¿½o'), 'Não')

    def test_normalizar_chave_texto_usa_saida_estavel_para_entradas_legadas(self):
        self.assertEqual(
            normalizar_chave_texto('Nï¿½mero ï¿½ parte de um experimento'),
            'numero e parte de um experimento',
        )
        self.assertEqual(
            normalizar_chave_texto('Usuï¿½rio decidiu nÃ£o receber MKT messages'),
            'usuario decidiu nao receber mkt messages',
        )


if __name__ == '__main__':
    unittest.main()
