import unittest
import warnings

import pandas as pd

from src.services.orquestracao_service import aplicar_classificacao_processo_acao


class OrquestracaoServiceTests(unittest.TestCase):
    def test_classificacao_aceita_coluna_legada_lida_reposta_nao_com_warning(self):
        df = pd.DataFrame(
            [
                {
                    'STATUS CHAVE': 'OK_PRINCIPAL',
                    'LIDA_REPOSTA_NAO': 1,
                    'LIDA': 1,
                    'LIDA_SEM_RESPOSTA': 0,
                    'ENTREGUE': 0,
                    'ENVIADA': 0,
                    'NAO_ENTREGUE_META': 0,
                    'MENSAGEM_NAO_ENTREGUE': 0,
                    'EXPERIMENTO': 0,
                    'OPT_OUT': 0,
                    'RESPOSTA': '',
                    'PROXIMO TELEFONE DISPONIVEL': 'TELEFONE 2',
                    'TELEFONE PRIORIDADE': 'TELEFONE 1',
                    'PROCESSO': '',
                    'ACAO': '',
                }
            ]
        )

        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter('always')
            saida = aplicar_classificacao_processo_acao(df)

        self.assertEqual(saida.loc[0, 'PROCESSO'], 'MUDAR_CONTATO_LIDO_NAO')
        self.assertEqual(saida.loc[0, 'ACAO'], 'TELEFONE 2')
        self.assertTrue(any('LIDA_REPOSTA_NAO' in str(w.message) for w in captured))


if __name__ == '__main__':
    unittest.main()
