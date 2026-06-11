import unittest
import warnings

import pandas as pd

from src.services.orquestracao_service import (
    _criar_aba_disparo,
    aplicar_classificacao_processo_acao,
    orquestrar_usuarios_respondidos,
)


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

    def test_usuarios_resolvidos_usam_chave_senha(self):
        usuarios = pd.DataFrame(
            [
                {'CHAVE_SENHA': 'SENHA001', 'USUARIO': 'Ana'},
                {'CHAVE_SENHA': 'SENHA002', 'USUARIO': 'Bruno'},
            ]
        )
        respondidos = pd.DataFrame(
            [
                {'CHAVE_SENHA': 'SENHA001', 'USUARIO': 'Ana'},
            ]
        )

        restantes, resolvidos = orquestrar_usuarios_respondidos(usuarios, respondidos)

        self.assertEqual(list(restantes['CHAVE_SENHA']), ['SENHA002'])
        self.assertEqual(list(resolvidos['CHAVE_SENHA']), ['SENHA001'])
        self.assertEqual(resolvidos.loc[0, 'PROCESSO'], 'RESOLVIDO')

    def test_disparo_deduplica_e_valida_por_chave_senha(self):
        usuarios = pd.DataFrame(
            [
                {
                    'CHAVE_SENHA': 'SENHA001',
                    'STATUS CHAVE': 'OK_PRINCIPAL',
                    'PROCESSO': 'MUDAR_CONTATO_ENVIADO',
                    'ACAO': 'TELEFONE 2',
                    'TELEFONE 1': '11999990001',
                    'TELEFONE 2': '11999990002',
                    'TELEFONE PRIORIDADE': 'TELEFONE 1',
                    'TELEFONE ENVIADO': '11999990001',
                    'PROXIMO TELEFONE DISPONIVEL': 'TELEFONE 2',
                    'USUARIO': 'Ana',
                },
                {
                    'CHAVE_SENHA': 'SENHA001',
                    'STATUS CHAVE': 'OK_PRINCIPAL',
                    'PROCESSO': 'MUDAR_CONTATO_ENVIADO',
                    'ACAO': 'TELEFONE 2',
                    'TELEFONE 1': '11999990001',
                    'TELEFONE 2': '11999990002',
                    'TELEFONE PRIORIDADE': 'TELEFONE 1',
                    'TELEFONE ENVIADO': '11999990001',
                    'PROXIMO TELEFONE DISPONIVEL': 'TELEFONE 2',
                    'USUARIO': 'Ana duplicada',
                },
            ]
        )

        disparo = _criar_aba_disparo(usuarios, usuarios.copy())

        self.assertEqual(len(disparo), 1)
        self.assertEqual(disparo.loc[0, 'CHAVE_SENHA'], 'SENHA001')
        self.assertEqual(disparo.loc[0, 'VALIDACAO CHAVE'], 'OK')
        self.assertEqual(disparo.loc[0, 'VALIDACAO FINAL'], 'OK')


if __name__ == '__main__':
    unittest.main()
