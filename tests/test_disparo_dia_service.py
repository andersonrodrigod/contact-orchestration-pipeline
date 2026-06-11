import shutil
import unittest
import uuid
from datetime import date
from pathlib import Path

import pandas as pd

from src.services.disparo_dia_service import (
    montar_disparo_dia,
    gerar_arquivos_disparo_dia,
)
from src.utils.arquivos import ler_arquivo_csv


class DisparoDiaServiceTests(unittest.TestCase):
    def _criar_pasta_tmp_teste(self):
        base = Path('tests/outputs/tmp')
        base.mkdir(parents=True, exist_ok=True)
        pasta = base / f'disparo_dia_service_{uuid.uuid4().hex}'
        pasta.mkdir(parents=True, exist_ok=True)
        return pasta

    def _limpar_pasta_tmp_teste(self, pasta):
        if pasta and pasta.exists():
            shutil.rmtree(pasta, ignore_errors=True)

    def _df_usuarios(self):
        return pd.DataFrame(
            [
                {
                    'TELEFONE 1': '11999990008',
                    'CHAVE RELATORIO': 'rel_8',
                    'USUARIO': 'Usuario oito',
                    'PROCEDIMENTO': 'Cirurgia A',
                    'PRESTADOR': 'Hospital A',
                    'DT INTERNACAO': '08/05/2026',
                },
                {
                    'TELEFONE 1': '11999990007',
                    'CHAVE RELATORIO': 'rel_7',
                    'USUARIO': 'Usuario sete',
                    'PROCEDIMENTO': 'Cirurgia B',
                    'PRESTADOR': 'Hospital B',
                    'DT INTERNACAO': '07/05/2026',
                },
                {
                    'TELEFONE 1': '11999990011',
                    'CHAVE RELATORIO': 'rel_11',
                    'USUARIO': 'Usuario onze',
                    'PROCEDIMENTO': 'Cirurgia C',
                    'PRESTADOR': 'Hospital C',
                    'DT INTERNACAO': '11/05/2026',
                },
                {
                    'TELEFONE 1': '11999990013',
                    'CHAVE RELATORIO': 'rel_13',
                    'USUARIO': 'Usuario treze',
                    'PROCEDIMENTO': 'Cirurgia D',
                    'PRESTADOR': 'Hospital D',
                    'DT INTERNACAO': '13/05/2026',
                },
            ]
        )

    def _df_disparo(self):
        return pd.DataFrame(
            [
                {
                    'TELEFONE DISPARO': '11888880001',
                    'CHAVE RELATORIO': 'disp_ok',
                    'USUARIO': 'Disparo ok',
                    'PROCEDIMENTO': 'Cirurgia E',
                    'PRESTADOR': 'Hospital E',
                    'DT INTERNACAO': '10/05/2026',
                    'VALIDACAO FINAL': 'OK',
                    'PROCESSO': 'MUDAR_CONTATO_ENVIADO',
                },
                {
                    'TELEFONE DISPARO': '11888880002',
                    'CHAVE RELATORIO': 'disp_segundo',
                    'USUARIO': 'Disparo segundo envio',
                    'PROCEDIMENTO': 'Cirurgia F',
                    'PRESTADOR': 'Hospital F',
                    'DT INTERNACAO': '10/05/2026',
                    'VALIDACAO FINAL': 'OK',
                    'PROCESSO': 'SEGUNDO_ENVIO',
                },
                {
                    'TELEFONE DISPARO': '11888880003',
                    'CHAVE RELATORIO': 'disp_dia_hoje',
                    'USUARIO': 'Disparo dia hoje',
                    'PROCEDIMENTO': 'Cirurgia G',
                    'PRESTADOR': 'Hospital G',
                    'DT INTERNACAO': '11/05/2026',
                    'VALIDACAO FINAL': 'OK',
                    'PROCESSO': 'MUDAR_CONTATO_ENVIADO',
                },
                {
                    'TELEFONE DISPARO': '11888880004',
                    'CHAVE RELATORIO': 'disp_invalido',
                    'USUARIO': 'Disparo invalido',
                    'PROCEDIMENTO': 'Cirurgia H',
                    'PRESTADOR': 'Hospital H',
                    'DT INTERNACAO': '10/05/2026',
                    'VALIDACAO FINAL': 'NAO ENCONTRADO',
                    'PROCESSO': 'MUDAR_CONTATO_ENVIADO',
                },
            ]
        )

    def test_segunda_usuarios_pega_hoje_e_anterior_e_disparo_so_validacao_ok(self):
        saida = montar_disparo_dia(
            self._df_usuarios(),
            self._df_disparo(),
            data_referencia=date(2026, 6, 8),
        )

        self.assertEqual(
            set(saida['Nome']),
            {'rel_8', 'rel_7', 'disp_ok', 'disp_segundo', 'disp_dia_hoje'},
        )

    def test_dia_comum_disparo_aplica_filtros_com_and(self):
        saida = montar_disparo_dia(
            self._df_usuarios(),
            self._df_disparo(),
            data_referencia=date(2026, 6, 11),
        )

        self.assertEqual(set(saida['Nome']), {'rel_11', 'disp_ok'})

    def test_sexta_gera_arquivo_dia_seguinte_somente_com_usuarios(self):
        base = self._criar_pasta_tmp_teste()
        try:
            arquivo_xlsx = base / 'complicacao_final.xlsx'
            with pd.ExcelWriter(arquivo_xlsx, engine='openpyxl') as writer:
                self._df_usuarios().to_excel(writer, sheet_name='usuarios', index=False)
                self._df_disparo().to_excel(writer, sheet_name='disparo', index=False)

            resultado = gerar_arquivos_disparo_dia(
                arquivo_xlsx,
                data_referencia=date(2026, 6, 12),
            )

            self.assertTrue(resultado['ok'])
            self.assertIn('disparo_dia', resultado['arquivos'])
            self.assertIn('disparo_dia_seguinte', resultado['arquivos'])

            df_dia_seguinte = ler_arquivo_csv(resultado['arquivos']['disparo_dia_seguinte'])
            self.assertEqual(list(df_dia_seguinte['Nome']), ['rel_13'])
            self.assertEqual(list(df_dia_seguinte['Telefone']), ['11999990013'])
        finally:
            self._limpar_pasta_tmp_teste(base)


if __name__ == '__main__':
    unittest.main()
