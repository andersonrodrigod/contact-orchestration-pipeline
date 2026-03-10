import json
import unittest
from pathlib import Path

import pandas as pd

from src.services.texto_service import simplificar_texto


class BaselineComplicacaoTests(unittest.TestCase):
    def test_baseline_status_e_status_resposta(self):
        baseline_status = json.loads(
            Path('tests/baseline/status_baseline.json').read_text(encoding='utf-8')
        )
        baseline_resposta = json.loads(
            Path('tests/baseline/status_resposta_baseline.json').read_text(encoding='utf-8')
        )

        df_status = pd.read_csv(
            baseline_status['arquivo'],
            sep=';',
            dtype=str,
            encoding='utf-8-sig',
            keep_default_na=False,
        )
        df_resposta = pd.read_csv(
            baseline_resposta['arquivo'],
            sep=';',
            dtype=str,
            encoding='utf-8-sig',
            keep_default_na=False,
        )

        self.assertEqual(
            len(df_status),
            baseline_status['linhas'],
            'Mudou total de linhas de status; revisar baseline.',
        )
        self.assertEqual(
            len(df_resposta),
            baseline_resposta['linhas'],
            'Mudou total de linhas de status_resposta; revisar baseline.',
        )

        status_counts = (
            df_status['Status'].apply(simplificar_texto).replace('', '<vazio>').value_counts(dropna=False)
        )
        status_counts_dict = {str(k): int(v) for k, v in status_counts.items()}
        self.assertDictEqual(
            status_counts_dict,
            baseline_status['status_normalizado'],
            'Distribuicao de Status normalizado mudou; revisar baseline.',
        )

        resposta_status_col = 'RESPOSTA' if 'RESPOSTA' in df_status.columns else 'Resposta'
        resposta_status_counts = (
            df_status[resposta_status_col]
            .apply(simplificar_texto)
            .replace('', '<vazio>')
            .value_counts(dropna=False)
        )
        resposta_status_counts_dict = {str(k): int(v) for k, v in resposta_status_counts.items()}
        self.assertDictEqual(
            resposta_status_counts_dict,
            baseline_status['resposta_em_status_normalizado'],
            'Distribuicao de RESPOSTA no status mudou; revisar baseline.',
        )

        resposta_counts = (
            df_resposta['resposta'].apply(simplificar_texto).replace('', '<vazio>').value_counts(dropna=False)
        )
        resposta_counts_dict = {str(k): int(v) for k, v in resposta_counts.items()}
        self.assertDictEqual(
            resposta_counts_dict,
            baseline_resposta['resposta_normalizado'],
            'Distribuicao de resposta em status_resposta mudou; revisar baseline.',
        )


if __name__ == '__main__':
    unittest.main()
