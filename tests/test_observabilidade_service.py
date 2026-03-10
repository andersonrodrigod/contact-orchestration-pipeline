import json
import unittest
from datetime import datetime
from pathlib import Path

from src.services.observabilidade_service import (
    _resolver_arquivo_historico_trimestral,
    registrar_historico_execucao,
)


class ObservabilidadeServiceTests(unittest.TestCase):
    def test_resolve_arquivo_historico_trimestral(self):
        base = Path('logs/historico_execucoes.jsonl')

        q1 = _resolver_arquivo_historico_trimestral(base, datetime(2026, 1, 15))
        q2 = _resolver_arquivo_historico_trimestral(base, datetime(2026, 5, 10))
        q4 = _resolver_arquivo_historico_trimestral(base, datetime(2026, 11, 1))

        self.assertEqual(str(q1), 'logs\\historico_execucoes_2026_Q1.jsonl')
        self.assertEqual(str(q2), 'logs\\historico_execucoes_2026_Q2.jsonl')
        self.assertEqual(str(q4), 'logs\\historico_execucoes_2026_Q4.jsonl')

    def test_registrar_historico_execucao_escreve_jsonl_rotacionado(self):
        base_dir = Path('logs')
        base_dir.mkdir(parents=True, exist_ok=True)
        arquivo_base = base_dir / 'historico_execucoes_teste.jsonl'
        caminho_rotacionado = _resolver_arquivo_historico_trimestral(arquivo_base, datetime.now())

        if caminho_rotacionado.exists():
            caminho_rotacionado.unlink()

        caminho_resultado = registrar_historico_execucao(
            {'ok': True, 'total_status': 12},
            modo='teste',
            arquivo_historico=arquivo_base,
        )

        caminho_resultado = Path(caminho_resultado)
        self.assertTrue(caminho_resultado.exists())
        self.assertIn('historico_execucoes_teste_', caminho_resultado.name)
        self.assertIn('_Q', caminho_resultado.name)
        self.assertTrue(caminho_resultado.name.endswith('.jsonl'))

        linhas = caminho_resultado.read_text(encoding='utf-8').strip().splitlines()
        self.assertEqual(len(linhas), 1)
        payload = json.loads(linhas[0])
        self.assertEqual(payload.get('modo'), 'teste')
        self.assertTrue(payload.get('ok'))
        self.assertEqual(payload.get('metricas', {}).get('total_status'), 12)

        caminho_resultado.unlink()


if __name__ == '__main__':
    unittest.main()
