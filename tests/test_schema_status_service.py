import unittest

import pandas as pd

from src.services.schema_status_service import normalizar_schema_status


class SchemaStatusServiceTests(unittest.TestCase):
    def test_normaliza_schema_status_mantem_ordem_canonica(self):
        df = pd.DataFrame(
            {
                'extra': ['x'],
                'Telefone': ['11999999999'],
                'Contato': ['usuario_1'],
                'Status': ['ENVIADA'],
                'Data agendamento': ['01/01/2026'],
            }
        )

        saida = normalizar_schema_status(df)

        self.assertEqual(
            list(saida.columns),
            ['Status', 'Data agendamento', 'Contato', 'Telefone', 'extra'],
        )


if __name__ == '__main__':
    unittest.main()
