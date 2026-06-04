import inspect
import unittest

from src.config.paths import DEFAULTS_COMPLICACAO
from src.contexts.pipeline_contextos import CONTEXTO_PIPELINE_COMPLICACAO
from src.pipelines.complicacao_orquestracao_pipeline import (
    run_complicacao_pipeline_orquestrar,
)
from src.pipelines.complicacao_pipeline import run_complicacao_pipeline
from src.pipelines.join_status_resposta_pipeline import (
    run_status_somente_complicacao_pipeline,
    run_unificar_status_resposta_complicacao_pipeline,
)
from src.pipelines.status_normalizar_complicacao_pipeline import (
    run_status_normalizar_complicacao_pipeline,
)


class PathsConfigTests(unittest.TestCase):
    def test_contexto_complicacao_usa_defaults_centrais(self):
        self.assertIs(CONTEXTO_PIPELINE_COMPLICACAO.defaults, DEFAULTS_COMPLICACAO)

    def test_defaults_do_coordenador_complicacao_vem_do_contexto(self):
        assinatura = inspect.signature(run_complicacao_pipeline)

        self.assertEqual(
            assinatura.parameters['arquivo_status'].default,
            DEFAULTS_COMPLICACAO['arquivo_status'],
        )
        self.assertEqual(
            assinatura.parameters['arquivo_status_resposta_complicacao'].default,
            DEFAULTS_COMPLICACAO['arquivo_status_resposta_complicacao'],
        )
        self.assertEqual(
            assinatura.parameters['arquivo_dataset_origem_complicacao'].default,
            DEFAULTS_COMPLICACAO['arquivo_dataset_origem_complicacao'],
        )
        self.assertEqual(
            assinatura.parameters['saida_status'].default,
            DEFAULTS_COMPLICACAO['saida_status'],
        )
        self.assertEqual(
            assinatura.parameters['saida_status_resposta'].default,
            DEFAULTS_COMPLICACAO['saida_status_resposta'],
        )
        self.assertEqual(
            assinatura.parameters['saida_status_integrado'].default,
            DEFAULTS_COMPLICACAO['saida_status_integrado'],
        )
        self.assertEqual(
            assinatura.parameters['saida_dataset_status'].default,
            DEFAULTS_COMPLICACAO['saida_dataset_status'],
        )
        self.assertEqual(
            assinatura.parameters['saida_dataset_final'].default,
            DEFAULTS_COMPLICACAO['saida_dataset_final'],
        )

    def test_defaults_da_orquestracao_vem_do_contexto(self):
        assinatura = inspect.signature(run_complicacao_pipeline_orquestrar)

        self.assertEqual(
            assinatura.parameters['arquivo_dataset_status'].default,
            DEFAULTS_COMPLICACAO['saida_dataset_status'],
        )
        self.assertEqual(
            assinatura.parameters['arquivo_saida_final'].default,
            DEFAULTS_COMPLICACAO['saida_dataset_final'],
        )
        self.assertEqual(
            assinatura.parameters['nome_logger'].default,
            CONTEXTO_PIPELINE_COMPLICACAO.logger_orquestracao,
        )

    def test_stub_somente_status_usa_defaults_centrais(self):
        assinatura = inspect.signature(run_status_normalizar_complicacao_pipeline)

        self.assertEqual(
            assinatura.parameters['arquivo_status'].default,
            DEFAULTS_COMPLICACAO['arquivo_status'],
        )
        self.assertEqual(
            assinatura.parameters['arquivo_status_normalizado'].default,
            DEFAULTS_COMPLICACAO['saida_status'],
        )
        self.assertEqual(
            assinatura.parameters['arquivo_saida'].default,
            DEFAULTS_COMPLICACAO['saida_status_sem_complicacao'],
        )

    def test_defaults_da_integracao_status_resposta_vem_do_contexto(self):
        assinatura = inspect.signature(run_unificar_status_resposta_complicacao_pipeline)

        self.assertEqual(
            assinatura.parameters['arquivo_status'].default,
            DEFAULTS_COMPLICACAO['saida_status'],
        )
        self.assertEqual(
            assinatura.parameters['arquivo_status_resposta'].default,
            DEFAULTS_COMPLICACAO['saida_status_resposta'],
        )
        self.assertEqual(
            assinatura.parameters['arquivo_saida'].default,
            DEFAULTS_COMPLICACAO['saida_status_integrado'],
        )

    def test_defaults_da_integracao_somente_status_vem_do_contexto(self):
        assinatura = inspect.signature(run_status_somente_complicacao_pipeline)

        self.assertEqual(
            assinatura.parameters['arquivo_status'].default,
            DEFAULTS_COMPLICACAO['saida_status'],
        )
        self.assertEqual(
            assinatura.parameters['arquivo_saida'].default,
            DEFAULTS_COMPLICACAO['saida_status_integrado'],
        )


if __name__ == '__main__':
    unittest.main()
