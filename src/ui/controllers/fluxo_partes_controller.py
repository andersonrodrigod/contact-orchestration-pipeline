from __future__ import annotations

from src.pipelines.complicacao_orquestracao_pipeline import run_complicacao_pipeline_orquestrar
from src.pipelines.dataset_etapas_pipeline import (
    run_complicacao_pipeline_aplicar_status_no_dataset,
    run_complicacao_pipeline_criar_dataset_base,
    run_internacao_eletivo_pipeline_aplicar_status_no_dataset,
    run_internacao_eletivo_pipeline_criar_dataset_base,
)
from src.pipelines.internacao_eletivo_orquestracao_pipeline import (
    run_internacao_eletivo_pipeline_orquestrar,
)
from src.pipelines.join_status_resposta_pipeline import (
    run_unificar_status_resposta_complicacao_pipeline,
    run_unificar_status_resposta_internacao_eletivo_pipeline,
)
from src.services.ingestao_service import executar_ingestao_complicacao, executar_normalizacao_padronizacao


class FluxoPartesController:
    @staticmethod
    def _build_resposta_output_path(saida_status: str) -> str:
        from pathlib import Path

        path = Path(saida_status)
        suffix = path.suffix.lower()
        if suffix in {".xlsx", ".xls"}:
            return str(path.with_name(f"{path.stem}_flow_resposta_limpo{suffix}"))
        if suffix == ".csv":
            return str(path.with_name(f"{path.stem}_flow_resposta_limpo.csv"))
        return str(path.with_name(f"{path.name}_flow_resposta_limpo.csv"))

    def _run_ingestao_internacao_fluxo_unificado(
        self,
        arquivo_status: str,
        arquivo_flow_resposta_internacao_eletivo: str,
        saida_status: str,
    ) -> dict:
        saida_status_resposta = self._build_resposta_output_path(saida_status)
        resultado = executar_normalizacao_padronizacao(
            arquivo_status=arquivo_status,
            arquivo_status_resposta=arquivo_flow_resposta_internacao_eletivo,
            saida_status=saida_status,
            saida_status_resposta=saida_status_resposta,
            contexto="internacao_eletivo",
        )
        if isinstance(resultado, dict):
            resultado["mensagens"] = resultado.get("mensagens", []) + [
                f"Flow resposta limpo salvo em: {saida_status_resposta}",
            ]
        return resultado

    def get_specs(self) -> dict[str, dict[str, dict]]:
        return {
            "complicacao": {
                "ingestao_normalizacao": {
                    "title": "Complicacao - Ingestao / Normalizacao",
                    "fields": [
                        ("arquivo_status", "Arquivo Status"),
                        ("arquivo_status_resposta_complicacao", "Arquivo Flow de Respostas"),
                        ("saida_status", "Salvar Status Limpo"),
                        ("saida_status_resposta", "Salvar Flow Resposta Limpo"),
                    ],
                    "run": executar_ingestao_complicacao,
                },
                "uniao_status": {
                    "title": "Complicacao - Uniao de Status",
                    "fields": [
                        ("arquivo_status", "Status Limpo"),
                        ("arquivo_status_resposta", "Flow Limpo"),
                        ("arquivo_saida", "Salvar Status Integrado"),
                    ],
                    "run": run_unificar_status_resposta_complicacao_pipeline,
                },
                "criar_dataset": {
                    "title": "Complicacao - Criar Dataset",
                    "fields": [
                        ("arquivo_origem_dataset", "Arquivo Base (Complicacao)"),
                        ("arquivo_saida_dataset", "Salvar Dataset Base"),
                    ],
                    "run": run_complicacao_pipeline_criar_dataset_base,
                },
                "enviar_status_dataset": {
                    "title": "Complicacao - Enviar Status para Dataset",
                    "fields": [
                        ("arquivo_dataset_base", "Arquivo Dataset Base"),
                        ("arquivo_status_integrado", "Arquivo Status Integrado"),
                        ("arquivo_saida_dataset", "Salvar Dataset com Status"),
                    ],
                    "run": run_complicacao_pipeline_aplicar_status_no_dataset,
                },
                "orquestrar": {
                    "title": "Complicacao - Somente Orquestrar",
                    "fields": [
                        ("arquivo_dataset_status", "Dataset Status"),
                        ("arquivo_saida_final", "Salvar Dataset Final"),
                    ],
                    "run": run_complicacao_pipeline_orquestrar,
                },
            },
            "internacao": {
                "ingestao_normalizacao": {
                    "title": "Internacao - Ingestao / Normalizacao",
                    "fields": [
                        ("arquivo_status", "Arquivo Status"),
                        ("arquivo_flow_resposta_internacao_eletivo", "Flow Resposta Internacao Eletivo"),
                        ("saida_status", "Salvar Status Limpo"),
                    ],
                    "run": self._run_ingestao_internacao_fluxo_unificado,
                },
                "uniao_status": {
                    "title": "Internacao - Uniao de Status",
                    "fields": [
                        ("arquivo_status", "Status Limpo"),
                        ("arquivo_status_resposta", "Flow Limpo"),
                        ("arquivo_saida", "Salvar Status Integrado"),
                    ],
                    "run": run_unificar_status_resposta_internacao_eletivo_pipeline,
                },
                "criar_dataset": {
                    "title": "Internacao - Criar Dataset",
                    "fields": [
                        ("arquivo_origem_dataset", "Arquivo Base (Internacao)"),
                        ("arquivo_saida_dataset", "Salvar Dataset Base"),
                    ],
                    "run": run_internacao_eletivo_pipeline_criar_dataset_base,
                },
                "enviar_status_dataset": {
                    "title": "Internacao - Enviar Status para Dataset",
                    "fields": [
                        ("arquivo_dataset_base", "Arquivo Dataset Base"),
                        ("arquivo_status_integrado", "Arquivo Status Integrado"),
                        ("arquivo_saida_dataset", "Salvar Dataset com Status"),
                    ],
                    "run": run_internacao_eletivo_pipeline_aplicar_status_no_dataset,
                },
                "orquestrar": {
                    "title": "Internacao - Somente Orquestrar",
                    "fields": [
                        ("arquivo_dataset_status", "Dataset Status"),
                        ("arquivo_saida_final", "Salvar Dataset Final"),
                    ],
                    "run": run_internacao_eletivo_pipeline_orquestrar,
                },
            },
        }

    @staticmethod
    def resolve_execution_request(
        file_values: dict[str, str],
        file_labels: dict[str, str],
    ) -> str | None:
        faltantes = [file_labels[k] for k in file_labels if not file_values.get(k, "").strip()]
        if faltantes:
            return "Arquivos obrigatorios ausentes: " + ", ".join(faltantes) + "."
        return None

    @staticmethod
    def default_output_filename(context: str, action: str, key: str) -> str:
        if key == "saida_status":
            return "status_internacao_eletivo_limpo.csv" if context == "internacao" else "status_complicacao_limpo.csv"
        if key == "saida_status_resposta":
            return (
                "flow_resposta_eletivo_internacao_limpo.csv"
                if context == "internacao"
                else "flow_resposta_complicacao_limpo.csv"
            )
        if key == "arquivo_status_resposta_unificado":
            return "flow_resposta_eletivo_internacao.csv"
        if key == "arquivo_saida":
            if action == "uniao_status":
                return "status_internacao_eletivo.csv" if context == "internacao" else "status_complicacao.csv"
            return "arquivo_saida.csv"
        if key == "arquivo_saida_dataset":
            if action == "criar_dataset":
                return "internacao_dataset_base.xlsx" if context == "internacao" else "complicacao_dataset_base.xlsx"
            return "internacao_status.xlsx" if context == "internacao" else "complicacao_status.xlsx"
        if key == "arquivo_saida_final":
            return "internacao_final.xlsx" if context == "internacao" else "complicacao_final.xlsx"
        return "saida.csv"
