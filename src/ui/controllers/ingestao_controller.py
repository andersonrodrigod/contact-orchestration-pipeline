from __future__ import annotations

from pathlib import Path

from src.services.ingestao_service import (
    executar_ingestao_somente_status,
    executar_normalizacao_padronizacao,
)
from src.services.normalizacao_services import (
    formatar_coluna_data_br,
    limpar_texto_colunas_alvo,
    normalizar_tipos_dataframe,
)
from src.services.padronizacao_service import padronizar_colunas_status_resposta
from src.utils.arquivos import ler_arquivo_csv, salvar_dataframe


class IngestaoController:
    @staticmethod
    def default_output_filename(mode: str, key: str) -> str:
        if mode == "internacao":
            if key == "saida_status":
                return "status_internacao_eletivo_limpo.csv"
            return "status_resposta_eletivo_internacao_limpo.csv"
        if key == "saida_status":
            return "status_complicacao_limpo.csv"
        return "status_resposta_complicacao_limpo.csv"

    @staticmethod
    def _normalizar_status_resposta_somente(
        arquivo_status_resposta: str,
        saida_status_resposta: str,
    ) -> dict:
        df = ler_arquivo_csv(arquivo_status_resposta)
        df = padronizar_colunas_status_resposta(df)
        df = normalizar_tipos_dataframe(df, colunas_data=["DT_ATENDIMENTO"])
        df = limpar_texto_colunas_alvo(
            df,
            colunas_alvo=["HSM", "Status", "Respondido", "resposta"],
        )
        formatar_coluna_data_br(df, "DT_ATENDIMENTO")
        salvar_dataframe(df, saida_status_resposta)
        return {
            "ok": True,
            "arquivo_saida": saida_status_resposta,
            "mensagens": ["Normalizacao de status_resposta executada com sucesso."],
        }

    def resolve_execution_plan(
        self,
        mode: str,
        file_values: dict[str, str],
        file_labels: dict[str, str],
    ) -> tuple[dict[str, str], str | None]:
        tem_status = bool(file_values.get("arquivo_status", "").strip())
        tem_resposta = bool(file_values.get("arquivo_status_resposta", "").strip())
        tem_saida_status = bool(file_values.get("saida_status", "").strip())
        tem_saida_resposta = bool(file_values.get("saida_status_resposta", "").strip())

        if not tem_status and not tem_resposta:
            return {}, (
                "Informe pelo menos um arquivo de entrada: "
                f"{file_labels.get('arquivo_status', 'arquivo_status')} ou "
                f"{file_labels.get('arquivo_status_resposta', 'arquivo_status_resposta')}."
            )

        if tem_status and not tem_saida_status:
            return {}, f"Arquivo obrigatorio ausente: {file_labels.get('saida_status', 'saida_status')}."

        if tem_resposta and not tem_saida_resposta:
            return {}, (
                f"Arquivo obrigatorio ausente: "
                f"{file_labels.get('saida_status_resposta', 'saida_status_resposta')}."
            )

        contexto = "internacao_eletivo" if mode == "internacao" else "complicacao"
        if tem_status and tem_resposta:
            return {"tipo": "completo", "contexto": contexto}, None
        if tem_status:
            return {"tipo": "status", "contexto": contexto}, None
        return {"tipo": "resposta", "contexto": contexto}, None

    def run(self, plan: dict[str, str], file_values: dict[str, str]) -> dict:
        tipo = plan["tipo"]
        contexto = plan["contexto"]

        if tipo == "completo":
            return executar_normalizacao_padronizacao(
                arquivo_status=file_values["arquivo_status"],
                arquivo_status_resposta=file_values["arquivo_status_resposta"],
                saida_status=file_values["saida_status"],
                saida_status_resposta=file_values["saida_status_resposta"],
                contexto=contexto,
            )

        if tipo == "status":
            return executar_ingestao_somente_status(
                arquivo_status=file_values["arquivo_status"],
                saida_status=file_values["saida_status"],
                contexto=contexto,
            )

        resultado = self._normalizar_status_resposta_somente(
            arquivo_status_resposta=file_values["arquivo_status_resposta"],
            saida_status_resposta=file_values["saida_status_resposta"],
        )
        return resultado
