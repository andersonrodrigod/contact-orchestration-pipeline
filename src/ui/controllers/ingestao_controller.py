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
    def _output_extension(file_values: dict[str, str]) -> str:
        for key in ("arquivo_status", "arquivo_status_resposta"):
            valor = (file_values.get(key) or "").strip().lower()
            if valor.endswith(".xlsx") or valor.endswith(".xls"):
                return ".xlsx"
        return ".csv"

    @classmethod
    def _default_output_filename(
        cls,
        mode: str,
        kind: str,
        file_values: dict[str, str],
    ) -> str:
        ext = cls._output_extension(file_values)
        if mode == "internacao":
            if kind == "status":
                return f"status_internacao_eletivo_limpo{ext}"
            return f"status_resposta_eletivo_internacao_limpo{ext}"
        if kind == "status":
            return f"status_complicacao_limpo{ext}"
        return f"status_resposta_complicacao_limpo{ext}"

    @classmethod
    def build_output_path(
        cls,
        mode: str,
        pasta_saida: str,
        kind: str,
        file_values: dict[str, str],
    ) -> str:
        return str(Path(pasta_saida) / cls._default_output_filename(mode, kind, file_values))

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
        tem_pasta_saida = bool(file_values.get("pasta_saida", "").strip())

        if not tem_status and not tem_resposta:
            return {}, (
                "Informe pelo menos um arquivo de entrada: "
                f"{file_labels.get('arquivo_status', 'arquivo_status')} ou "
                f"{file_labels.get('arquivo_status_resposta', 'arquivo_status_resposta')}."
            )

        if (tem_status or tem_resposta) and not tem_pasta_saida:
            return {}, (
                f"Arquivo obrigatorio ausente: "
                f"{file_labels.get('pasta_saida', 'pasta_saida')}."
            )

        contexto = "internacao_eletivo" if mode == "internacao" else "complicacao"
        if tem_status and tem_resposta:
            return {"tipo": "completo", "contexto": contexto, "mode": mode}, None
        if tem_status:
            return {"tipo": "status", "contexto": contexto, "mode": mode}, None
        return {"tipo": "resposta", "contexto": contexto, "mode": mode}, None

    def run(self, plan: dict[str, str], file_values: dict[str, str]) -> dict:
        tipo = plan["tipo"]
        contexto = plan["contexto"]
        pasta_saida = file_values["pasta_saida"]
        saida_status = self.build_output_path(plan["mode"], pasta_saida, "status", file_values)
        saida_status_resposta = self.build_output_path(
            plan["mode"], pasta_saida, "resposta", file_values
        )

        if tipo == "completo":
            return executar_normalizacao_padronizacao(
                arquivo_status=file_values["arquivo_status"],
                arquivo_status_resposta=file_values["arquivo_status_resposta"],
                saida_status=saida_status,
                saida_status_resposta=saida_status_resposta,
                contexto=contexto,
            )

        if tipo == "status":
            return executar_ingestao_somente_status(
                arquivo_status=file_values["arquivo_status"],
                saida_status=saida_status,
                contexto=contexto,
            )

        resultado = self._normalizar_status_resposta_somente(
            arquivo_status_resposta=file_values["arquivo_status_resposta"],
            saida_status_resposta=saida_status_resposta,
        )
        return resultado
