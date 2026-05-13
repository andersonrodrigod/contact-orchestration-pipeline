from __future__ import annotations


class ComplicacaoController:
    def __init__(self) -> None:
        self._ack_missing_resposta = False

    def reset_response_warning_flags(self) -> None:
        self._ack_missing_resposta = False

    def resolve_execution_plan(
        self,
        file_values: dict[str, str],
        file_labels: dict[str, str],
    ) -> tuple[dict[str, str], str | None]:
        obrigatorios = ["complicacao_dataset", "status", "flow_complicacao"]
        faltantes_obrigatorios = [
            file_labels[k] for k in obrigatorios if not file_values.get(k, "").strip()
        ]
        if faltantes_obrigatorios:
            return {}, (
                "Arquivos obrigatórios ausentes: " + ", ".join(faltantes_obrigatorios) + "."
            )

        plano_execucao = {"complicacao": "com_resposta"}
        return plano_execucao, None

    def needs_missing_response_confirmation(self, plano_execucao: dict[str, str]) -> bool:
        return False

    def ack_missing_response_confirmation(self) -> None:
        self._ack_missing_resposta = True

    @staticmethod
    def missing_response_warning() -> tuple[str, str]:
        return (
            "Resposta obrigatoria",
            "O fluxo de complicacao agora exige status e status_resposta.",
        )
