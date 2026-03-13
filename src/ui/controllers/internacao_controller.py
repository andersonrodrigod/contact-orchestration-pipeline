from __future__ import annotations


class InternacaoController:
    def __init__(self) -> None:
        self._ack_missing_resposta = False

    def reset_response_warning_flags(self) -> None:
        self._ack_missing_resposta = False

    def resolve_execution_plan(
        self,
        file_values: dict[str, str],
        file_labels: dict[str, str],
    ) -> tuple[dict[str, str], str | None]:
        obrigatorios = ["internacao_dataset", "status"]
        faltantes_obrigatorios = [
            file_labels[k] for k in obrigatorios if not file_values.get(k, "").strip()
        ]
        if faltantes_obrigatorios:
            return {}, (
                "Arquivos obrigatórios ausentes: " + ", ".join(faltantes_obrigatorios) + "."
            )

        tem_eletivo = bool(file_values.get("flow_internacao_eletivo", "").strip())
        tem_urgencia = bool(file_values.get("flow_internacao_urgencia", "").strip())

        if tem_eletivo != tem_urgencia:
            return {}, (
                "Para Internação, informe os dois arquivos de flow "
                "(Eletivo e Urgência) ou não informe nenhum."
            )

        plano_execucao = {
            "internacao": "com_resposta" if (tem_eletivo and tem_urgencia) else "somente_status",
        }
        return plano_execucao, None

    def needs_missing_response_confirmation(self, plano_execucao: dict[str, str]) -> bool:
        return (
            plano_execucao.get("internacao") == "somente_status"
            and not self._ack_missing_resposta
        )

    def ack_missing_response_confirmation(self) -> None:
        self._ack_missing_resposta = True

    @staticmethod
    def missing_response_warning() -> tuple[str, str]:
        return (
            "Internação sem resposta",
            (
                "Você está ciente que se executar nesse formato irá perder os "
                "dados de Respostas dos clientes?\n\n"
                "Internação será executado em modo somente status."
            ),
        )
