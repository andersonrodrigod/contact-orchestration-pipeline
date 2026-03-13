from __future__ import annotations


class AmbosController:
    def __init__(self) -> None:
        self._ack_missing_respostas = False

    def reset_response_warning_flags(self) -> None:
        self._ack_missing_respostas = False

    def resolve_execution_plan(
        self,
        file_values: dict[str, str],
        file_labels: dict[str, str],
    ) -> tuple[dict[str, str], str | None]:
        obrigatorios = ["complicacao", "internacao", "status"]
        faltantes_obrigatorios = [
            file_labels[k] for k in obrigatorios if not file_values.get(k, "").strip()
        ]
        if faltantes_obrigatorios:
            return {}, (
                "Arquivos obrigatórios ausentes: " + ", ".join(faltantes_obrigatorios) + "."
            )

        tem_resposta_complicacao = bool(file_values.get("flow_complicacao", "").strip())
        tem_resposta_internacao_eletivo = bool(
            file_values.get("flow_internacao_eletivo", "").strip()
        )
        tem_resposta_internacao_urgencia = bool(
            file_values.get("flow_internacao_urgencia", "").strip()
        )

        qtd_flows_preenchidos = sum(
            [
                tem_resposta_complicacao,
                tem_resposta_internacao_eletivo,
                tem_resposta_internacao_urgencia,
            ]
        )

        if qtd_flows_preenchidos not in (0, 3):
            return {}, (
                "Para executar, informe os 3 arquivos de flow (Complicações, "
                "Internação Eletivo e Internação Urgência) ou não informe nenhum."
            )

        plano_execucao = {
            "complicacao": "com_resposta" if qtd_flows_preenchidos == 3 else "somente_status",
            "internacao": "com_resposta" if qtd_flows_preenchidos == 3 else "somente_status",
        }
        return plano_execucao, None

    def needs_missing_responses_confirmation(self, plano_execucao: dict[str, str]) -> bool:
        return (
            plano_execucao.get("complicacao") == "somente_status"
            and plano_execucao.get("internacao") == "somente_status"
            and not self._ack_missing_respostas
        )

    def ack_missing_responses_confirmation(self) -> None:
        self._ack_missing_respostas = True

    @staticmethod
    def missing_responses_warning() -> tuple[str, str]:
        return (
            "Execução sem arquivos de resposta",
            (
                "Você está ciente que se executar nesse formato irá perder os "
                "dados de Respostas dos clientes?\n\n"
                "Complicação e Internação serão executados em modo somente status."
            ),
        )
