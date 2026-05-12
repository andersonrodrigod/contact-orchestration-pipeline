from __future__ import annotations


class PipelineRunner:
    @staticmethod
    def build_complicacao_steps(plano_execucao: dict[str, str]) -> list[str]:
        steps = ["Validando preflight de arquivos e colunas..."]

        if plano_execucao["complicacao"] == "com_resposta":
            steps.append("Complicação: normalizando status e flow de resposta...")
            steps.append("Complicação: integrando status com resposta...")
        else:
            steps.append("Complicação: executando fluxo somente status...")

        steps.append("Complicação: gerando dataset status...")
        steps.append("Complicação: executando orquestração...")
        steps.append("Consolidando métricas e artefatos finais...")
        steps.append("Finalizando execução e registrando histórico...")
        return steps
