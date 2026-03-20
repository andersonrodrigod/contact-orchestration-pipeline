from __future__ import annotations


class ConcatenarController:
    def resolve_execution_request(
        self,
        mode: str,
        file_values: dict[str, str],
        file_labels: dict[str, str],
    ) -> tuple[dict[str, str], str | None]:
        faltantes = [file_labels[k] for k in file_labels if not file_values.get(k, "").strip()]
        if faltantes:
            return {}, "Arquivos obrigatórios ausentes: " + ", ".join(faltantes) + "."

        return {"mode": mode}, None
