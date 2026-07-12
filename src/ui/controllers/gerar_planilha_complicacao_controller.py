from __future__ import annotations

from pathlib import Path


class GerarPlanilhaComplicacaoController:
    EXTENSOES_EXCEL = {".xlsx", ".xls"}

    def resolve_execution_request(
        self,
        file_values: dict[str, str],
        file_labels: dict[str, str],
    ) -> tuple[dict[str, Path], str | None]:
        obrigatorios = [
            "arquivo_base",
            "arquivo_telefones",
            "arquivo_utilidade",
            "output_dir",
        ]
        faltantes = [
            file_labels.get(key, key)
            for key in obrigatorios
            if not file_values.get(key, "").strip()
        ]
        if faltantes:
            return {}, "Arquivos obrigatorios ausentes: " + ", ".join(faltantes) + "."

        arquivo_base = Path(file_values["arquivo_base"])
        arquivo_telefones = Path(file_values["arquivo_telefones"])
        arquivo_utilidade = Path(file_values["arquivo_utilidade"])
        output_dir = Path(file_values["output_dir"])

        erro = self._validar_arquivo(arquivo_base, "Planilha complicacao base", self.EXTENSOES_EXCEL)
        if erro:
            return {}, erro

        erro = self._validar_arquivo(arquivo_telefones, "Telefones CSV", {".csv"})
        if erro:
            return {}, erro

        erro = self._validar_arquivo(arquivo_utilidade, "Utilidade", self.EXTENSOES_EXCEL)
        if erro:
            return {}, erro

        return {
            "arquivo_base": arquivo_base,
            "arquivo_telefones": arquivo_telefones,
            "arquivo_utilidade": arquivo_utilidade,
            "output_dir": output_dir,
            "arquivo_saida": output_dir / "complicacao.xlsx",
        }, None

    @staticmethod
    def _validar_arquivo(caminho: Path, label: str, extensoes: set[str]) -> str | None:
        if not caminho.exists():
            return f"{label} nao encontrado: {caminho}"
        if caminho.suffix.lower() not in extensoes:
            permitidas = ", ".join(sorted(extensoes))
            return f"{label} deve ter extensao {permitidas}: {caminho}"
        return None
