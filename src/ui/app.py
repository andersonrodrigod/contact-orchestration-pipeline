from __future__ import annotations

from pathlib import Path
import threading
from tkinter import filedialog
from typing import Callable

from src.ui.controllers.ambos_controller import AmbosController
from src.ui.controllers.concatenar_controller import ConcatenarController
from src.ui.controllers.complicacao_controller import ComplicacaoController
from src.ui.controllers.fluxo_partes_controller import FluxoPartesController
from src.ui.controllers.ingestao_controller import IngestaoController
from src.ui.controllers.internacao_controller import InternacaoController
from src.ui.controllers.uniao_status_controller import UniaoStatusController
from src.pipelines.concatenar_livre_pipeline import run_unificar_arquivos_livre_pipeline
from src.pipelines.concatenar_status_pipeline import run_unificar_status_pipeline
from src.pipelines.concatenar_status_respostas_pipeline import (
    run_unificar_status_respostas_pipeline,
)
from src.pipelines.join_status_resposta_pipeline import (
    run_unificar_status_resposta_complicacao_pipeline,
    run_unificar_status_resposta_internacao_eletivo_pipeline,
)
from src.ui.services.pipeline_runner import PipelineRunner
from src.ui.state import UIStyle, UIRuntimeState
from src.ui.views.ambos_view import AmbosView
from src.ui.views.concatenar_view import ConcatenarView
from src.ui.views.complicacao_view import ComplicacaoView
from src.ui.views.fluxo_partes_view import FluxoPartesView
from src.ui.views.ingestao_view import IngestaoView
from src.ui.views.internacao_view import InternacaoView
from src.ui.views.menu_view import MenuView
from src.ui.views.progress_modal import ProgressModal
from src.ui.views.uniao_status_view import UniaoStatusView

try:
    import customtkinter as ctk
except ImportError as erro:  # pragma: no cover
    raise ImportError(
        "customtkinter nao encontrado. Instale com: pip install customtkinter"
    ) from erro

try:
    from PIL import Image
except ImportError as erro:  # pragma: no cover
    raise ImportError("Pillow nao encontrado. Instale com: pip install pillow") from erro


class App(ctk.CTk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Menu Principal")
        self.geometry("1320x840")
        self.minsize(1176, 730)

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.style = UIStyle()
        self.ui_state = UIRuntimeState()
        self.ambos_controller = AmbosController()
        self.concatenar_controller = ConcatenarController()
        self.complicacao_controller = ComplicacaoController()
        self.fluxo_partes_controller = FluxoPartesController()
        self.ingestao_controller = IngestaoController()
        self.internacao_controller = InternacaoController()
        self.uniao_status_controller = UniaoStatusController()
        self.pipeline_runner = PipelineRunner()

        self._icon_refs: list[ctk.CTkImage] = []
        self.frames: dict[str, ctk.CTkFrame] = {}

        self.menu_view: MenuView | None = None
        self.ambos_view: AmbosView | None = None
        self.concatenar_view: ConcatenarView | None = None
        self.complicacao_view: ComplicacaoView | None = None
        self.fluxo_partes_view: FluxoPartesView | None = None
        self.ingestao_view: IngestaoView | None = None
        self.internacao_view: InternacaoView | None = None
        self.uniao_status_view: UniaoStatusView | None = None
        self.progress_modal: ProgressModal | None = None

        self._etl_cancelled = False
        self._etl_after_id: str | None = None
        self._warning_modal: ctk.CTkToplevel | None = None
        self._etl_steps: list[str] = []
        self._current_execution_context: str = "ambos"

        self._build_ui()

    def _build_ui(self) -> None:
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self.container = ctk.CTkFrame(self, fg_color="#101A35", corner_radius=0)
        self.container.grid(row=0, column=0, sticky="nsew")
        self.container.grid_rowconfigure(0, weight=1)
        self.container.grid_columnconfigure(0, weight=1)

        self._create_menu_frame()
        self._create_frame_fluxo_completo()
        self._create_frame_modo_complicacao()
        self._create_frame_modo_internacao()
        self._create_frame_fluxo_partes()
        self._create_frame_juntar_status()
        self._create_frame_concatenar()
        self._create_frame_limpeza_dados()
        self._create_frame_utilitario()
        self._create_frame_configuracoes()

        self.show_frame("menu_frame")

    def show_frame(self, frame_name: str) -> None:
        for frame in self.frames.values():
            frame.grid_remove()
        if frame_name in self.frames:
            self.frames[frame_name].grid(row=0, column=0, sticky="nsew")

    def _build_base_content(self, frame: ctk.CTkFrame, titulo: str) -> None:
        frame.grid_columnconfigure(0, weight=1)
        frame.grid_rowconfigure(0, weight=1)
        title = ctk.CTkLabel(
            frame,
            text=titulo,
            font=ctk.CTkFont(size=28, weight="bold"),
            text_color="#e8eeff",
        )
        title.place(relx=0.5, rely=0.42, anchor="center")

        back_shadow = ctk.CTkFrame(
            frame,
            width=220,
            height=44,
            corner_radius=self.style.btn_corner_radius,
            fg_color=self.style.btn_shadow_color,
        )
        back_shadow.place(relx=0.5, rely=0.524, anchor="center")

        back_btn = ctk.CTkButton(
            frame,
            text="Voltar ao Menu",
            font=ctk.CTkFont(family="Segoe UI", size=16, weight="bold"),
            width=220,
            height=44,
            fg_color=self.style.btn_fg_color,
            hover_color=self.style.btn_hover_color,
            border_color=self.style.btn_border_color,
            text_color=self.style.btn_text_color,
            corner_radius=self.style.btn_corner_radius,
            border_width=self.style.btn_border_width,
            command=lambda: self.show_frame("menu_frame"),
        )
        back_btn.place(relx=0.5, rely=0.52, anchor="center")

    def _create_menu_frame(self) -> None:
        self.menu_view = MenuView(
            parent=self.container,
            style=self.style,
            load_icon=self._load_icon,
            on_navigate=self.show_frame,
        )
        self.frames["menu_frame"] = self.menu_view

    def _create_frame_fluxo_completo(self) -> None:
        self.ambos_view = AmbosView(
            parent=self.container,
            style=self.style,
            on_back=lambda: self.show_frame("menu_frame"),
            on_execute=self._start_etl_execution,
            on_select_file=self._select_file,
            on_clear_file=self._clear_file,
        )
        self.frames["frame_fluxo_completo"] = self.ambos_view

    def _create_frame_modo_complicacao(self) -> None:
        self.complicacao_view = ComplicacaoView(
            parent=self.container,
            style=self.style,
            on_back=lambda: self.show_frame("menu_frame"),
            on_execute=self._start_complicacao_execution,
            on_select_file=self._select_file_complicacao,
            on_clear_file=self._clear_file_complicacao,
        )
        self.frames["frame_modo_complicacao"] = self.complicacao_view

    def _create_frame_modo_internacao(self) -> None:
        self.internacao_view = InternacaoView(
            parent=self.container,
            style=self.style,
            on_back=lambda: self.show_frame("menu_frame"),
            on_execute=self._start_internacao_execution,
            on_select_file=self._select_file_internacao,
            on_clear_file=self._clear_file_internacao,
        )
        self.frames["frame_modo_internacao"] = self.internacao_view

    def _select_file_concatenar(self, mode: str, key: str) -> None:
        if self.concatenar_view is None:
            return
        labels = self.concatenar_view.get_file_labels(mode)
        if key == "arquivo_saida":
            selected_dir = filedialog.askdirectory(
                title=f"Selecionar pasta - {labels.get(key, key)}"
            )
            if selected_dir:
                path = str(Path(selected_dir) / self._default_output_filename_concatenar(mode))
            else:
                path = ""
        else:
            path = filedialog.askopenfilename(
                title=f"Selecionar arquivo - {labels.get(key, key)}",
                filetypes=[
                    ("CSV e Excel", "*.csv;*.xlsx;*.xls"),
                    ("CSV", "*.csv"),
                    ("Excel", "*.xlsx;*.xls"),
                    ("Todos os arquivos", "*.*"),
                ],
            )

        if path:
            self.concatenar_view.set_file_value(mode, key, path)
            self.concatenar_view.clear_status_message(mode)

    @staticmethod
    def _default_output_filename_concatenar(mode: str) -> str:
        if mode == "status":
            return "status_complicacao_internacao_eletivo.csv"
        if mode == "status_resposta":
            return "status_resposta_eletivo_internacao.csv"
        return "concatenacao_livre.csv"

    def _clear_file_concatenar(self, mode: str, key: str) -> None:
        if self.concatenar_view is None:
            return
        self.concatenar_view.clear_file_value(mode, key)
        self.concatenar_view.clear_status_message(mode)

    def _start_concatenar_execution(self, mode: str) -> None:
        if self.concatenar_view is None:
            return

        file_values = self.concatenar_view.get_file_values(mode)
        file_labels = self.concatenar_view.get_file_labels(mode)
        _, erro_validacao = self.concatenar_controller.resolve_execution_request(
            mode=mode,
            file_values=file_values,
            file_labels=file_labels,
        )
        if erro_validacao:
            self.concatenar_view.set_status_message(mode, erro_validacao, "#FFB1B1")
            return

        try:
            if mode == "status":
                arquivo_saida = file_values["arquivo_saida"]
                saida_norm = self._build_normalized_output_path(arquivo_saida)
                resultado = run_unificar_status_pipeline(
                    arquivo_status_complicacao=file_values["status_complicacao"],
                    arquivo_status_internacao_eletivo=file_values["status_internacao_eletivo"],
                    arquivo_saida=arquivo_saida,
                    arquivo_saida_normalizado=saida_norm,
                )
            elif mode == "status_resposta":
                resultado = run_unificar_status_respostas_pipeline(
                    arquivo_eletivo=file_values["resposta_eletivo"],
                    arquivo_internacao=file_values["resposta_internacao"],
                    arquivo_saida=file_values["arquivo_saida"],
                )
            else:
                resultado = run_unificar_arquivos_livre_pipeline(
                    arquivo_a=file_values["arquivo_a"],
                    arquivo_b=file_values["arquivo_b"],
                    arquivo_saida=file_values["arquivo_saida"],
                )
        except Exception as erro:
            self.concatenar_view.set_status_message(
                mode,
                f"Falha na concatenação: {type(erro).__name__}: {erro}",
                "#FFB1B1",
            )
            return

        if resultado.get("ok", False):
            self.concatenar_view.set_status_message(
                mode,
                "Concatenação concluída com sucesso.",
                "#AEE3B8",
            )
            return

        mensagens = resultado.get("mensagens", [])
        mensagem = mensagens[0] if mensagens else "Falha na concatenação."
        self.concatenar_view.set_status_message(mode, mensagem, "#FFB1B1")

    @staticmethod
    def _build_normalized_output_path(arquivo_saida: str) -> str:
        path = Path(arquivo_saida)
        suffix = path.suffix.lower()
        if suffix in {".xlsx", ".xls"}:
            return str(path.with_name(f"{path.stem}_normalizado{suffix}"))
        if suffix == ".csv":
            return str(path.with_name(f"{path.stem}_normalizado.csv"))
        return str(path.with_name(f"{path.name}_normalizado.csv"))

    def _get_fluxo_partes_specs(self) -> dict[str, dict[str, dict]]:
        return self.fluxo_partes_controller.get_specs()

    def _select_file_fluxo_partes(self, context: str, action: str, key: str) -> None:
        if self.fluxo_partes_view is None:
            return
        labels = self.fluxo_partes_view.get_file_labels(context, action)
        if key.startswith("saida_") or key.startswith("arquivo_saida"):
            selected_dir = filedialog.askdirectory(
                title=f"Selecionar pasta - {labels.get(key, key)}"
            )
            if selected_dir:
                filename = self.fluxo_partes_controller.default_output_filename(context, action, key)
                path = str(Path(selected_dir) / filename)
            else:
                path = ""
        else:
            path = filedialog.askopenfilename(
                title=f"Selecionar arquivo - {labels.get(key, key)}",
                filetypes=[
                    ("CSV e Excel", "*.csv;*.xlsx;*.xls"),
                    ("CSV", "*.csv"),
                    ("Excel", "*.xlsx;*.xls"),
                    ("Todos os arquivos", "*.*"),
                ],
            )
        if path:
            self.fluxo_partes_view.set_file_value(context, action, key, path)
            self.fluxo_partes_view.clear_status_message(context, action)

    def _clear_file_fluxo_partes(self, context: str, action: str, key: str) -> None:
        if self.fluxo_partes_view is None:
            return
        self.fluxo_partes_view.clear_file_value(context, action, key)
        self.fluxo_partes_view.clear_status_message(context, action)

    def _start_fluxo_partes_execution(self, context: str, action: str) -> None:
        if self.fluxo_partes_view is None:
            return

        specs = self._get_fluxo_partes_specs()
        action_spec = specs.get(context, {}).get(action)
        if action_spec is None:
            self.fluxo_partes_view.set_status_message(context, action, "Acao desconhecida.", "#FFB1B1")
            return

        file_values = self.fluxo_partes_view.get_file_values(context, action)
        file_labels = self.fluxo_partes_view.get_file_labels(context, action)
        erro_validacao = self.fluxo_partes_controller.resolve_execution_request(
            file_values=file_values,
            file_labels=file_labels,
        )
        if erro_validacao:
            self.fluxo_partes_view.set_status_message(
                context,
                action,
                erro_validacao,
                "#FFB1B1",
            )
            return

        self.fluxo_partes_view.set_status_message(
            context,
            action,
            f"Executando: {action_spec['title']}...",
            "#A7C8FF",
        )
        threading.Thread(
            target=self._run_fluxo_partes_worker,
            args=(context, action, action_spec["title"], action_spec["run"], file_values),
            daemon=True,
        ).start()

    def _run_fluxo_partes_worker(
        self,
        context: str,
        action: str,
        action_title: str,
        runner: Callable[..., dict],
        kwargs: dict[str, str],
    ) -> None:
        try:
            result = runner(**kwargs)
            if not isinstance(result, dict):
                result = {"ok": False, "mensagens": [f"Retorno invalido: {type(result).__name__}"]}
        except Exception as erro:
            message = f"Falha em {action_title}: {type(erro).__name__}: {erro}"
            self.after(0, lambda: self._set_fluxo_partes_status(context, action, message, "#FFB1B1"))
            return

        ok = bool(result.get("ok", False))
        mensagens = result.get("mensagens", [])
        detalhe = mensagens[0] if mensagens else ("Concluido com sucesso." if ok else "Falha na execucao.")
        color = "#AEE3B8" if ok else "#FFB1B1"
        self.after(
            0,
            lambda: self._set_fluxo_partes_status(context, action, f"{action_title}: {detalhe}", color),
        )

    def _set_fluxo_partes_status(self, context: str, action: str, text: str, color: str) -> None:
        if self.fluxo_partes_view is not None:
            self.fluxo_partes_view.set_status_message(context, action, text, color)

    def _select_file_ingestao(self, mode: str, key: str) -> None:
        if self.ingestao_view is None:
            return
        labels = self.ingestao_view.get_file_labels(mode)
        if key.startswith("saida_"):
            selected_dir = filedialog.askdirectory(
                title=f"Selecionar pasta - {labels.get(key, key)}"
            )
            if selected_dir:
                filename = self.ingestao_controller.default_output_filename(mode, key)
                path = str(Path(selected_dir) / filename)
            else:
                path = ""
        else:
            path = filedialog.askopenfilename(
                title=f"Selecionar arquivo - {labels.get(key, key)}",
                filetypes=[
                    ("CSV e Excel", "*.csv;*.xlsx;*.xls"),
                    ("CSV", "*.csv"),
                    ("Excel", "*.xlsx;*.xls"),
                    ("Todos os arquivos", "*.*"),
                ],
            )
        if path:
            self.ingestao_view.set_file_value(mode, key, path)
            self.ingestao_view.clear_status_message(mode)

    def _clear_file_ingestao(self, mode: str, key: str) -> None:
        if self.ingestao_view is None:
            return
        self.ingestao_view.clear_file_value(mode, key)
        self.ingestao_view.clear_status_message(mode)

    def _start_ingestao_execution(self, mode: str) -> None:
        if self.ingestao_view is None:
            return
        file_values = self.ingestao_view.get_file_values(mode)
        file_labels = self.ingestao_view.get_file_labels(mode)
        plan, erro_validacao = self.ingestao_controller.resolve_execution_plan(
            mode=mode,
            file_values=file_values,
            file_labels=file_labels,
        )
        if erro_validacao:
            self.ingestao_view.set_status_message(
                mode,
                erro_validacao,
                "#FFB1B1",
            )
            return

        self.ingestao_view.set_status_message(mode, "Executando ingestao...", "#A7C8FF")
        threading.Thread(
            target=self._run_ingestao_worker,
            args=(mode, plan, file_values),
            daemon=True,
        ).start()

    def _run_ingestao_worker(self, mode: str, plan: dict[str, str], file_values: dict[str, str]) -> None:
        try:
            result = self.ingestao_controller.run(plan, file_values)
            if not isinstance(result, dict):
                result = {"ok": False, "mensagens": [f"Retorno invalido: {type(result).__name__}"]}
        except Exception as erro:
            self.after(
                0,
                lambda: self._set_ingestao_status(
                    mode,
                    f"Falha na ingestao: {type(erro).__name__}: {erro}",
                    "#FFB1B1",
                ),
            )
            return

        ok = bool(result.get("ok", False))
        mensagens = result.get("mensagens", [])
        detalhe = mensagens[0] if mensagens else ("Concluido com sucesso." if ok else "Falha na execucao.")
        self.after(0, lambda: self._set_ingestao_status(mode, detalhe, "#AEE3B8" if ok else "#FFB1B1"))

    def _set_ingestao_status(self, mode: str, text: str, color: str) -> None:
        if self.ingestao_view is not None:
            self.ingestao_view.set_status_message(mode, text, color)

    def _select_file_uniao_status(self, mode: str, key: str) -> None:
        if self.uniao_status_view is None:
            return
        labels = self.uniao_status_view.get_file_labels(mode)
        if key == "arquivo_saida":
            selected_dir = filedialog.askdirectory(
                title=f"Selecionar pasta - {labels.get(key, key)}"
            )
            if selected_dir:
                filename = "status_internacao_eletivo.csv" if mode == "internacao" else "status_complicacao.csv"
                path = str(Path(selected_dir) / filename)
            else:
                path = ""
        else:
            path = filedialog.askopenfilename(
                title=f"Selecionar arquivo - {labels.get(key, key)}",
                filetypes=[
                    ("CSV e Excel", "*.csv;*.xlsx;*.xls"),
                    ("CSV", "*.csv"),
                    ("Excel", "*.xlsx;*.xls"),
                    ("Todos os arquivos", "*.*"),
                ],
            )

        if path:
            self.uniao_status_view.set_file_value(mode, key, path)
            self.uniao_status_view.clear_status_message(mode)

    def _clear_file_uniao_status(self, mode: str, key: str) -> None:
        if self.uniao_status_view is None:
            return
        self.uniao_status_view.clear_file_value(mode, key)
        self.uniao_status_view.clear_status_message(mode)

    def _start_uniao_status_execution(self, mode: str) -> None:
        if self.uniao_status_view is None:
            return

        file_values = self.uniao_status_view.get_file_values(mode)
        file_labels = self.uniao_status_view.get_file_labels(mode)
        _, erro_validacao = self.uniao_status_controller.resolve_execution_request(
            mode=mode,
            file_values=file_values,
            file_labels=file_labels,
        )
        if erro_validacao:
            self.uniao_status_view.set_status_message(mode, erro_validacao, "#FFB1B1")
            return

        try:
            if mode == "complicacao":
                resultado = run_unificar_status_resposta_complicacao_pipeline(
                    arquivo_status=file_values["arquivo_status"],
                    arquivo_status_resposta=file_values["arquivo_flow_resposta"],
                    arquivo_saida=file_values["arquivo_saida"],
                )
            else:
                resultado = run_unificar_status_resposta_internacao_eletivo_pipeline(
                    arquivo_status=file_values["arquivo_status"],
                    arquivo_status_resposta=file_values["arquivo_flow_resposta"],
                    arquivo_saida=file_values["arquivo_saida"],
                )
        except Exception as erro:
            self.uniao_status_view.set_status_message(
                mode,
                f"Falha na uniao: {type(erro).__name__}: {erro}",
                "#FFB1B1",
            )
            return

        if resultado.get("ok", False):
            self.uniao_status_view.set_status_message(
                mode,
                "Uniao concluida com sucesso.",
                "#AEE3B8",
            )
            return

        mensagens = resultado.get("mensagens", [])
        mensagem = mensagens[0] if mensagens else "Falha na uniao."
        self.uniao_status_view.set_status_message(mode, mensagem, "#FFB1B1")

    def _select_file(self, key: str) -> None:
        if self.ambos_view is None:
            return
        labels = self.ambos_view.get_file_labels()
        if key == "output_dir":
            path = filedialog.askdirectory(title="Selecionar pasta de saída")
        else:
            path = filedialog.askopenfilename(
                title=f"Selecionar arquivo - {labels.get(key, key)}"
            )
        if path:
            self.ambos_view.set_file_value(key, path)
            self._reset_response_warning_flags()
            self.ambos_view.clear_status_message()

    def _clear_file(self, key: str) -> None:
        if self.ambos_view is None:
            return
        self.ambos_view.clear_file_value(key)
        self._reset_response_warning_flags()

    def _select_file_complicacao(self, key: str) -> None:
        if self.complicacao_view is None:
            return
        labels = self.complicacao_view.get_file_labels()
        if key == "output_dir":
            path = filedialog.askdirectory(title="Selecionar pasta de saída")
        else:
            path = filedialog.askopenfilename(
                title=f"Selecionar arquivo - {labels.get(key, key)}"
            )
        if path:
            self.complicacao_view.set_file_value(key, path)
            self._reset_response_warning_flags()
            self.complicacao_view.clear_status_message()

    def _clear_file_complicacao(self, key: str) -> None:
        if self.complicacao_view is None:
            return
        self.complicacao_view.clear_file_value(key)
        self._reset_response_warning_flags()

    def _select_file_internacao(self, key: str) -> None:
        if self.internacao_view is None:
            return
        labels = self.internacao_view.get_file_labels()
        if key == "output_dir":
            path = filedialog.askdirectory(title="Selecionar pasta de saída")
        else:
            path = filedialog.askopenfilename(
                title=f"Selecionar arquivo - {labels.get(key, key)}"
            )
        if path:
            self.internacao_view.set_file_value(key, path)
            self._reset_response_warning_flags()
            self.internacao_view.clear_status_message()

    def _clear_file_internacao(self, key: str) -> None:
        if self.internacao_view is None:
            return
        self.internacao_view.clear_file_value(key)
        self._reset_response_warning_flags()

    def _start_etl_execution(self) -> None:
        if self.ambos_view is None:
            return

        file_values = self.ambos_view.get_file_values()
        if not file_values.get("output_dir", "").strip():
            self.ambos_view.set_status_message(
                "Selecione a Pasta de saída antes de executar o ETL.",
                "#FFB1B1",
            )
            return
        file_labels = self.ambos_view.get_file_labels()
        plano_execucao, erro_validacao = self.ambos_controller.resolve_execution_plan(
            file_values=file_values,
            file_labels=file_labels,
        )

        if erro_validacao:
            self.ambos_view.set_status_message(erro_validacao, "#FFB1B1")
            return

        if self.ambos_controller.needs_missing_responses_confirmation(plano_execucao):
            title, message = self.ambos_controller.missing_responses_warning()
            self._show_warning_popup(
                title=title,
                message=message,
                on_ack=self._ack_ambos_warning,
            )
            return

        self.ui_state.current_execution_plan = plano_execucao
        self._etl_steps = self.pipeline_runner.build_etl_steps(plano_execucao)
        self._current_execution_context = "ambos"

        self.ambos_view.set_status_message(
            (
                "Plano selecionado: "
                f"Complicação {plano_execucao['complicacao']} | "
                f"Internação {plano_execucao['internacao']}"
            ),
            "#A7C8FF",
        )

        self._open_progress_modal()

    def _start_complicacao_execution(self) -> None:
        if self.complicacao_view is None:
            return

        file_values = self.complicacao_view.get_file_values()
        if not file_values.get("output_dir", "").strip():
            self.complicacao_view.set_status_message(
                "Selecione a Pasta de saída antes de executar o ETL.",
                "#FFB1B1",
            )
            return
        file_labels = self.complicacao_view.get_file_labels()
        plano_execucao, erro_validacao = self.complicacao_controller.resolve_execution_plan(
            file_values=file_values,
            file_labels=file_labels,
        )

        if erro_validacao:
            self.complicacao_view.set_status_message(erro_validacao, "#FFB1B1")
            return

        if self.complicacao_controller.needs_missing_response_confirmation(plano_execucao):
            title, message = self.complicacao_controller.missing_response_warning()
            self._show_warning_popup(
                title=title,
                message=message,
                on_ack=self._ack_complicacao_warning,
            )
            return

        self.ui_state.current_execution_plan = plano_execucao
        self._etl_steps = self.pipeline_runner.build_complicacao_steps(plano_execucao)
        self._current_execution_context = "complicacao"

        self.complicacao_view.set_status_message(
            f"Plano selecionado: Complicação {plano_execucao['complicacao']}",
            "#A7C8FF",
        )
        self._open_progress_modal()

    def _start_internacao_execution(self) -> None:
        if self.internacao_view is None:
            return

        file_values = self.internacao_view.get_file_values()
        if not file_values.get("output_dir", "").strip():
            self.internacao_view.set_status_message(
                "Selecione a Pasta de saída antes de executar o ETL.",
                "#FFB1B1",
            )
            return
        file_labels = self.internacao_view.get_file_labels()
        plano_execucao, erro_validacao = self.internacao_controller.resolve_execution_plan(
            file_values=file_values,
            file_labels=file_labels,
        )

        if erro_validacao:
            self.internacao_view.set_status_message(erro_validacao, "#FFB1B1")
            return

        if self.internacao_controller.needs_missing_response_confirmation(plano_execucao):
            title, message = self.internacao_controller.missing_response_warning()
            self._show_warning_popup(
                title=title,
                message=message,
                on_ack=self._ack_internacao_warning,
            )
            return

        self.ui_state.current_execution_plan = plano_execucao
        self._etl_steps = self.pipeline_runner.build_internacao_steps(plano_execucao)
        self._current_execution_context = "internacao"

        self.internacao_view.set_status_message(
            f"Plano selecionado: Internação {plano_execucao['internacao']}",
            "#A7C8FF",
        )
        self._open_progress_modal()

    def _reset_response_warning_flags(self) -> None:
        self.ambos_controller.reset_response_warning_flags()
        self.complicacao_controller.reset_response_warning_flags()
        self.internacao_controller.reset_response_warning_flags()

    def _ack_ambos_warning(self) -> None:
        self.ambos_controller.ack_missing_responses_confirmation()

    def _ack_complicacao_warning(self) -> None:
        self.complicacao_controller.ack_missing_response_confirmation()

    def _ack_internacao_warning(self) -> None:
        self.internacao_controller.ack_missing_response_confirmation()

    def _show_warning_popup(
        self, title: str, message: str, on_ack: Callable[[], None]
    ) -> None:
        if self._warning_modal is not None and self._warning_modal.winfo_exists():
            self._warning_modal.destroy()

        modal = ctk.CTkToplevel(self)
        self._warning_modal = modal
        modal.title(title)
        modal.geometry("760x360")
        modal.resizable(False, False)
        modal.transient(self)
        modal.grab_set()
        modal.configure(fg_color="#101A35")
        self._center_window(modal, 760, 360)

        card = ctk.CTkFrame(
            modal,
            fg_color="#1A2A52",
            corner_radius=22,
            border_color="#223D73",
            border_width=1,
        )
        card.pack(fill="both", expand=True, padx=22, pady=20)

        titulo = ctk.CTkLabel(
            card,
            text=title,
            font=ctk.CTkFont(family="Segoe UI", size=30, weight="bold"),
            text_color="#E6F0FF",
            fg_color="transparent",
        )
        titulo.pack(pady=(24, 8))

        texto = ctk.CTkLabel(
            card,
            text=message,
            font=ctk.CTkFont(family="Segoe UI", size=20),
            text_color="#D8E8FF",
            fg_color="transparent",
            justify="center",
        )
        texto.pack(padx=36, pady=(8, 26))

        ok_btn = ctk.CTkButton(
            card,
            text="OK",
            width=220,
            height=52,
            font=ctk.CTkFont(family="Segoe UI", size=24, weight="bold"),
            fg_color=self.style.btn_fg_color,
            hover_color=self.style.btn_hover_color,
            border_color=self.style.btn_border_color,
            text_color=self.style.btn_text_color,
            corner_radius=self.style.btn_corner_radius,
            border_width=self.style.btn_border_width,
            command=lambda: self._close_warning_popup(on_ack),
        )
        ok_btn.pack(pady=(0, 24))

        modal.protocol("WM_DELETE_WINDOW", lambda: self._close_warning_popup(on_ack))

    def _close_warning_popup(self, on_ack: Callable[[], None]) -> None:
        on_ack()
        if self._warning_modal is not None and self._warning_modal.winfo_exists():
            self._warning_modal.grab_release()
            self._warning_modal.destroy()
        self._warning_modal = None

    def _open_progress_modal(self) -> None:
        self._etl_cancelled = False
        self._etl_after_id = None

        self.progress_modal = ProgressModal(
            parent=self,
            style=self.style,
            center_window=self._center_window,
            on_cancel=self._cancel_etl_execution,
        )
        self.progress_modal.open()
        self._advance_progress_step(0)

    def _advance_progress_step(self, step_index: int) -> None:
        if self.progress_modal is None or not self.progress_modal.exists():
            return

        if self._etl_cancelled:
            self._set_progress_ui("Execução cancelada pelo usuário.", step_index)
            return

        total_steps = len(self._etl_steps)
        if step_index >= total_steps:
            self._set_progress_ui("Processo concluído com sucesso.", total_steps)
            self.progress_modal.set_cancel_as_close(self._close_progress_modal)
            self._set_execution_status_message("Execução ETL finalizada com sucesso.", "#AEE3B8")
            return

        self._set_progress_ui(self._etl_steps[step_index], step_index + 1)
        self._etl_after_id = self.after(850, lambda: self._advance_progress_step(step_index + 1))

    def _set_progress_ui(self, status_text: str, progress_step: int) -> None:
        total_steps = max(len(self._etl_steps), 1)
        pct = min(progress_step / total_steps, 1.0)
        if self.progress_modal is not None:
            self.progress_modal.set_progress(pct, f"{int(pct * 100)}%")
            self.progress_modal.set_status(status_text)

    def _cancel_etl_execution(self) -> None:
        self._etl_cancelled = True
        if self._etl_after_id is not None:
            self.after_cancel(self._etl_after_id)
            self._etl_after_id = None

        self._set_execution_status_message("Execução ETL cancelada.", "#FFD39A")

        self._close_progress_modal()

    def _close_progress_modal(self) -> None:
        if self.progress_modal is not None:
            self.progress_modal.close()
        self.progress_modal = None

    def _set_execution_status_message(self, text: str, color: str) -> None:
        if self._current_execution_context == "complicacao":
            if self.complicacao_view is not None:
                self.complicacao_view.set_status_message(text, color)
            return
        if self._current_execution_context == "internacao":
            if self.internacao_view is not None:
                self.internacao_view.set_status_message(text, color)
            return
        if self.ambos_view is not None:
            self.ambos_view.set_status_message(text, color)

    def _center_window(self, window: ctk.CTkToplevel, width: int, height: int) -> None:
        window.update_idletasks()
        x = (window.winfo_screenwidth() // 2) - (width // 2)
        y = (window.winfo_screenheight() // 2) - (height // 2)
        window.geometry(f"{width}x{height}+{x}+{y}")

    def _load_icon(self, name: str, width: int, height: int) -> ctk.CTkImage:
        icon_path = Path("assets/icons") / name
        image = Image.open(icon_path).convert("RGBA")
        tinted = self._tint_icon(image)
        icon = ctk.CTkImage(
            light_image=tinted,
            dark_image=tinted,
            size=(width, height),
        )
        self._icon_refs.append(icon)
        return icon

    def _tint_icon(self, image: Image.Image) -> Image.Image:
        alpha = image.split()[-1]
        tinted = Image.new("RGBA", image.size, self.style.icon_color)
        tinted.putalpha(alpha)
        return tinted

    def _create_frame_fluxo_partes(self) -> None:
        self.fluxo_partes_view = FluxoPartesView(
            parent=self.container,
            style=self.style,
            specs=self._get_fluxo_partes_specs(),
            on_back=lambda: self.show_frame("menu_frame"),
            on_select_file=self._select_file_fluxo_partes,
            on_clear_file=self._clear_file_fluxo_partes,
            on_execute=self._start_fluxo_partes_execution,
        )
        self.frames["frame_fluxo_partes"] = self.fluxo_partes_view

    def _create_frame_juntar_status(self) -> None:
        self.uniao_status_view = UniaoStatusView(
            parent=self.container,
            style=self.style,
            on_back=lambda: self.show_frame("menu_frame"),
            on_select_file=self._select_file_uniao_status,
            on_clear_file=self._clear_file_uniao_status,
            on_execute=self._start_uniao_status_execution,
        )
        self.frames["frame_juntar_status"] = self.uniao_status_view

    def _create_frame_concatenar(self) -> None:
        self.concatenar_view = ConcatenarView(
            parent=self.container,
            style=self.style,
            on_back=lambda: self.show_frame("menu_frame"),
            on_select_file=self._select_file_concatenar,
            on_clear_file=self._clear_file_concatenar,
            on_execute=self._start_concatenar_execution,
        )
        self.frames["frame_concatenar"] = self.concatenar_view

    def _create_frame_limpeza_dados(self) -> None:
        self.ingestao_view = IngestaoView(
            parent=self.container,
            style=self.style,
            on_back=lambda: self.show_frame("menu_frame"),
            on_select_file=self._select_file_ingestao,
            on_clear_file=self._clear_file_ingestao,
            on_execute=self._start_ingestao_execution,
        )
        self.frames["frame_limpeza_dados"] = self.ingestao_view

    def _create_frame_utilitario(self) -> None:
        frame = ctk.CTkFrame(self.container, fg_color="transparent")
        self.frames["frame_utilitario"] = frame
        self._build_base_content(frame, "frame_utilitario")

    def _create_frame_configuracoes(self) -> None:
        frame = ctk.CTkFrame(self.container, fg_color="transparent")
        self.frames["frame_configuracoes"] = frame
        self._build_base_content(frame, "frame_configuracoes")
