from __future__ import annotations

from pathlib import Path
import sys
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
from src.pipelines.complicacao_orquestracao_pipeline import run_complicacao_pipeline_orquestrar
from src.pipelines.complicacao_status_pipeline import (
    run_complicacao_pipeline_gerar_status_dataset,
    run_complicacao_pipeline_gerar_status_dataset_somente_status,
)
from src.pipelines.concatenar_livre_pipeline import run_unificar_arquivos_livre_pipeline
from src.pipelines.concatenar_status_pipeline import run_unificar_status_pipeline
from src.pipelines.concatenar_status_respostas_pipeline import (
    run_unificar_status_respostas_pipeline,
)
from src.pipelines.internacao_eletivo_orquestracao_pipeline import (
    run_internacao_eletivo_pipeline_orquestrar,
)
from src.pipelines.internacao_eletivo_status_pipeline import (
    run_internacao_eletivo_pipeline_gerar_status_dataset,
    run_internacao_eletivo_pipeline_gerar_status_dataset_somente_status,
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
        self._progress_current_step: int = 0
        self._current_concatenar_mode: str = "status"

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
        file_values = self.concatenar_view.get_file_values(mode)
        if key == "arquivo_saida":
            selected_dir = filedialog.askdirectory(
                title=f"Selecionar pasta - {labels.get(key, key)}"
            )
            if selected_dir:
                default_name = self._default_output_filename_concatenar(
                    mode=mode,
                    file_values=file_values,
                )
                path = str(Path(selected_dir) / default_name)
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
    def _default_output_filename_concatenar(
        mode: str,
        file_values: dict[str, str] | None = None,
    ) -> str:
        ext = ".csv"
        if file_values:
            keys_by_mode = {
                "status": ["status_complicacao", "status_internacao_eletivo"],
                "status_resposta": ["resposta_eletivo", "resposta_internacao"],
                "livre": ["arquivo_a", "arquivo_b"],
            }
            for key in keys_by_mode.get(mode, []):
                v = (file_values.get(key) or "").strip().lower()
                if v.endswith(".xlsx") or v.endswith(".xls"):
                    ext = ".xlsx"
                    break

        if mode == "status":
            return f"status_complicacao_internacao_eletivo{ext}"
        if mode == "status_resposta":
            return f"status_resposta_eletivo_internacao{ext}"
        return f"concatenacao_livre{ext}"

    @staticmethod
    def _default_output_filename_uniao_status(
        mode: str,
        file_values: dict[str, str] | None = None,
    ) -> str:
        ext = ".csv"
        if file_values:
            for key in ("arquivo_status", "arquivo_flow_resposta"):
                v = (file_values.get(key) or "").strip().lower()
                if v.endswith(".xlsx") or v.endswith(".xls"):
                    ext = ".xlsx"
                    break

        if mode == "internacao":
            return f"status_internacao_eletivo{ext}"
        return f"status_complicacao{ext}"

    @staticmethod
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
        self._current_execution_context = "concatenar"
        self._current_concatenar_mode = mode
        self.concatenar_view.set_status_message(mode, "Executando concatenação...", "#A7C8FF")
        self._etl_steps = [
            "Preparando concatenação...",
            "Executando concatenação...",
            "Finalizando concatenação...",
        ]
        self._open_progress_modal_manual()
        threading.Thread(
            target=self._run_concatenar_worker,
            args=(mode, file_values),
            daemon=True,
        ).start()

    def _run_concatenar_worker(self, mode: str, file_values: dict[str, str]) -> None:
        try:
            self._publish_real_progress(1, "Executando concatenação...")
            if mode == "status":
                arquivo_saida = file_values["arquivo_saida"]
                resultado = run_unificar_status_pipeline(
                    arquivo_status_complicacao=file_values["status_complicacao"],
                    arquivo_status_internacao_eletivo=file_values["status_internacao_eletivo"],
                    arquivo_saida=arquivo_saida,
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
            self.after(
                0,
                lambda: self._finalize_real_progress(
                    False,
                    f"Falha na concatenação: {type(erro).__name__}: {erro}",
                ),
            )
            return

        if resultado.get("ok", False):
            mensagem = self._result_message(
                resultado,
                ok_default="Concatenação concluída com sucesso.",
            )
            self.after(0, lambda: self._finalize_real_progress(True, mensagem))
            return

        mensagem = self._result_message(
            resultado,
            fail_default="Falha na concatenação.",
        )
        self.after(0, lambda: self._finalize_real_progress(False, mensagem))

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
        detalhe = self._result_message(
            result,
            ok_default="Concluído com sucesso.",
            fail_default="Falha na execução.",
        )
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
        if key == "pasta_saida":
            path = filedialog.askdirectory(title=f"Selecionar pasta - {labels.get(key, key)}")
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
        detalhe = self._result_message(
            result,
            ok_default="Concluído com sucesso.",
            fail_default="Falha na execução.",
        )
        self.after(0, lambda: self._set_ingestao_status(mode, detalhe, "#AEE3B8" if ok else "#FFB1B1"))

    def _set_ingestao_status(self, mode: str, text: str, color: str) -> None:
        if self.ingestao_view is not None:
            self.ingestao_view.set_status_message(mode, text, color)

    def _select_file_uniao_status(self, mode: str, key: str) -> None:
        if self.uniao_status_view is None:
            return
        labels = self.uniao_status_view.get_file_labels(mode)
        file_values = self.uniao_status_view.get_file_values(mode)
        if key == "arquivo_saida":
            selected_dir = filedialog.askdirectory(
                title=f"Selecionar pasta - {labels.get(key, key)}"
            )
            if selected_dir:
                filename = self._default_output_filename_uniao_status(
                    mode=mode,
                    file_values=file_values,
                )
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
            mensagem = self._result_message(
                resultado,
                ok_default="União concluída com sucesso.",
            )
            self.uniao_status_view.set_status_message(mode, mensagem, "#AEE3B8")
            return

        mensagem = self._result_message(
            resultado,
            fail_default="Falha na união.",
        )
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
        self._current_execution_context = "ambos"

        self.ambos_view.set_status_message(
            (
                "Plano selecionado: "
                f"Complicação {plano_execucao['complicacao']} | "
                f"Internação {plano_execucao['internacao']}"
            ),
            "#A7C8FF",
        )
        self.ambos_view.set_status_message("Executando modo Ambos...", "#A7C8FF")
        self._etl_steps = self._build_real_ambos_steps()
        self._open_progress_modal_manual()
        threading.Thread(
            target=self._run_ambos_worker,
            args=(file_values, plano_execucao),
            daemon=True,
        ).start()

    @staticmethod
    def _normalized_messages(result: dict) -> list[str]:
        if not isinstance(result, dict):
            return []
        msgs = result.get("mensagens", [])
        if isinstance(msgs, str):
            msgs = [msgs]
        if not isinstance(msgs, list):
            msgs = []
        generic_markers = (
            "modo complicacao selecionado",
            "modo internação selecionado",
            "modo internacao selecionado",
            "modo internacao_eletivo selecionado",
            "modo ambos selecionado",
            "modo complicacao iniciado",
            "modo internacao iniciado",
            "modo internacao_eletivo iniciado",
            "modo ambos iniciado",
        )
        filtered: list[str] = []
        for msg in msgs:
            s = str(msg).strip()
            if not s:
                continue
            low = s.lower()
            if any(marker in low for marker in generic_markers):
                continue
            filtered.append(s)

        nested = result.get("resultados", {})
        if isinstance(nested, dict):
            for sub in nested.values():
                filtered.extend(App._normalized_messages(sub))

        # Remove duplicadas preservando ordem.
        unique: list[str] = []
        for msg in filtered:
            if msg not in unique:
                unique.append(msg)
        return unique

    @staticmethod
    def _first_message(result: dict) -> str:
        lines = App._normalized_messages(result)
        return lines[0] if lines else "Falha na execução."

    @staticmethod
    def _result_message(
        result: dict,
        *,
        ok_default: str = "Concluído com sucesso.",
        fail_default: str = "Falha na execução.",
        max_error_lines: int = 6,
    ) -> str:
        if not isinstance(result, dict):
            return fail_default
        lines = App._normalized_messages(result)
        ok = bool(result.get("ok", False))
        if not lines:
            return ok_default if ok else fail_default
        if ok:
            return lines[0]
        if len(lines) > max_error_lines:
            lines = lines[:max_error_lines] + ["..."]
        return "\n".join(lines)

    def _run_ambos_worker(
        self,
        file_values: dict[str, str],
        plano_execucao: dict[str, str],
    ) -> None:
        try:
            if self._etl_cancelled:
                return
            output_dir = Path(file_values["output_dir"])
            output_dir.mkdir(parents=True, exist_ok=True)

            comp_saida_status = str(output_dir / "status_complicacao_limpo.csv")
            comp_saida_resposta = str(output_dir / "status_resposta_complicacao_limpo.csv")
            comp_saida_integrado = str(output_dir / "status_complicacao.csv")
            comp_saida_dataset_status = str(output_dir / "complicacao_status.xlsx")
            comp_saida_final = str(output_dir / "complicacao_final.xlsx")

            int_saida_status = str(output_dir / "status_internacao_eletivo_limpo.csv")
            int_saida_resposta_unificado = str(output_dir / "status_resposta_eletivo_internacao.csv")
            int_saida_resposta = str(output_dir / "status_resposta_eletivo_internacao_limpo.csv")
            int_saida_integrado = str(output_dir / "status_internacao_eletivo.csv")
            int_saida_dataset_status = str(output_dir / "internacao_status.xlsx")
            int_saida_final = str(output_dir / "internacao_final.xlsx")

            self._publish_real_progress(1, "Complicação: gerando dataset status...")
            if plano_execucao["complicacao"] == "com_resposta":
                resultado_comp = run_complicacao_pipeline_gerar_status_dataset(
                    arquivo_status=file_values["status"],
                    arquivo_status_resposta_complicacao=file_values["flow_complicacao"],
                    arquivo_dataset_origem_complicacao=file_values["complicacao"],
                    saida_status=comp_saida_status,
                    saida_status_resposta=comp_saida_resposta,
                    saida_status_integrado=comp_saida_integrado,
                    saida_dataset_status=comp_saida_dataset_status,
                )
            else:
                resultado_comp = run_complicacao_pipeline_gerar_status_dataset_somente_status(
                    arquivo_status=file_values["status"],
                    arquivo_dataset_origem_complicacao=file_values["complicacao"],
                    saida_status=comp_saida_status,
                    saida_status_integrado=comp_saida_integrado,
                    saida_dataset_status=comp_saida_dataset_status,
                )
            if not resultado_comp.get("ok", False):
                detalhe = self._result_message(
                    resultado_comp,
                    fail_default="Falha na etapa de criação de dataset da complicação.",
                )
                self.after(0, lambda: self._finalize_real_progress(False, f"Complicação falhou: {detalhe}"))
                return

            if self._etl_cancelled:
                return
            self._publish_real_progress(2, "Complicação: orquestrando dataset...")
            resultado_comp_orq = run_complicacao_pipeline_orquestrar(
                arquivo_dataset_status=comp_saida_dataset_status,
                arquivo_saida_final=comp_saida_final,
                nome_logger=(
                    "orquestracao_complicacao_ui"
                    if plano_execucao["complicacao"] == "com_resposta"
                    else "orquestracao_complicacao_somente_status_ui"
                ),
            )
            if not resultado_comp_orq.get("ok", False):
                detalhe = self._result_message(
                    resultado_comp_orq,
                    fail_default="Falha na etapa de orquestração da complicação.",
                )
                self.after(0, lambda: self._finalize_real_progress(False, f"Complicação falhou: {detalhe}"))
                return

            if self._etl_cancelled:
                return
            self._publish_real_progress(3, "Internação: gerando dataset status...")
            if plano_execucao["internacao"] == "com_resposta":
                resultado_int = run_internacao_eletivo_pipeline_gerar_status_dataset(
                    arquivo_status=file_values["status"],
                    arquivo_status_resposta_eletivo=file_values["flow_internacao_eletivo"],
                    arquivo_status_resposta_internacao=file_values["flow_internacao_urgencia"],
                    arquivo_status_resposta_unificado=int_saida_resposta_unificado,
                    arquivo_dataset_origem_internacao=file_values["internacao"],
                    saida_status=int_saida_status,
                    saida_status_resposta=int_saida_resposta,
                    saida_status_integrado=int_saida_integrado,
                    saida_dataset_status=int_saida_dataset_status,
                )
            else:
                resultado_int = run_internacao_eletivo_pipeline_gerar_status_dataset_somente_status(
                    arquivo_status=file_values["status"],
                    arquivo_dataset_origem_internacao=file_values["internacao"],
                    saida_status=int_saida_status,
                    saida_status_integrado=int_saida_integrado,
                    saida_dataset_status=int_saida_dataset_status,
                )
            if not resultado_int.get("ok", False):
                detalhe = self._result_message(
                    resultado_int,
                    fail_default="Falha na etapa de criação de dataset da internação.",
                )
                self.after(0, lambda: self._finalize_real_progress(False, f"Internação falhou: {detalhe}"))
                return

            if self._etl_cancelled:
                return
            self._publish_real_progress(4, "Internação: orquestrando dataset...")
            resultado_int_orq = run_internacao_eletivo_pipeline_orquestrar(
                arquivo_dataset_status=int_saida_dataset_status,
                arquivo_saida_final=int_saida_final,
                nome_logger=(
                    "orquestracao_internacao_ui"
                    if plano_execucao["internacao"] == "com_resposta"
                    else "orquestracao_internacao_somente_status_ui"
                ),
            )
            if not resultado_int_orq.get("ok", False):
                detalhe = self._result_message(
                    resultado_int_orq,
                    fail_default="Falha na etapa de orquestração da internação.",
                )
                self.after(0, lambda: self._finalize_real_progress(False, f"Internação falhou: {detalhe}"))
                return
        except Exception as erro:
            self.after(
                0,
                lambda: self._finalize_real_progress(
                    False,
                    f"Falha no modo Ambos: {type(erro).__name__}: {erro}",
                ),
            )
            return

        self.after(0, lambda: self._finalize_real_progress(True, "Modo Ambos executado com sucesso."))

    @staticmethod
    def _build_real_ambos_steps() -> list[str]:
        return [
            "Preparando execução...",
            "Complicação: gerando dataset status...",
            "Complicação: orquestrando dataset...",
            "Internação: gerando dataset status...",
            "Internação: orquestrando dataset...",
            "Finalizando execução...",
        ]

    @staticmethod
    def _build_real_complicacao_steps() -> list[str]:
        return [
            "Preparando execução...",
            "Complicação: gerando dataset status...",
            "Complicação: orquestrando dataset...",
            "Finalizando execução...",
        ]

    @staticmethod
    def _build_real_internacao_steps() -> list[str]:
        return [
            "Preparando execução...",
            "Internação: gerando dataset status...",
            "Internação: orquestrando dataset...",
            "Finalizando execução...",
        ]

    def _open_progress_modal_manual(self) -> None:
        self._etl_cancelled = False
        self._etl_after_id = None
        self.progress_modal = ProgressModal(
            parent=self,
            style=self.style,
            center_window=self._center_window,
            on_cancel=self._cancel_etl_execution,
        )
        self.progress_modal.open()
        self._set_progress_ui("Preparando execução...", 0)

    def _publish_real_progress(self, step_index: int, text: str) -> None:
        self.after(0, lambda: self._set_progress_ui(text, step_index))

    def _finalize_real_progress(self, ok: bool, mensagem: str) -> None:
        if ok:
            self._set_progress_ui(mensagem, len(self._etl_steps))
        else:
            step = max(1, min(self._progress_current_step, max(len(self._etl_steps) - 1, 1)))
            self._set_progress_ui(f"Falha na execução: {mensagem}", step)
            if self.progress_modal is not None and self.progress_modal.exists():
                self.progress_modal.set_error_state(f"Falha na execução:\n{mensagem}")
        cor = "#AEE3B8" if ok else "#FFB1B1"
        self._set_execution_status_message(mensagem, cor)
        if self.progress_modal is not None and self.progress_modal.exists():
            self.progress_modal.set_cancel_as_close(self._close_progress_modal)

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
        self._current_execution_context = "complicacao"

        self.complicacao_view.set_status_message(
            f"Plano selecionado: Complicação {plano_execucao['complicacao']}",
            "#A7C8FF",
        )
        self.complicacao_view.set_status_message("Executando modo Complicação...", "#A7C8FF")
        self._etl_steps = self._build_real_complicacao_steps()
        self._open_progress_modal_manual()
        threading.Thread(
            target=self._run_complicacao_worker,
            args=(file_values, plano_execucao),
            daemon=True,
        ).start()

    def _run_complicacao_worker(
        self,
        file_values: dict[str, str],
        plano_execucao: dict[str, str],
    ) -> None:
        try:
            if self._etl_cancelled:
                return
            output_dir = Path(file_values["output_dir"])
            output_dir.mkdir(parents=True, exist_ok=True)

            comp_saida_status = str(output_dir / "status_complicacao_limpo.csv")
            comp_saida_resposta = str(output_dir / "status_resposta_complicacao_limpo.csv")
            comp_saida_integrado = str(output_dir / "status_complicacao.csv")
            comp_saida_dataset_status = str(output_dir / "complicacao_status.xlsx")
            comp_saida_final = str(output_dir / "complicacao_final.xlsx")

            self._publish_real_progress(1, "Complicação: gerando dataset status...")
            if plano_execucao["complicacao"] == "com_resposta":
                resultado = run_complicacao_pipeline_gerar_status_dataset(
                    arquivo_status=file_values["status"],
                    arquivo_status_resposta_complicacao=file_values["flow_complicacao"],
                    arquivo_dataset_origem_complicacao=file_values["complicacao_dataset"],
                    saida_status=comp_saida_status,
                    saida_status_resposta=comp_saida_resposta,
                    saida_status_integrado=comp_saida_integrado,
                    saida_dataset_status=comp_saida_dataset_status,
                )
            else:
                resultado = run_complicacao_pipeline_gerar_status_dataset_somente_status(
                    arquivo_status=file_values["status"],
                    arquivo_dataset_origem_complicacao=file_values["complicacao_dataset"],
                    saida_status=comp_saida_status,
                    saida_status_integrado=comp_saida_integrado,
                    saida_dataset_status=comp_saida_dataset_status,
                )
            if not resultado.get("ok", False):
                detalhe = self._result_message(
                    resultado,
                    fail_default="Falha na etapa de criação de dataset da complicação.",
                )
                self.after(0, lambda: self._finalize_real_progress(False, f"Complicação falhou: {detalhe}"))
                return

            if self._etl_cancelled:
                return
            self._publish_real_progress(2, "Complicação: orquestrando dataset...")
            resultado_orq = run_complicacao_pipeline_orquestrar(
                arquivo_dataset_status=comp_saida_dataset_status,
                arquivo_saida_final=comp_saida_final,
                nome_logger=(
                    "orquestracao_complicacao_ui"
                    if plano_execucao["complicacao"] == "com_resposta"
                    else "orquestracao_complicacao_somente_status_ui"
                ),
            )
            if not resultado_orq.get("ok", False):
                detalhe = self._result_message(
                    resultado_orq,
                    fail_default="Falha na etapa de orquestração da complicação.",
                )
                self.after(0, lambda: self._finalize_real_progress(False, f"Complicação falhou: {detalhe}"))
                return
        except Exception as erro:
            self.after(
                0,
                lambda: self._finalize_real_progress(
                    False,
                    f"Falha no modo Complicação: {type(erro).__name__}: {erro}",
                ),
            )
            return

        self.after(0, lambda: self._finalize_real_progress(True, "Modo Complicação executado com sucesso."))

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
        self._current_execution_context = "internacao"

        self.internacao_view.set_status_message(
            f"Plano selecionado: Internação {plano_execucao['internacao']}",
            "#A7C8FF",
        )
        self.internacao_view.set_status_message("Executando modo Internação...", "#A7C8FF")
        self._etl_steps = self._build_real_internacao_steps()
        self._open_progress_modal_manual()
        threading.Thread(
            target=self._run_internacao_worker,
            args=(file_values, plano_execucao),
            daemon=True,
        ).start()

    def _run_internacao_worker(
        self,
        file_values: dict[str, str],
        plano_execucao: dict[str, str],
    ) -> None:
        try:
            if self._etl_cancelled:
                return
            output_dir = Path(file_values["output_dir"])
            output_dir.mkdir(parents=True, exist_ok=True)

            int_saida_status = str(output_dir / "status_internacao_eletivo_limpo.csv")
            int_saida_resposta_unificado = str(output_dir / "status_resposta_eletivo_internacao.csv")
            int_saida_resposta = str(output_dir / "status_resposta_eletivo_internacao_limpo.csv")
            int_saida_integrado = str(output_dir / "status_internacao_eletivo.csv")
            int_saida_dataset_status = str(output_dir / "internacao_status.xlsx")
            int_saida_final = str(output_dir / "internacao_final.xlsx")

            self._publish_real_progress(1, "Internação: gerando dataset status...")
            if plano_execucao["internacao"] == "com_resposta":
                resultado = run_internacao_eletivo_pipeline_gerar_status_dataset(
                    arquivo_status=file_values["status"],
                    arquivo_status_resposta_eletivo=file_values["flow_internacao_eletivo"],
                    arquivo_status_resposta_internacao=file_values["flow_internacao_urgencia"],
                    arquivo_status_resposta_unificado=int_saida_resposta_unificado,
                    arquivo_dataset_origem_internacao=file_values["internacao_dataset"],
                    saida_status=int_saida_status,
                    saida_status_resposta=int_saida_resposta,
                    saida_status_integrado=int_saida_integrado,
                    saida_dataset_status=int_saida_dataset_status,
                )
            else:
                resultado = run_internacao_eletivo_pipeline_gerar_status_dataset_somente_status(
                    arquivo_status=file_values["status"],
                    arquivo_dataset_origem_internacao=file_values["internacao_dataset"],
                    saida_status=int_saida_status,
                    saida_status_integrado=int_saida_integrado,
                    saida_dataset_status=int_saida_dataset_status,
                )
            if not resultado.get("ok", False):
                detalhe = self._result_message(
                    resultado,
                    fail_default="Falha na etapa de criação de dataset da internação.",
                )
                self.after(0, lambda: self._finalize_real_progress(False, f"Internação falhou: {detalhe}"))
                return

            if self._etl_cancelled:
                return
            self._publish_real_progress(2, "Internação: orquestrando dataset...")
            resultado_orq = run_internacao_eletivo_pipeline_orquestrar(
                arquivo_dataset_status=int_saida_dataset_status,
                arquivo_saida_final=int_saida_final,
                nome_logger=(
                    "orquestracao_internacao_ui"
                    if plano_execucao["internacao"] == "com_resposta"
                    else "orquestracao_internacao_somente_status_ui"
                ),
            )
            if not resultado_orq.get("ok", False):
                detalhe = self._result_message(
                    resultado_orq,
                    fail_default="Falha na etapa de orquestração da internação.",
                )
                self.after(0, lambda: self._finalize_real_progress(False, f"Internação falhou: {detalhe}"))
                return
        except Exception as erro:
            self.after(
                0,
                lambda: self._finalize_real_progress(
                    False,
                    f"Falha no modo Internação: {type(erro).__name__}: {erro}",
                ),
            )
            return

        self.after(0, lambda: self._finalize_real_progress(True, "Modo Internação executado com sucesso."))

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
        self._progress_current_step = max(0, progress_step)
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
        if self._current_execution_context == "concatenar":
            if self.concatenar_view is not None:
                self.concatenar_view.set_status_message(self._current_concatenar_mode, text, color)
            return
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
        icon_path = self._resolver_caminho_asset("icons", name)
        image = Image.open(icon_path).convert("RGBA")
        tinted = self._tint_icon(image)
        icon = ctk.CTkImage(
            light_image=tinted,
            dark_image=tinted,
            size=(width, height),
        )
        self._icon_refs.append(icon)
        return icon

    def _resolver_caminho_asset(self, *partes: str) -> Path:
        candidatos = []
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            candidatos.append(Path(meipass) / "assets")
        candidatos.append(Path(__file__).resolve().parents[2] / "assets")
        candidatos.append(Path.cwd() / "assets")

        for base in candidatos:
            caminho = base.joinpath(*partes)
            if caminho.exists():
                return caminho

        raise FileNotFoundError(
            f"Asset nao encontrado: {'/'.join(partes)}. Candidatos: "
            + ", ".join(str(base.joinpath(*partes)) for base in candidatos)
        )

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
