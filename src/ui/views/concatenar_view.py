from __future__ import annotations

from typing import Callable

import customtkinter as ctk

from src.ui.components.file_selector_row import FileSelectorRow
from src.ui.state import UIStyle


class ConcatenarView(ctk.CTkFrame):
    MODES = (
        ("status", "Concatenar Status"),
        ("status_resposta", "Concatenar Status Resposta"),
        ("livre", "Concatenação Livre"),
    )

    def __init__(
        self,
        parent: ctk.CTkFrame,
        style: UIStyle,
        on_back: Callable[[], None],
        on_select_file: Callable[[str, str], None],
        on_clear_file: Callable[[str, str], None],
        on_execute: Callable[[str], None],
    ) -> None:
        super().__init__(parent, fg_color="transparent")
        self._style = style
        self._on_back = on_back
        self._on_select_file = on_select_file
        self._on_clear_file = on_clear_file
        self._on_execute = on_execute

        self._active_mode = "status"
        self.mode_buttons: dict[str, ctk.CTkButton] = {}
        self.mode_frames: dict[str, ctk.CTkFrame] = {}
        self.rows: dict[str, dict[str, FileSelectorRow]] = {}
        self.exec_status_labels: dict[str, ctk.CTkLabel] = {}

        self._build()

    def _build(self) -> None:
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        titulo = ctk.CTkLabel(
            self,
            text="Concatenar Arquivos",
            font=ctk.CTkFont(size=42, weight="bold"),
            text_color="#E6F0FF",
        )
        titulo.place(relx=0.5, rely=0.085, anchor="center")

        voltar_shadow = ctk.CTkFrame(
            self,
            width=62,
            height=52,
            corner_radius=self._style.btn_corner_radius,
            fg_color=self._style.btn_shadow_color,
        )
        voltar_shadow.place(x=44, y=52, anchor="nw")

        voltar_btn = ctk.CTkButton(
            self,
            text="←",
            width=60,
            height=50,
            font=ctk.CTkFont(family="Segoe UI", size=26, weight="bold"),
            fg_color=self._style.btn_fg_color,
            hover_color=self._style.btn_hover_color,
            border_color=self._style.btn_border_color,
            text_color=self._style.btn_text_color,
            corner_radius=self._style.btn_corner_radius,
            border_width=self._style.btn_border_width,
            command=self._on_back,
        )
        voltar_btn.place(x=42, y=50, anchor="nw")

        card_shadow = ctk.CTkFrame(
            self,
            width=1120,
            height=500,
            corner_radius=20,
            fg_color=self._style.btn_shadow_color,
        )
        card_shadow.place(relx=0.5, rely=0.555, anchor="center")

        card = ctk.CTkFrame(
            self,
            width=1120,
            height=496,
            corner_radius=20,
            fg_color="#17264A",
            border_color="#21386B",
            border_width=1,
        )
        card.place(relx=0.5, rely=0.55, anchor="center")
        card.grid_propagate(False)

        switcher = ctk.CTkFrame(card, fg_color="transparent")
        switcher.grid(row=0, column=0, padx=24, pady=(24, 8), sticky="ew")
        switcher.grid_columnconfigure((0, 1, 2), weight=1, uniform="concat_modes")

        for col, (mode, title) in enumerate(self.MODES):
            btn = ctk.CTkButton(
                switcher,
                text=title,
                height=44,
                font=ctk.CTkFont(family="Segoe UI", size=18, weight="bold"),
                fg_color=self._style.btn_fg_color,
                hover_color=self._style.btn_hover_color,
                border_color=self._style.btn_border_color,
                text_color=self._style.btn_text_color,
                corner_radius=self._style.btn_corner_radius,
                border_width=self._style.btn_border_width,
                command=lambda m=mode: self.set_active_mode(m),
            )
            btn.grid(row=0, column=col, padx=6, sticky="ew")
            self.mode_buttons[mode] = btn

        content = ctk.CTkFrame(card, fg_color="transparent")
        content.grid(row=1, column=0, padx=0, pady=(4, 0), sticky="nsew")
        card.grid_rowconfigure(1, weight=1)
        card.grid_columnconfigure(0, weight=1)
        content.grid_rowconfigure(0, weight=1)
        content.grid_columnconfigure(0, weight=1)

        self._build_mode_frame(
            parent=content,
            mode="status",
            fields=[
                ("status_complicacao", "Status Complicação"),
                ("status_internacao_eletivo", "Status Internação Eletivo"),
                ("arquivo_saida", "Salvar arquivo concatenado"),
            ],
            execute_text="Executar Concatenação",
        )
        self._build_mode_frame(
            parent=content,
            mode="status_resposta",
            fields=[
                ("resposta_eletivo", "Status Resposta Eletivo"),
                ("resposta_internacao", "Status Resposta Internação"),
                ("arquivo_saida", "Salvar arquivo concatenado"),
            ],
            execute_text="Executar Concatenação",
        )
        self._build_mode_frame(
            parent=content,
            mode="livre",
            fields=[
                ("arquivo_a", "Arquivo A"),
                ("arquivo_b", "Arquivo B"),
                ("arquivo_saida", "Salvar arquivo concatenado"),
            ],
            execute_text="Executar Concatenação Livre",
        )

        self.set_active_mode(self._active_mode)

    def _build_mode_frame(
        self,
        parent: ctk.CTkFrame,
        mode: str,
        fields: list[tuple[str, str]],
        execute_text: str,
    ) -> None:
        frame = ctk.CTkFrame(parent, fg_color="transparent")
        frame.grid(row=0, column=0, sticky="nsew")
        frame.grid_columnconfigure(0, weight=0)
        frame.grid_columnconfigure(1, weight=1)
        frame.grid_columnconfigure(2, weight=0)
        self.mode_frames[mode] = frame
        self.rows[mode] = {}

        for row_index, (key, label) in enumerate(fields):
            row = FileSelectorRow(
                parent=frame,
                row_index=row_index,
                key=key,
                label=label,
                style=self._style,
                on_select=lambda k, m=mode: self._on_select_file(m, k),
                on_clear=lambda k, m=mode: self._on_clear_file(m, k),
            )
            self.rows[mode][key] = row

        execute_shadow = ctk.CTkFrame(
            frame,
            width=420,
            height=58,
            corner_radius=self._style.btn_corner_radius,
            fg_color=self._style.btn_shadow_color,
        )
        execute_shadow.grid(row=len(fields) + 1, column=0, columnspan=3, pady=(16, 0))

        execute_btn = ctk.CTkButton(
            frame,
            text=execute_text,
            width=420,
            height=56,
            font=ctk.CTkFont(family="Segoe UI", size=30, weight="bold"),
            fg_color=self._style.btn_fg_color,
            hover_color=self._style.btn_hover_color,
            border_color=self._style.btn_border_color,
            text_color=self._style.btn_text_color,
            corner_radius=self._style.btn_corner_radius,
            border_width=self._style.btn_border_width,
            command=lambda m=mode: self._on_execute(m),
        )
        execute_btn.grid(row=len(fields) + 1, column=0, columnspan=3, pady=(14, 0))

        status_label = ctk.CTkLabel(
            frame,
            text="",
            font=ctk.CTkFont(family="Segoe UI", size=16),
            text_color="#FFB1B1",
            fg_color="transparent",
        )
        status_label.grid(row=len(fields) + 2, column=0, columnspan=3, pady=(6, 8))
        self.exec_status_labels[mode] = status_label

    def set_active_mode(self, mode: str) -> None:
        if mode not in self.mode_frames:
            return
        self._active_mode = mode
        for key, frame in self.mode_frames.items():
            if key == mode:
                frame.grid()
            else:
                frame.grid_remove()

        for key, btn in self.mode_buttons.items():
            fg = self._style.btn_hover_color if key == mode else self._style.btn_fg_color
            btn.configure(fg_color=fg)

    def get_file_values(self, mode: str) -> dict[str, str]:
        return {key: row.get_value() for key, row in self.rows.get(mode, {}).items()}

    def set_file_value(self, mode: str, key: str, value: str) -> None:
        if mode in self.rows and key in self.rows[mode]:
            self.rows[mode][key].set_value(value)

    def clear_file_value(self, mode: str, key: str) -> None:
        if mode in self.rows and key in self.rows[mode]:
            self.rows[mode][key].clear()

    def get_file_labels(self, mode: str) -> dict[str, str]:
        return {key: row.label for key, row in self.rows.get(mode, {}).items()}

    def set_status_message(self, mode: str, text: str, color: str) -> None:
        if mode in self.exec_status_labels:
            self.exec_status_labels[mode].configure(text=text, text_color=color)

    def clear_status_message(self, mode: str) -> None:
        if mode in self.exec_status_labels:
            self.exec_status_labels[mode].configure(text="")
