from __future__ import annotations

from typing import Callable

import customtkinter as ctk

from src.ui.components.file_selector_row import FileSelectorRow
from src.ui.state import UIStyle


class ComplicacaoView(ctk.CTkFrame):
    def __init__(
        self,
        parent: ctk.CTkFrame,
        style: UIStyle,
        on_back: Callable[[], None],
        on_execute: Callable[[], None],
        on_select_file: Callable[[str], None],
        on_clear_file: Callable[[str], None],
    ) -> None:
        super().__init__(parent, fg_color="transparent")
        self._style = style
        self._on_back = on_back
        self._on_execute = on_execute
        self._on_select_file = on_select_file
        self._on_clear_file = on_clear_file

        self.rows: dict[str, FileSelectorRow] = {}
        self.exec_status_label: ctk.CTkLabel | None = None

        self._build()

    def _build(self) -> None:
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        titulo = ctk.CTkLabel(
            self,
            text="Modo Complicação",
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
            height=458,
            corner_radius=20,
            fg_color=self._style.btn_shadow_color,
        )
        card_shadow.place(relx=0.5, rely=0.53, anchor="center")

        card = ctk.CTkFrame(
            self,
            width=1120,
            height=454,
            corner_radius=20,
            fg_color="#17264A",
            border_color="#21386B",
            border_width=1,
        )
        card.place(relx=0.5, rely=0.525, anchor="center")
        card.grid_propagate(False)

        card.grid_columnconfigure(0, weight=0)
        card.grid_columnconfigure(1, weight=1)
        card.grid_columnconfigure(2, weight=0)

        campos = [
            ("complicacao_dataset", "Complicações cirúrgicas"),
            ("status", "Status"),
            ("flow_complicacao", "Flow de Resposta Complicações cirúrgicas"),
            ("output_dir", "Pasta de saída"),
        ]

        for row_index, (key, label) in enumerate(campos):
            self.rows[key] = FileSelectorRow(
                parent=card,
                row_index=row_index,
                key=key,
                label=label,
                style=self._style,
                on_select=self._on_select_file,
                on_clear=self._on_clear_file,
            )

        executar_shadow = ctk.CTkFrame(
            card,
            width=360,
            height=58,
            corner_radius=self._style.btn_corner_radius,
            fg_color=self._style.btn_shadow_color,
        )
        executar_shadow.grid(row=len(campos) + 1, column=0, columnspan=3, pady=(20, 0))

        executar_btn = ctk.CTkButton(
            card,
            text="Executar ETL",
            width=360,
            height=56,
            font=ctk.CTkFont(family="Segoe UI", size=34, weight="bold"),
            fg_color=self._style.btn_fg_color,
            hover_color=self._style.btn_hover_color,
            border_color=self._style.btn_border_color,
            text_color=self._style.btn_text_color,
            corner_radius=self._style.btn_corner_radius,
            border_width=self._style.btn_border_width,
            command=self._on_execute,
        )
        executar_btn.grid(row=len(campos) + 1, column=0, columnspan=3, pady=(16, 0))

        self.exec_status_label = ctk.CTkLabel(
            card,
            text="",
            font=ctk.CTkFont(family="Segoe UI", size=16),
            text_color="#FFB1B1",
            fg_color="transparent",
        )
        self.exec_status_label.grid(row=len(campos) + 2, column=0, columnspan=3, pady=(8, 18))

    def get_file_values(self) -> dict[str, str]:
        return {key: row.get_value() for key, row in self.rows.items()}

    def set_file_value(self, key: str, value: str) -> None:
        if key in self.rows:
            self.rows[key].set_value(value)

    def clear_file_value(self, key: str) -> None:
        if key in self.rows:
            self.rows[key].clear()

    def get_file_labels(self) -> dict[str, str]:
        return {key: row.label for key, row in self.rows.items()}

    def set_status_message(self, text: str, color: str) -> None:
        if self.exec_status_label is not None:
            self.exec_status_label.configure(text=text, text_color=color)

    def clear_status_message(self) -> None:
        if self.exec_status_label is not None:
            self.exec_status_label.configure(text="")
