from __future__ import annotations

from typing import Callable

import customtkinter as ctk

from src.ui.components.file_selector_row import FileSelectorRow
from src.ui.components.ui_factory import create_back_button, grid_shadowed_primary_button
from src.ui.state import UIStyle


class FileModeBaseView(ctk.CTkFrame):
    def __init__(
        self,
        parent: ctk.CTkFrame,
        style: UIStyle,
        title: str,
        fields: list[tuple[str, str]],
        card_shadow_height: int,
        card_height: int,
        status_bottom_pady: int,
        on_back: Callable[[], None],
        on_execute: Callable[[], None],
        on_select_file: Callable[[str], None],
        on_clear_file: Callable[[str], None],
    ) -> None:
        super().__init__(parent, fg_color="transparent")
        self._style = style
        self._title = title
        self._fields = fields
        self._card_shadow_height = card_shadow_height
        self._card_height = card_height
        self._status_bottom_pady = status_bottom_pady
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

        title_label = ctk.CTkLabel(
            self,
            text=self._title,
            font=ctk.CTkFont(size=42, weight="bold"),
            text_color="#E6F0FF",
        )
        title_label.place(relx=0.5, rely=0.085, anchor="center")

        create_back_button(self, self._style, self._on_back)

        card_shadow = ctk.CTkFrame(
            self,
            width=1120,
            height=self._card_shadow_height,
            corner_radius=20,
            fg_color=self._style.btn_shadow_color,
        )
        card_shadow.place(relx=0.5, rely=0.555, anchor="center")

        card = ctk.CTkFrame(
            self,
            width=1120,
            height=self._card_height,
            corner_radius=20,
            fg_color="#17264A",
            border_color="#21386B",
            border_width=1,
        )
        card.place(relx=0.5, rely=0.55, anchor="center")
        card.grid_propagate(False)

        card.grid_columnconfigure(0, weight=0)
        card.grid_columnconfigure(1, weight=1)
        card.grid_columnconfigure(2, weight=0)

        for row_index, (key, label) in enumerate(self._fields):
            self.rows[key] = FileSelectorRow(
                parent=card,
                row_index=row_index,
                key=key,
                label=label,
                style=self._style,
                on_select=self._on_select_file,
                on_clear=self._on_clear_file,
            )

        grid_shadowed_primary_button(
            parent=card,
            style=self._style,
            row=len(self._fields) + 1,
            text="Executar ETL",
            width=360,
            height=56,
            font_size=34,
            command=self._on_execute,
            shadow_pady=(20, 0),
            button_pady=(16, 0),
        )

        self.exec_status_label = ctk.CTkLabel(
            card,
            text="",
            font=ctk.CTkFont(family="Segoe UI", size=16),
            text_color="#FFB1B1",
            fg_color="transparent",
        )
        self.exec_status_label.grid(
            row=len(self._fields) + 2,
            column=0,
            columnspan=3,
            pady=(8, self._status_bottom_pady),
        )

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
