from __future__ import annotations

from typing import Callable

import customtkinter as ctk

from src.ui.state import UIStyle


class FileSelectorRow:
    def __init__(
        self,
        parent: ctk.CTkFrame,
        row_index: int,
        key: str,
        label: str,
        style: UIStyle,
        on_select: Callable[[str], None],
        on_clear: Callable[[str], None],
    ) -> None:
        self.key = key
        self.label = label
        self.var = ctk.StringVar(value="")

        row_pady = (36, 8) if row_index == 0 else 8

        self.select_btn = ctk.CTkButton(
            parent,
            text=label,
            width=275,
            height=56,
            font=ctk.CTkFont(family="Segoe UI", size=20, weight="bold"),
            fg_color=style.btn_fg_color,
            hover_color=style.btn_hover_color,
            border_color=style.btn_border_color,
            text_color=style.btn_text_color,
            corner_radius=style.btn_corner_radius,
            border_width=style.btn_border_width,
            anchor="w",
            command=lambda: on_select(key),
        )
        self.select_btn.grid(row=row_index, column=0, sticky="ew", padx=(24, 12), pady=row_pady)

        self.path_entry = ctk.CTkEntry(
            parent,
            textvariable=self.var,
            height=56,
            font=ctk.CTkFont(family="Segoe UI", size=18),
            fg_color="#0E1A38",
            border_color=style.btn_border_color,
            text_color=style.btn_text_color,
            corner_radius=style.btn_corner_radius,
        )
        self.path_entry.grid(row=row_index, column=1, sticky="ew", padx=(0, 12), pady=row_pady)

        self.clear_btn = ctk.CTkButton(
            parent,
            text="X",
            width=56,
            height=56,
            font=ctk.CTkFont(family="Segoe UI", size=28, weight="bold"),
            fg_color=style.btn_fg_color,
            hover_color=style.btn_hover_color,
            border_color=style.btn_border_color,
            text_color="#AAC9FF",
            corner_radius=style.btn_corner_radius,
            border_width=style.btn_border_width,
            command=lambda: on_clear(key),
        )
        self.clear_btn.grid(row=row_index, column=2, sticky="ew", padx=(0, 24), pady=row_pady)

    def set_value(self, value: str) -> None:
        self.var.set(value)

    def get_value(self) -> str:
        return self.var.get()

    def clear(self) -> None:
        self.var.set("")
