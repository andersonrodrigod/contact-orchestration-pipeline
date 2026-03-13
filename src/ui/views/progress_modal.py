from __future__ import annotations

from typing import Callable

import customtkinter as ctk

from src.ui.state import UIStyle


class ProgressModal:
    def __init__(
        self,
        parent: ctk.CTk,
        style: UIStyle,
        center_window: Callable[[ctk.CTkToplevel, int, int], None],
        on_cancel: Callable[[], None],
    ) -> None:
        self.parent = parent
        self.style = style
        self._center_window = center_window
        self._on_cancel = on_cancel

        self.window: ctk.CTkToplevel | None = None
        self.progress_label: ctk.CTkLabel | None = None
        self.progress_pct_label: ctk.CTkLabel | None = None
        self.progress_bar: ctk.CTkProgressBar | None = None
        self.cancel_btn: ctk.CTkButton | None = None

    def open(self) -> None:
        self.close()

        modal = ctk.CTkToplevel(self.parent)
        self.window = modal
        modal.title("Executando ETL")
        modal.geometry("940x520")
        modal.resizable(False, False)
        modal.transient(self.parent)
        modal.grab_set()
        modal.configure(fg_color="#101A35")
        self._center_window(modal, 940, 520)

        card = ctk.CTkFrame(
            modal,
            fg_color="#1A2A52",
            corner_radius=22,
            border_color="#223D73",
            border_width=1,
        )
        card.pack(fill="both", expand=True, padx=26, pady=22)

        titulo = ctk.CTkLabel(
            card,
            text="Executando ETL",
            font=ctk.CTkFont(family="Segoe UI", size=48, weight="bold"),
            text_color="#E6F0FF",
            fg_color="transparent",
        )
        titulo.pack(pady=(30, 12))

        separador = ctk.CTkFrame(card, height=2, fg_color="#2E4B83")
        separador.pack(fill="x", padx=40, pady=(0, 24))

        progress_area = ctk.CTkFrame(card, fg_color="transparent")
        progress_area.pack(fill="x", padx=64)
        progress_area.grid_columnconfigure(0, weight=1)
        progress_area.grid_columnconfigure(1, weight=0)

        self.progress_bar = ctk.CTkProgressBar(
            progress_area,
            height=34,
            corner_radius=8,
            border_width=1,
            border_color="#3B67B5",
            fg_color="#0F1D3E",
            progress_color="#3B78F0",
        )
        self.progress_bar.grid(row=0, column=0, sticky="ew", padx=(0, 14))
        self.progress_bar.set(0)

        self.progress_pct_label = ctk.CTkLabel(
            progress_area,
            text="0%",
            font=ctk.CTkFont(family="Segoe UI", size=24, weight="bold"),
            text_color="#DCEAFF",
            fg_color="transparent",
        )
        self.progress_pct_label.grid(row=0, column=1, sticky="e")

        self.progress_label = ctk.CTkLabel(
            card,
            text="Preparando execução...",
            font=ctk.CTkFont(family="Segoe UI", size=20),
            text_color="#D5E4FF",
            fg_color="transparent",
        )
        self.progress_label.pack(pady=(30, 42))

        self.cancel_btn = ctk.CTkButton(
            card,
            text="Cancelar",
            width=260,
            height=60,
            font=ctk.CTkFont(family="Segoe UI", size=36, weight="bold"),
            fg_color=self.style.btn_fg_color,
            hover_color=self.style.btn_hover_color,
            border_color=self.style.btn_border_color,
            text_color=self.style.btn_text_color,
            corner_radius=self.style.btn_corner_radius,
            border_width=self.style.btn_border_width,
            command=self._on_cancel,
        )
        self.cancel_btn.pack(pady=(0, 24))

        modal.protocol("WM_DELETE_WINDOW", self._on_cancel)

    def set_progress(self, ratio: float, pct_text: str) -> None:
        if self.progress_bar is not None:
            self.progress_bar.set(ratio)
        if self.progress_pct_label is not None:
            self.progress_pct_label.configure(text=pct_text)

    def set_status(self, text: str) -> None:
        if self.progress_label is not None:
            self.progress_label.configure(text=text)

    def set_cancel_as_close(self, on_close: Callable[[], None]) -> None:
        if self.cancel_btn is not None:
            self.cancel_btn.configure(text="Fechar", command=on_close)

    def exists(self) -> bool:
        return self.window is not None and self.window.winfo_exists()

    def close(self) -> None:
        if self.window is not None and self.window.winfo_exists():
            self.window.grab_release()
            self.window.destroy()
        self.window = None
        self.progress_label = None
        self.progress_pct_label = None
        self.progress_bar = None
        self.cancel_btn = None
