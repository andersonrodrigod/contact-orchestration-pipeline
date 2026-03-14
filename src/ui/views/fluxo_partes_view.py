from __future__ import annotations

from typing import Callable

import customtkinter as ctk

from src.ui.components.file_selector_row import FileSelectorRow
from src.ui.components.ui_factory import (
    create_back_button,
    create_primary_button,
    grid_shadowed_primary_button,
)
from src.ui.state import UIStyle


class FluxoPartesView(ctk.CTkFrame):
    def __init__(
        self,
        parent: ctk.CTkFrame,
        style: UIStyle,
        specs: dict[str, dict[str, dict]],
        on_back: Callable[[], None],
        on_select_file: Callable[[str, str, str], None],
        on_clear_file: Callable[[str, str, str], None],
        on_execute: Callable[[str, str], None],
    ) -> None:
        super().__init__(parent, fg_color="transparent")
        self._style = style
        self._specs = specs
        self._on_back = on_back
        self._on_select_file = on_select_file
        self._on_clear_file = on_clear_file
        self._on_execute = on_execute

        self._active_context = "complicacao"
        self._active_action = "ingestao_normalizacao"

        self.context_buttons: dict[str, ctk.CTkButton] = {}
        self.action_buttons: dict[str, ctk.CTkButton] = {}
        self.action_frames: dict[tuple[str, str], ctk.CTkFrame] = {}
        self.rows: dict[tuple[str, str], dict[str, FileSelectorRow]] = {}
        self.status_labels: dict[tuple[str, str], ctk.CTkLabel] = {}

        self._build()

    def _build(self) -> None:
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        title = ctk.CTkLabel(
            self,
            text="Execução em Partes",
            font=ctk.CTkFont(size=42, weight="bold"),
            text_color="#E6F0FF",
        )
        title.place(relx=0.5, rely=0.085, anchor="center")

        create_back_button(self, self._style, self._on_back)

        card_shadow = ctk.CTkFrame(
            self,
            width=1120,
            height=560,
            corner_radius=20,
            fg_color=self._style.btn_shadow_color,
        )
        card_shadow.place(relx=0.5, rely=0.555, anchor="center")

        card = ctk.CTkFrame(
            self,
            width=1120,
            height=556,
            corner_radius=20,
            fg_color="#17264A",
            border_color="#21386B",
            border_width=1,
        )
        card.place(relx=0.5, rely=0.55, anchor="center")
        card.grid_propagate(False)
        card.grid_rowconfigure(2, weight=1)
        card.grid_columnconfigure(0, weight=1)

        context_switch = ctk.CTkFrame(card, fg_color="transparent")
        context_switch.grid(row=0, column=0, padx=24, pady=(20, 8), sticky="ew")
        context_switch.grid_columnconfigure((0, 1), weight=1, uniform="context")

        for col, (ctx_key, label) in enumerate(
            (("complicacao", "Complicação"), ("internacao", "Internação"))
        ):
            btn = create_primary_button(
                parent=context_switch,
                style=self._style,
                text=label,
                width=420,
                height=42,
                font_size=20,
                command=lambda c=ctx_key: self.set_active_context(c),
            )
            btn.grid(row=0, column=col, padx=6, sticky="ew")
            self.context_buttons[ctx_key] = btn

        action_switch = ctk.CTkFrame(card, fg_color="transparent")
        action_switch.grid(row=1, column=0, padx=24, pady=(6, 4), sticky="ew")
        action_switch.grid_columnconfigure((0, 1, 2, 3, 4), weight=1, uniform="actions")

        action_order = [
            ("ingestao_normalizacao", "Ingestão"),
            ("uniao_status", "União de Status"),
            ("criar_dataset", "Criar Dataset"),
            ("enviar_status_dataset", "Enviar Status"),
            ("orquestrar", "Orquestrar"),
        ]
        for col, (action_key, label) in enumerate(action_order):
            btn = create_primary_button(
                parent=action_switch,
                style=self._style,
                text=label,
                width=200,
                height=38,
                font_size=16,
                command=lambda a=action_key: self.set_active_action(a),
            )
            btn.grid(row=0, column=col, padx=4, sticky="ew")
            self.action_buttons[action_key] = btn

        content = ctk.CTkFrame(card, fg_color="transparent")
        content.grid(row=2, column=0, padx=0, pady=(2, 0), sticky="nsew")
        content.grid_rowconfigure(0, weight=1)
        content.grid_columnconfigure(0, weight=1)

        for context_key, actions in self._specs.items():
            for action_key, spec in actions.items():
                self._build_action_frame(
                    parent=content,
                    context_key=context_key,
                    action_key=action_key,
                    fields=spec["fields"],
                )

        self.set_active_context(self._active_context)
        self.set_active_action(self._active_action)

    def _build_action_frame(
        self,
        parent: ctk.CTkFrame,
        context_key: str,
        action_key: str,
        fields: list[tuple[str, str]],
    ) -> None:
        frame = ctk.CTkFrame(parent, fg_color="#17264A")
        frame.grid(row=0, column=0, sticky="nsew")
        frame.grid_columnconfigure(0, weight=0)
        frame.grid_columnconfigure(1, weight=1)
        frame.grid_columnconfigure(2, weight=0)
        key = (context_key, action_key)
        self.action_frames[key] = frame
        self.rows[key] = {}

        for row_index, (field_key, field_label) in enumerate(fields):
            row = FileSelectorRow(
                parent=frame,
                row_index=row_index,
                key=field_key,
                label=field_label,
                style=self._style,
                on_select=lambda k, c=context_key, a=action_key: self._on_select_file(c, a, k),
                on_clear=lambda k, c=context_key, a=action_key: self._on_clear_file(c, a, k),
            )
            self.rows[key][field_key] = row

        grid_shadowed_primary_button(
            parent=frame,
            style=self._style,
            row=len(fields) + 1,
            text="Executar Etapa",
            width=380,
            height=56,
            font_size=28,
            command=lambda c=context_key, a=action_key: self._on_execute(c, a),
            shadow_pady=(16, 0),
            button_pady=(12, 0),
        )

        status_label = ctk.CTkLabel(
            frame,
            text="",
            font=ctk.CTkFont(family="Segoe UI", size=16),
            text_color="#A7C8FF",
            fg_color="transparent",
            anchor="w",
            justify="left",
            wraplength=1020,
        )
        status_label.grid(row=len(fields) + 2, column=0, columnspan=3, padx=24, pady=(8, 8), sticky="w")
        self.status_labels[key] = status_label

    def set_active_context(self, context_key: str) -> None:
        if context_key not in self.context_buttons:
            return
        self._active_context = context_key
        for key, btn in self.context_buttons.items():
            btn.configure(
                fg_color=self._style.btn_hover_color if key == context_key else self._style.btn_fg_color
            )
        self._show_active_frame()

    def set_active_action(self, action_key: str) -> None:
        if action_key not in self.action_buttons:
            return
        self._active_action = action_key
        for key, btn in self.action_buttons.items():
            btn.configure(
                fg_color=self._style.btn_hover_color if key == action_key else self._style.btn_fg_color
            )
        self._show_active_frame()

    def _show_active_frame(self) -> None:
        active_key = (self._active_context, self._active_action)
        for frame_key, frame in self.action_frames.items():
            if frame_key == active_key:
                frame.grid()
            else:
                frame.grid_remove()

    def get_file_values(self, context_key: str, action_key: str) -> dict[str, str]:
        key = (context_key, action_key)
        return {field_key: row.get_value() for field_key, row in self.rows.get(key, {}).items()}

    def set_file_value(self, context_key: str, action_key: str, field_key: str, value: str) -> None:
        key = (context_key, action_key)
        if key in self.rows and field_key in self.rows[key]:
            self.rows[key][field_key].set_value(value)

    def clear_file_value(self, context_key: str, action_key: str, field_key: str) -> None:
        key = (context_key, action_key)
        if key in self.rows and field_key in self.rows[key]:
            self.rows[key][field_key].clear()

    def get_file_labels(self, context_key: str, action_key: str) -> dict[str, str]:
        key = (context_key, action_key)
        return {field_key: row.label for field_key, row in self.rows.get(key, {}).items()}

    def set_status_message(self, context_key: str, action_key: str, text: str, color: str) -> None:
        key = (context_key, action_key)
        if key in self.status_labels:
            self.status_labels[key].configure(text=text, text_color=color)

    def clear_status_message(self, context_key: str, action_key: str) -> None:
        key = (context_key, action_key)
        if key in self.status_labels:
            self.status_labels[key].configure(text="")
