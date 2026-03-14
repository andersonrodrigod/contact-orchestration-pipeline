from __future__ import annotations

from typing import Callable

import customtkinter as ctk

from src.ui.views.file_mode_base_view import FileModeBaseView
from src.ui.state import UIStyle


class ComplicacaoView(FileModeBaseView):
    def __init__(
        self,
        parent: ctk.CTkFrame,
        style: UIStyle,
        on_back: Callable[[], None],
        on_execute: Callable[[], None],
        on_select_file: Callable[[str], None],
        on_clear_file: Callable[[str], None],
    ) -> None:
        fields = [
            ("complicacao_dataset", "Complicações cirúrgicas"),
            ("status", "Status"),
            ("flow_complicacao", "Flow de Resposta Complicações cirúrgicas"),
            ("output_dir", "Pasta de saída"),
        ]
        super().__init__(
            parent=parent,
            style=style,
            title="Modo Complicação",
            fields=fields,
            card_shadow_height=458,
            card_height=454,
            status_bottom_pady=18,
            on_back=on_back,
            on_execute=on_execute,
            on_select_file=on_select_file,
            on_clear_file=on_clear_file,
        )
