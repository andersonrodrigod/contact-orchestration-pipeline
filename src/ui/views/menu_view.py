from __future__ import annotations

from typing import Callable

import customtkinter as ctk

from src.ui.state import UIStyle


class MenuView(ctk.CTkFrame):
    def __init__(
        self,
        parent: ctk.CTkFrame,
        style: UIStyle,
        load_icon: Callable[[str, int, int], ctk.CTkImage],
        on_navigate: Callable[[str], None],
    ) -> None:
        super().__init__(parent, fg_color="transparent")
        self._style = style
        self._load_icon = load_icon
        self._on_navigate = on_navigate
        self._build()

    def _build(self) -> None:
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        titulo = ctk.CTkLabel(
            self,
            text="Menu Principal",
            font=ctk.CTkFont(size=40, weight="bold"),
            text_color="#eaf0ff",
        )
        titulo.place(relx=0.5, rely=0.065, anchor="center")

        botoes_frame = ctk.CTkFrame(self, fg_color="transparent")
        botoes_frame.place(relx=0.5, rely=0.50, anchor="center")

        botoes = [
            ("Modo Ambos Complicação e Internação", "frame_fluxo_completo", "ambos.png"),
            ("Modo Complicação", "frame_modo_complicacao", "complicacao.png"),
            ("Modo Internação", "frame_modo_internacao", "internacao.png"),
            ("Concatenar Arquivos", "frame_concatenar", "concatenar.png"),
            ("União de Status e Flow de Respostas", "frame_juntar_status", "uniao.png"),
            ("Execução em Partes", "frame_fluxo_partes", "partes.png"),
            ("Limpeza de Dados", "frame_limpeza_dados", "limpeza.png"),
            ("Utilitário", "frame_utilitario", "utils.png"),
            ("Configurações", "frame_configuracoes", "configuracao.png"),
        ]

        btn_height = 65
        icon_height = int(btn_height * 0.81)
        left_icon_width = int((30 * (4 / 3)) * 1.2)
        right_icon_width = int((24 * (4 / 3)) * 1.2)
        right_icon = self._load_icon("right.png", right_icon_width, icon_height)

        for idx, (texto, destino, icon_name) in enumerate(botoes):
            left_icon = self._load_icon(icon_name, left_icon_width, icon_height)

            item_frame = ctk.CTkFrame(
                botoes_frame,
                fg_color="transparent",
                width=800,
                height=72,
            )
            item_frame.pack(pady=(40, 0) if idx == 0 else 0)
            item_frame.pack_propagate(False)

            sombra = ctk.CTkFrame(
                item_frame,
                width=800,
                height=65,
                corner_radius=self._style.btn_corner_radius,
                fg_color=self._style.btn_shadow_color,
            )
            sombra.place(x=0, y=4)

            btn = ctk.CTkButton(
                item_frame,
                text="",
                width=794,
                height=btn_height,
                corner_radius=self._style.btn_corner_radius,
                fg_color=self._style.btn_fg_color,
                hover_color=self._style.btn_hover_color,
                border_color=self._style.btn_border_color,
                text_color=self._style.btn_text_color,
                border_width=self._style.btn_border_width,
                command=lambda d=destino: self._on_navigate(d),
            )
            btn.place(x=0, y=0)

            left_icon_label = ctk.CTkLabel(
                btn,
                text="",
                image=left_icon,
                fg_color="transparent",
                bg_color="transparent",
            )
            left_icon_label.place(x=20, y=32, anchor="w")

            text_label = ctk.CTkLabel(
                btn,
                text=texto,
                font=ctk.CTkFont(family="Segoe UI", size=24, weight="bold"),
                text_color=self._style.btn_text_color,
                fg_color="transparent",
                bg_color="transparent",
            )
            text_label.place(x=20 + left_icon_width + 10, y=32, anchor="w")

            right_icon_label = ctk.CTkLabel(
                btn,
                text="",
                image=right_icon,
                fg_color="transparent",
                bg_color="transparent",
            )
            right_icon_label.place(x=770, y=32, anchor="e")

            overlays = (left_icon_label, text_label, right_icon_label)

            def on_enter(
                _event: object,
                b: ctk.CTkButton = btn,
                ws: tuple[ctk.CTkLabel, ...] = overlays,
            ) -> None:
                b.configure(fg_color=self._style.btn_hover_color)
                for w in ws:
                    w.configure(
                        fg_color=self._style.btn_hover_color,
                        bg_color=self._style.btn_hover_color,
                    )

            def on_leave(
                _event: object,
                b: ctk.CTkButton = btn,
                ws: tuple[ctk.CTkLabel, ...] = overlays,
            ) -> None:
                b.configure(fg_color=self._style.btn_fg_color)
                for w in ws:
                    w.configure(
                        fg_color=self._style.btn_fg_color,
                        bg_color=self._style.btn_fg_color,
                    )

            for overlay in overlays:
                overlay.bind("<Button-1>", lambda _event, d=destino: self._on_navigate(d))
                overlay.bind("<Enter>", on_enter)
                overlay.bind("<Leave>", on_leave)

            btn.bind("<Enter>", on_enter)
            btn.bind("<Leave>", on_leave)
