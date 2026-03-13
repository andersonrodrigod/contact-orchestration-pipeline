from __future__ import annotations
from pathlib import Path

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

        self.btn_fg_color = "#1C3A66"
        self.btn_hover_color = "#234B85"
        self.btn_border_color = "#2C5AA0"
        self.btn_text_color = "#E6F0FF"
        self.btn_corner_radius = 14
        self.btn_border_width = 1
        self.btn_shadow_color = "#0D1526"
        self.icon_color = "#8FB7FF"

        self._icon_refs: list[ctk.CTkImage] = []
        self.frames: dict[str, ctk.CTkFrame] = {}
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
            corner_radius=self.btn_corner_radius,
            fg_color=self.btn_shadow_color,
        )
        back_shadow.place(relx=0.5, rely=0.524, anchor="center")

        back_btn = ctk.CTkButton(
            frame,
            text="Voltar ao Menu",
            font=ctk.CTkFont(family="Segoe UI", size=16, weight="bold"),
            width=220,
            height=44,
            fg_color=self.btn_fg_color,
            hover_color=self.btn_hover_color,
            border_color=self.btn_border_color,
            text_color=self.btn_text_color,
            corner_radius=self.btn_corner_radius,
            border_width=self.btn_border_width,
            command=lambda: self.show_frame("menu_frame"),
        )
        back_btn.place(relx=0.5, rely=0.52, anchor="center")

    def _create_menu_frame(self) -> None:
        frame = ctk.CTkFrame(self.container, fg_color="transparent")
        self.frames["menu_frame"] = frame

        frame.grid_columnconfigure(0, weight=1)
        frame.grid_rowconfigure(0, weight=1)

        titulo = ctk.CTkLabel(
            frame,
            text="Menu Principal",
            font=ctk.CTkFont(size=40, weight="bold"),
            text_color="#eaf0ff",
        )
        titulo.place(relx=0.5, rely=0.065, anchor="center")

        botoes_frame = ctk.CTkFrame(frame, fg_color="transparent")
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
                corner_radius=self.btn_corner_radius,
                fg_color=self.btn_shadow_color,
            )
            sombra.place(x=0, y=4)

            btn = ctk.CTkButton(
                item_frame,
                text="",
                width=794,
                height=btn_height,
                corner_radius=self.btn_corner_radius,
                fg_color=self.btn_fg_color,
                hover_color=self.btn_hover_color,
                border_color=self.btn_border_color,
                text_color=self.btn_text_color,
                border_width=self.btn_border_width,
                command=lambda d=destino: self.show_frame(d),
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
                text_color=self.btn_text_color,
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

            overlays = (
                left_icon_label,
                text_label,
                right_icon_label,
            )

            def on_enter(_event: object, b: ctk.CTkButton = btn, ws: tuple[ctk.CTkLabel, ...] = overlays) -> None:
                b.configure(fg_color=self.btn_hover_color)
                for w in ws:
                    w.configure(fg_color=self.btn_hover_color, bg_color=self.btn_hover_color)

            def on_leave(_event: object, b: ctk.CTkButton = btn, ws: tuple[ctk.CTkLabel, ...] = overlays) -> None:
                b.configure(fg_color=self.btn_fg_color)
                for w in ws:
                    w.configure(fg_color=self.btn_fg_color, bg_color=self.btn_fg_color)

            for overlay in overlays:
                overlay.bind("<Button-1>", lambda _event, d=destino: self.show_frame(d))
                overlay.bind("<Enter>", on_enter)
                overlay.bind("<Leave>", on_leave)

            btn.bind("<Enter>", on_enter)
            btn.bind("<Leave>", on_leave)

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
        tinted = Image.new("RGBA", image.size, self.icon_color)
        tinted.putalpha(alpha)
        return tinted

    def _create_frame_fluxo_completo(self) -> None:
        frame = ctk.CTkFrame(self.container, fg_color="transparent")
        self.frames["frame_fluxo_completo"] = frame
        self._build_base_content(frame, "frame_fluxo_completo")

    def _create_frame_modo_complicacao(self) -> None:
        frame = ctk.CTkFrame(self.container, fg_color="transparent")
        self.frames["frame_modo_complicacao"] = frame
        self._build_base_content(frame, "frame_modo_complicacao")

    def _create_frame_modo_internacao(self) -> None:
        frame = ctk.CTkFrame(self.container, fg_color="transparent")
        self.frames["frame_modo_internacao"] = frame
        self._build_base_content(frame, "frame_modo_internacao")

    def _create_frame_fluxo_partes(self) -> None:
        frame = ctk.CTkFrame(self.container, fg_color="transparent")
        self.frames["frame_fluxo_partes"] = frame
        self._build_base_content(frame, "frame_fluxo_partes")

    def _create_frame_juntar_status(self) -> None:
        frame = ctk.CTkFrame(self.container, fg_color="transparent")
        self.frames["frame_juntar_status"] = frame
        self._build_base_content(frame, "frame_juntar_status")

    def _create_frame_concatenar(self) -> None:
        frame = ctk.CTkFrame(self.container, fg_color="transparent")
        self.frames["frame_concatenar"] = frame
        self._build_base_content(frame, "frame_concatenar")

    def _create_frame_limpeza_dados(self) -> None:
        frame = ctk.CTkFrame(self.container, fg_color="transparent")
        self.frames["frame_limpeza_dados"] = frame
        self._build_base_content(frame, "frame_limpeza_dados")

    def _create_frame_utilitario(self) -> None:
        frame = ctk.CTkFrame(self.container, fg_color="transparent")
        self.frames["frame_utilitario"] = frame
        self._build_base_content(frame, "frame_utilitario")

    def _create_frame_configuracoes(self) -> None:
        frame = ctk.CTkFrame(self.container, fg_color="transparent")
        self.frames["frame_configuracoes"] = frame
        self._build_base_content(frame, "frame_configuracoes")
