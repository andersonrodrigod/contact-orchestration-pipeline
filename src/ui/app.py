from __future__ import annotations

try:
    import customtkinter as ctk
except ImportError as erro:  # pragma: no cover
    raise ImportError(
        "customtkinter nao encontrado. Instale com: pip install customtkinter"
    ) from erro


class App(ctk.CTk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Menu Principal")
        self.geometry("1320x840")
        self.minsize(1176, 730)

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

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

        back_btn = ctk.CTkButton(
            frame,
            text="Voltar ao Menu",
            width=220,
            height=44,
            fg_color="#1b4fb8",
            hover_color="#2668ed",
            border_width=0,
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
            ("Modo Ambos Complicação e Internação", "frame_fluxo_completo"),
            ("Modo Complicação", "frame_modo_complicacao"),
            ("Modo Internação", "frame_modo_internacao"),
            ("Concatenar Arquivos", "frame_concatenar"),
            ("União de Status e Flow de Respostas", "frame_juntar_status"),
            ("Execução em Partes", "frame_fluxo_partes"),
            ("Limpeza de Dados", "frame_limpeza_dados"),
            ("Utilitário", "frame_utilitario"),
        ]

        for i, (texto, destino) in enumerate(botoes):
            item_frame = ctk.CTkFrame(
                botoes_frame,
                fg_color="transparent",
                width=600,
                height=73,
            )
            item_frame.pack(pady=(30, 4) if i == 0 else 4)
            item_frame.pack_propagate(False)

            sombra = ctk.CTkFrame(
                item_frame,
                width=600,
                height=65,
                corner_radius=12,
                fg_color="#0b1731",
            )
            sombra.place(x=0, y=6)

            btn = ctk.CTkButton(
                item_frame,
                text=texto,
                width=600,
                height=65,
                corner_radius=12,
                fg_color="#122852",
                hover_color="#1b3a73",
                border_width=0,
                anchor="w",
                command=lambda d=destino: self.show_frame(d),
            )
            btn.place(x=0, y=0)

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
