from __future__ import annotations

from collections.abc import Callable

import customtkinter as ctk

from src.ui.state import UIStyle


def create_back_button(
    parent: ctk.CTkFrame,
    style: UIStyle,
    command: Callable[[], None],
    shadow_xy: tuple[int, int] = (44, 52),
    button_xy: tuple[int, int] = (42, 50),
) -> ctk.CTkButton:
    shadow = ctk.CTkFrame(
        parent,
        width=62,
        height=52,
        corner_radius=style.btn_corner_radius,
        fg_color=style.btn_shadow_color,
    )
    shadow.place(x=shadow_xy[0], y=shadow_xy[1], anchor="nw")

    btn = ctk.CTkButton(
        parent,
        text="\u2190",
        width=60,
        height=50,
        font=ctk.CTkFont(family="Segoe UI", size=26, weight="bold"),
        fg_color=style.btn_fg_color,
        hover_color=style.btn_hover_color,
        border_color=style.btn_border_color,
        text_color=style.btn_text_color,
        corner_radius=style.btn_corner_radius,
        border_width=style.btn_border_width,
        command=command,
    )
    btn.place(x=button_xy[0], y=button_xy[1], anchor="nw")
    return btn


def create_primary_button(
    parent: ctk.CTkFrame,
    style: UIStyle,
    text: str,
    width: int,
    height: int,
    font_size: int,
    command: Callable[[], None],
) -> ctk.CTkButton:
    return ctk.CTkButton(
        parent,
        text=text,
        width=width,
        height=height,
        font=ctk.CTkFont(family="Segoe UI", size=font_size, weight="bold"),
        fg_color=style.btn_fg_color,
        hover_color=style.btn_hover_color,
        border_color=style.btn_border_color,
        text_color=style.btn_text_color,
        corner_radius=style.btn_corner_radius,
        border_width=style.btn_border_width,
        command=command,
    )


def grid_shadowed_primary_button(
    parent: ctk.CTkFrame,
    style: UIStyle,
    row: int,
    text: str,
    width: int,
    height: int,
    font_size: int,
    command: Callable[[], None],
    columnspan: int = 3,
    shadow_pady: tuple[int, int] = (16, 0),
    button_pady: tuple[int, int] = (12, 0),
) -> ctk.CTkButton:
    shadow = ctk.CTkFrame(
        parent,
        width=width,
        height=height + 2,
        corner_radius=style.btn_corner_radius,
        fg_color=style.btn_shadow_color,
    )
    shadow.grid(row=row, column=0, columnspan=columnspan, pady=shadow_pady)

    btn = create_primary_button(
        parent=parent,
        style=style,
        text=text,
        width=width,
        height=height,
        font_size=font_size,
        command=command,
    )
    btn.grid(row=row, column=0, columnspan=columnspan, pady=button_pady)
    return btn
