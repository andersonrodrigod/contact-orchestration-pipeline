from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class UIStyle:
    btn_fg_color: str = "#1C3A66"
    btn_hover_color: str = "#234B85"
    btn_border_color: str = "#2C5AA0"
    btn_text_color: str = "#E6F0FF"
    btn_corner_radius: int = 14
    btn_border_width: int = 1
    btn_shadow_color: str = "#0D1526"
    icon_color: str = "#8FB7FF"


@dataclass
class UIRuntimeState:
    current_execution_plan: dict[str, str] = field(default_factory=dict)
    ack_missing_all_respostas: bool = False
