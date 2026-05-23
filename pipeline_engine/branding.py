"""OmniCAD CLI branding: load config from JSON, render startup banner."""
from __future__ import annotations

from importlib import metadata
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel
from rich import box as rich_box
from rich.align import Align
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from pipeline_engine.core import storage

_BOX_MAP: dict[str, rich_box.Box] = {
    "ROUNDED": rich_box.ROUNDED,
    "HEAVY":   rich_box.HEAVY,
    "DOUBLE":  rich_box.DOUBLE,
    "SQUARE":  rich_box.SQUARE,
    "MINIMAL": rich_box.MINIMAL,
}


class BrandingConfig(BaseModel):
    name: str
    display_name: str
    prompt: str
    version: str
    description: str
    logo: str = ""
    logo_style: str = "light_steel_blue1"
    border_style: str = "grey50"
    tagline_style: str = "grey70"
    box_style: Literal["ROUNDED", "HEAVY", "DOUBLE", "SQUARE", "MINIMAL"] = "ROUNDED"

    @property
    def resolved_version(self) -> str:
        if self.version == "@auto":
            try:
                return metadata.version("pipeline-engine")
            except metadata.PackageNotFoundError:
                return "0.0.0"
        return self.version


_BUNDLED_DEFAULT = BrandingConfig(
    name="omnicad",
    display_name="OmniCAD",
    prompt="OmniCAD",
    version="@auto",
    description="DAG-based CAD workflow orchestration engine",
    logo="",
)


def _default_config_path() -> Path:
    return Path(__file__).resolve().parent.parent / "config" / "branding.json"


def load_branding(path: Optional[Path] = None) -> BrandingConfig:
    """Load branding config from JSON; returns bundled defaults if file missing."""
    p = path or _default_config_path()
    data = storage.load_json_safe(p)
    if data is None:
        return _BUNDLED_DEFAULT
    return BrandingConfig.model_validate(data)


def print_banner(
    console: Console,
    cfg: Optional[BrandingConfig] = None,
    *,
    workspace: Optional[Path] = None,
) -> None:
    """Render the startup banner inside a rounded Panel, centered."""
    cfg = cfg or load_branding()

    text = Text()
    if cfg.logo:
        text.append(cfg.logo + "\n\n", style=cfg.logo_style)
    text.append(
        f"{cfg.display_name}  v{cfg.resolved_version}\n",
        style=cfg.tagline_style,
    )
    text.append(cfg.description, style=cfg.tagline_style)

    panel = Panel(
        Align.center(text),
        box=_BOX_MAP[cfg.box_style],
        border_style=cfg.border_style,
        padding=(1, 2),
    )
    console.print(panel)
    if workspace is not None:
        console.print(f"  [grey70]cwd: {workspace}[/grey70]\n")
