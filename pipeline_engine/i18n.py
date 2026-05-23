"""Minimal i18n engine for OmniCAD CLI.

Language resolution order (highest priority first):
  1. Explicit lang argument passed to init()
  2. OMNICAD_LANG environment variable
  3. config/i18n.json  {"language": "..."}
  4. System locale auto-detect
  5. Hard-coded fallback: zh_CN

Extension point: drop a new config/i18n/<locale>.json file — no code change needed.
"""
from __future__ import annotations

import json
import locale
import os
from pathlib import Path
from typing import Optional

_translations: dict[str, str] = {}
_current_lang: str = "zh_CN"
_initialized: bool = False
_FALLBACK = "zh_CN"


def init(lang: Optional[str] = None) -> None:
    """Load translations for the resolved language. Safe to call multiple times."""
    global _translations, _current_lang, _initialized
    _current_lang = (
        lang
        or os.environ.get("OMNICAD_LANG")
        or _config_lang()
        or _system_lang()
        or _FALLBACK
    )
    _translations = _load(_current_lang)
    if _current_lang != _FALLBACK:
        for k, v in _load(_FALLBACK).items():
            _translations.setdefault(k, v)
    _initialized = True


def t(key: str) -> str:
    """Return the translation for key; returns key itself if not found."""
    if not _initialized:
        init()
    return _translations.get(key, key)


def current_lang() -> str:
    """Return the currently active language code."""
    return _current_lang


def _i18n_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "config" / "i18n"


def _load(lang: str) -> dict[str, str]:
    p = _i18n_dir() / f"{lang}.json"
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def _config_lang() -> Optional[str]:
    cfg = Path(__file__).resolve().parent.parent / "config" / "i18n.json"
    if not cfg.exists():
        return None
    try:
        return json.loads(cfg.read_text(encoding="utf-8")).get("language")
    except Exception:
        return None


def _system_lang() -> Optional[str]:
    try:
        loc = locale.getdefaultlocale()[0] or ""
    except Exception:
        return None
    if loc.startswith("zh"):
        return "zh_CN"
    if loc.startswith("en"):
        return "en"
    return None
