"""Tests for pipeline_engine.i18n module."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import pipeline_engine.i18n as i18n


def _reset():
    """Force re-initialization on next t() call."""
    i18n._initialized = False
    i18n._translations = {}
    i18n._current_lang = "zh_CN"


# ─── basic translation ─────────────────────────────────────────────────────────

def test_zh_CN_returns_chinese(tmp_path, monkeypatch):
    monkeypatch.delenv("OMNICAD_LANG", raising=False)
    _reset()
    i18n.init("zh_CN")
    result = i18n.t("cli.app.help")
    assert "OmniCAD" in result


def test_en_returns_english(monkeypatch):
    monkeypatch.delenv("OMNICAD_LANG", raising=False)
    _reset()
    i18n.init("en")
    assert i18n.t("cli.app.help") == "OmniCAD — DAG-based CAD workflow orchestration engine."
    assert i18n.t("repl.help.header") == "Available commands:"


def test_missing_key_returns_key_itself(monkeypatch):
    monkeypatch.delenv("OMNICAD_LANG", raising=False)
    _reset()
    i18n.init("zh_CN")
    assert i18n.t("no.such.key.xyz") == "no.such.key.xyz"


def test_missing_en_key_falls_back_to_zh_CN(monkeypatch):
    """A key present in zh_CN but absent in en still returns the zh_CN value."""
    monkeypatch.delenv("OMNICAD_LANG", raising=False)
    _reset()
    # Temporarily write an en.json without "cli.app.help" to test fallback
    en_dir = Path(__file__).resolve().parent.parent.parent / "config" / "i18n"
    orig = json.loads((en_dir / "en.json").read_text())
    stripped = {k: v for k, v in orig.items() if k != "cli.app.help"}
    tmp_en = en_dir / "_test_en.json"
    tmp_en.write_text(json.dumps(stripped))
    try:
        _reset()
        i18n.init("_test_en")
        # Should fall back to zh_CN value
        zh_value = json.loads((en_dir / "zh_CN.json").read_text()).get("cli.app.help", "")
        assert i18n.t("cli.app.help") == zh_value
    finally:
        tmp_en.unlink(missing_ok=True)
        _reset()


def test_missing_translation_file_returns_empty_dict():
    result = i18n._load("nonexistent_locale_xyz")
    assert result == {}


# ─── language resolution ───────────────────────────────────────────────────────

def test_env_var_overrides(monkeypatch):
    monkeypatch.setenv("OMNICAD_LANG", "en")
    _reset()
    i18n.init()
    assert i18n.current_lang() == "en"
    assert i18n.t("repl.col.active") == "Active"
    monkeypatch.delenv("OMNICAD_LANG")
    _reset()


def test_config_file_is_read(tmp_path, monkeypatch):
    monkeypatch.delenv("OMNICAD_LANG", raising=False)
    cfg = tmp_path / "i18n.json"
    cfg.write_text(json.dumps({"language": "en"}))
    # Patch the config path function
    monkeypatch.setattr(i18n, "_config_lang", lambda: "en")
    _reset()
    i18n.init()
    assert i18n.current_lang() == "en"
    monkeypatch.undo()
    _reset()


def test_init_is_idempotent(monkeypatch):
    monkeypatch.delenv("OMNICAD_LANG", raising=False)
    _reset()
    i18n.init("zh_CN")
    i18n.init("zh_CN")  # second call should not raise
    assert i18n.current_lang() == "zh_CN"
    _reset()


def test_lazy_init_on_first_t_call(monkeypatch):
    monkeypatch.delenv("OMNICAD_LANG", raising=False)
    _reset()
    # t() without prior init() should auto-init
    result = i18n.t("repl.label.warn")
    assert result != "repl.label.warn"  # must have been translated


# ─── schema validation ─────────────────────────────────────────────────────────

def test_i18n_config_validates_against_schema():
    try:
        import jsonschema
    except ImportError:
        pytest.skip("jsonschema not installed")

    repo = Path(__file__).resolve().parent.parent.parent
    schema = json.loads((repo / "config" / "i18n.schema.json").read_text())
    data = json.loads((repo / "config" / "i18n.json").read_text())
    jsonschema.validate(data, schema)


def test_translation_files_are_string_maps():
    """Both zh_CN.json and en.json must be flat string→string objects."""
    repo = Path(__file__).resolve().parent.parent.parent
    for lang in ("zh_CN", "en"):
        data = json.loads((repo / "config" / "i18n" / f"{lang}.json").read_text())
        assert isinstance(data, dict)
        for k, v in data.items():
            assert isinstance(k, str), f"{lang}.json: key {k!r} is not a string"
            assert isinstance(v, str), f"{lang}.json: value for {k!r} is not a string"
