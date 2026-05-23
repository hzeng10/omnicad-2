"""Tests for pipeline_engine.branding module."""
from __future__ import annotations

import json
from importlib import metadata
from pathlib import Path

import pytest
from pydantic import ValidationError
from rich.console import Console

from pipeline_engine.branding import (
    BrandingConfig,
    _BUNDLED_DEFAULT,
    load_branding,
    print_banner,
)


def test_load_branding_missing_file(tmp_path):
    cfg = load_branding(tmp_path / "nonexistent.json")
    assert cfg.name == _BUNDLED_DEFAULT.name
    assert cfg.display_name == _BUNDLED_DEFAULT.display_name


def test_load_branding_valid_file(tmp_path):
    data = {
        "name": "testapp",
        "display_name": "TestApp",
        "prompt": "TestApp",
        "version": "1.2.3",
        "description": "A test app",
    }
    p = tmp_path / "branding.json"
    p.write_text(json.dumps(data))
    cfg = load_branding(p)
    assert cfg.name == "testapp"
    assert cfg.version == "1.2.3"


def test_branding_config_missing_required_field():
    with pytest.raises(ValidationError):
        BrandingConfig.model_validate({
            "name": "x",
            "display_name": "X",
            # missing: prompt, version, description
        })


def test_resolved_version_literal():
    cfg = BrandingConfig(
        name="x", display_name="X", prompt="X",
        version="2.5.0", description="test",
    )
    assert cfg.resolved_version == "2.5.0"


def test_resolved_version_auto():
    cfg = BrandingConfig(
        name="x", display_name="X", prompt="X",
        version="@auto", description="test",
    )
    try:
        expected = metadata.version("pipeline-engine")
    except metadata.PackageNotFoundError:
        expected = "0.0.0"
    assert cfg.resolved_version == expected


def test_print_banner_renders_display_name():
    cfg = BrandingConfig(
        name="testapp", display_name="MyApp", prompt="MyApp",
        version="9.9.9", description="Just testing", logo="",
    )
    console = Console(record=True, width=120)
    print_banner(console, cfg)
    text = console.export_text()
    assert "MyApp" in text
    assert "9.9.9" in text
    assert "Just testing" in text


def test_print_banner_box_style_rounded():
    cfg = BrandingConfig(
        name="x", display_name="X", prompt="X",
        version="1.0", description="d", logo="", box_style="ROUNDED",
    )
    console = Console(record=True, width=60)
    print_banner(console, cfg)
    text = console.export_text()
    assert "╭" in text


def test_print_banner_box_style_double():
    cfg = BrandingConfig(
        name="x", display_name="X", prompt="X",
        version="1.0", description="d", logo="", box_style="DOUBLE",
    )
    console = Console(record=True, width=60)
    print_banner(console, cfg)
    text = console.export_text()
    assert "╔" in text


def test_invalid_box_style_rejected():
    with pytest.raises(ValidationError):
        BrandingConfig.model_validate({
            "name": "x", "display_name": "X", "prompt": "X",
            "version": "1.0", "description": "d",
            "box_style": "INVALID",
        })


def test_cli_subcommand_no_banner_in_stdout(tmp_path):
    """One-shot subcommands produce clean JSON stdout — no banner text."""
    from typer.testing import CliRunner
    from pipeline_engine.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["--no-autoload", "list"])
    parsed = json.loads(result.stdout)
    assert isinstance(parsed, dict)
    assert "ok" in parsed
    assert "OmniCAD" not in result.stdout
    assert "╭" not in result.stdout


def test_branding_json_validates_against_schema():
    try:
        import jsonschema
    except ImportError:
        pytest.skip("jsonschema not installed")

    repo_root = Path(__file__).resolve().parent.parent.parent
    schema_path = repo_root / "config" / "branding.schema.json"
    data_path = repo_root / "config" / "branding.json"

    schema = json.loads(schema_path.read_text())
    data = json.loads(data_path.read_text())
    jsonschema.validate(data, schema)
