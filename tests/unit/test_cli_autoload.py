"""Tests for CLI autoload behavior: discover ./pipelines/*/pipeline.yaml on startup."""
from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest
from typer.testing import CliRunner

from pipeline_engine.cli import app

runner = CliRunner()


def _j(result) -> dict:
    """Parse the JSON envelope from result.output.

    Warnings printed to stderr may be mixed into the output; extract only the
    line that looks like a JSON object (starts with '{').
    """
    for line in result.output.splitlines():
        stripped = line.strip()
        if stripped.startswith("{"):
            return json.loads(stripped)
    raise ValueError(f"No JSON line found in output:\n{result.output!r}")


def _make_pipeline_yaml(base_dir: Path, pid: str) -> Path:
    """Create <base_dir>/<pid>/pipeline.yaml."""
    plugin = "tests.unit.test_cli_autoload.PassTask"
    pdir = base_dir / pid
    pdir.mkdir(parents=True, exist_ok=True)
    yaml_p = pdir / "pipeline.yaml"
    yaml_p.write_text(textwrap.dedent(f"""\
        version: "1.0"
        pipeline:
          id: {pid}
          name: "Autoload Test {pid}"
          type: "test"
        steps:
          - id: step_a
            tasks:
              - id: t1
                plugin: {plugin}
    """))
    return yaml_p


# ── stub task ─────────────────────────────────────────────────────────────────
from pipeline_engine.core.base_task import BaseTask


class PassTask(BaseTask):
    async def execute(self, inputs, progress):
        await progress(100)
        return {}


# ─── tests ────────────────────────────────────────────────────────────────────

def test_autoload_default_pipelines_dir(tmp_path, monkeypatch):
    """Default base_dir = cwd/pipelines; discovered pipelines appear in list."""
    monkeypatch.chdir(tmp_path)
    _make_pipeline_yaml(tmp_path / "pipelines", "auto_pipe_a")

    result = runner.invoke(app, ["list", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    payload = _j(result)
    assert payload["ok"] is True
    assert any(p["pipeline_id"] == "auto_pipe_a" for p in payload["pipelines"])


def test_autoload_custom_pipelines_dir(tmp_path):
    """--pipelines-dir points to a non-default location."""
    custom = tmp_path / "custom_pl"
    _make_pipeline_yaml(custom, "custom_pipe")

    result = runner.invoke(app, [
        "--pipelines-dir", str(custom),
        "list", "--workspace", str(tmp_path),
    ])
    assert result.exit_code == 0
    payload = _j(result)
    assert any(p["pipeline_id"] == "custom_pipe" for p in payload["pipelines"])


def test_autoload_env_var(tmp_path, monkeypatch):
    """PIPELINE_AUTOLOAD_DIR env var is equivalent to --pipelines-dir."""
    custom = tmp_path / "env_pl"
    _make_pipeline_yaml(custom, "env_pipe")
    monkeypatch.setenv("PIPELINE_AUTOLOAD_DIR", str(custom))

    result = runner.invoke(app, ["list", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    payload = _j(result)
    assert any(p["pipeline_id"] == "env_pipe" for p in payload["pipelines"])


def test_no_autoload_flag_disables_discovery(tmp_path, monkeypatch):
    """--no-autoload prevents discovery even when ./pipelines exists."""
    monkeypatch.chdir(tmp_path)
    _make_pipeline_yaml(tmp_path / "pipelines", "should_not_appear")

    result = runner.invoke(app, ["--no-autoload", "list", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    payload = _j(result)
    assert not any(p["pipeline_id"] == "should_not_appear" for p in payload["pipelines"])


def test_autoload_bad_yaml_skipped_good_loaded(tmp_path, monkeypatch):
    """A bad YAML does not block the good one; exit code still 0 for list."""
    monkeypatch.chdir(tmp_path)
    pl_dir = tmp_path / "pipelines"

    # Valid pipeline
    _make_pipeline_yaml(pl_dir, "good_pipe")

    # Invalid YAML
    bad_dir = pl_dir / "bad_pipe"
    bad_dir.mkdir(parents=True)
    (bad_dir / "pipeline.yaml").write_text("{not: [valid: yaml")

    result = runner.invoke(app, ["list", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    # _j extracts the JSON line even if a warning line was mixed in
    payload = _j(result)
    pids = [p["pipeline_id"] for p in payload["pipelines"]]
    assert "good_pipe" in pids
    assert "bad_pipe" not in pids


def test_autoload_nonexistent_dir_does_not_error(tmp_path, monkeypatch):
    """If ./pipelines doesn't exist, autoload is silently skipped."""
    monkeypatch.chdir(tmp_path)
    # no pipelines dir created
    result = runner.invoke(app, ["list", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    payload = _j(result)
    assert payload["ok"] is True
    assert payload["pipelines"] == []
