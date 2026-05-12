"""Unit/integration tests for CLI subcommands via typer CliRunner."""
from __future__ import annotations

import asyncio
import json
import textwrap
from pathlib import Path

import pytest
from typer.testing import CliRunner

from pipeline_engine.cli import app

runner = CliRunner()


def _make_yaml(tmp_path: Path, pid: str, plugin: str = "tests.unit.test_cli.EchoTask") -> Path:
    content = textwrap.dedent(f"""\
        version: "1.0"
        pipeline:
          id: {pid}
          name: "CLI Test {pid}"
        steps:
          - id: step_a
            tasks:
              - id: t1
                plugin: {plugin}
    """)
    p = tmp_path / f"{pid}.yaml"
    p.write_text(content)
    return p


# -- stub task used by CLI tests --
from pipeline_engine.core.base_task import BaseTask

class EchoTask(BaseTask):
    async def execute(self, inputs, progress):
        await progress(100)
        return {"ok": True}


# ─── lint ─────────────────────────────────────────────────────────────────────

def test_lint_valid_pipeline(tmp_path):
    yaml_p = _make_yaml(tmp_path, "lint_ok")
    result = runner.invoke(app, ["lint", str(yaml_p)])
    assert result.exit_code == 0
    assert "lint_ok" in result.output


def test_lint_missing_file(tmp_path):
    result = runner.invoke(app, ["lint", str(tmp_path / "no_such.yaml")])
    assert result.exit_code != 0


def test_lint_invalid_yaml(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("{not: [valid: yaml")
    result = runner.invoke(app, ["lint", str(bad)])
    assert result.exit_code != 0


# ─── load ─────────────────────────────────────────────────────────────────────

def test_load_single_pipeline(tmp_path):
    yaml_p = _make_yaml(tmp_path, "loadme")
    result = runner.invoke(app, ["load", str(yaml_p), "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    assert "loadme" in result.output


def test_load_multiple_pipelines(tmp_path):
    y1 = _make_yaml(tmp_path, "load_a")
    y2 = _make_yaml(tmp_path, "load_b")
    result = runner.invoke(app, ["load", str(y1), str(y2), "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    assert "load_a" in result.output
    assert "load_b" in result.output


# ─── list ─────────────────────────────────────────────────────────────────────

def test_list_after_load(tmp_path):
    yaml_p = _make_yaml(tmp_path, "listed_pipe")
    runner.invoke(app, ["load", str(yaml_p), "--workspace", str(tmp_path)])
    result = runner.invoke(app, ["list", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    assert "listed_pipe" in result.output


def test_list_empty_workspace(tmp_path):
    result = runner.invoke(app, ["list", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    assert "No pipelines" in result.output


# ─── run ──────────────────────────────────────────────────────────────────────

def test_run_starts_pipeline(tmp_path):
    yaml_p = _make_yaml(tmp_path, "run_pipe")
    runner.invoke(app, ["load", str(yaml_p), "--workspace", str(tmp_path)])
    result = runner.invoke(app, ["run", "run_pipe", "--workspace", str(tmp_path), "--wait"])
    assert result.exit_code == 0
    assert "run_pipe" in result.output


def test_run_unknown_pipeline(tmp_path):
    result = runner.invoke(app, ["run", "no_such_pipe", "--workspace", str(tmp_path)])
    assert result.exit_code != 0


def test_run_with_wait_shows_status(tmp_path):
    yaml_p = _make_yaml(tmp_path, "waiter_pipe")
    runner.invoke(app, ["load", str(yaml_p), "--workspace", str(tmp_path)])
    result = runner.invoke(app, ["run", "waiter_pipe", "--workspace", str(tmp_path), "--wait"])
    assert result.exit_code == 0
    # Should print run_id and final status
    assert "Started:" in result.output


# ─── status & inspect ─────────────────────────────────────────────────────────

def test_status_unknown_run(tmp_path):
    result = runner.invoke(app, ["status", "bad_ref", "--workspace", str(tmp_path)])
    assert result.exit_code != 0


def test_inspect_unknown_run(tmp_path):
    result = runner.invoke(app, ["inspect", "bad_ref", "--workspace", str(tmp_path)])
    assert result.exit_code != 0
