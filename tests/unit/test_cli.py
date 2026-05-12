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
          type: "测试"
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
    result = runner.invoke(app, ["start", "run_pipe", "--workspace", str(tmp_path), "--wait"])
    assert result.exit_code == 0
    assert "run_pipe" in result.output


def test_run_unknown_pipeline(tmp_path):
    result = runner.invoke(app, ["start", "no_such_pipe", "--workspace", str(tmp_path)])
    assert result.exit_code != 0


def test_run_with_wait_shows_status(tmp_path):
    yaml_p = _make_yaml(tmp_path, "waiter_pipe")
    runner.invoke(app, ["load", str(yaml_p), "--workspace", str(tmp_path)])
    result = runner.invoke(app, ["start", "waiter_pipe", "--workspace", str(tmp_path), "--wait"])
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


# ─── stop ─────────────────────────────────────────────────────────────────────

def test_stop_unknown_run(tmp_path):
    result = runner.invoke(app, ["stop", "no_such_run", "--workspace", str(tmp_path)])
    assert result.exit_code != 0


def test_stop_known_run(tmp_path):
    yaml_p = _make_yaml(tmp_path, "stop_pipe")
    runner.invoke(app, ["load", str(yaml_p), "--workspace", str(tmp_path)])
    # Run with --wait so the run_id is in the registry on disk
    run_result = runner.invoke(app, ["start", "stop_pipe", "--workspace", str(tmp_path), "--wait"])
    assert run_result.exit_code == 0
    # Extract run_id from "Started: <run_id> ..."
    run_id = run_result.output.split("Started: ")[1].split()[0]
    result = runner.invoke(app, ["stop", run_id, "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    assert "Stopped" in result.output


# ─── resume ───────────────────────────────────────────────────────────────────

def test_resume_unknown_run(tmp_path):
    result = runner.invoke(app, ["resume", "no_such_run", "--workspace", str(tmp_path)])
    assert result.exit_code != 0


def test_resume_completed_run(tmp_path):
    yaml_p = _make_yaml(tmp_path, "res_pipe")
    runner.invoke(app, ["load", str(yaml_p), "--workspace", str(tmp_path)])
    run_result = runner.invoke(app, ["start", "res_pipe", "--workspace", str(tmp_path), "--wait"])
    run_id = run_result.output.split("Started: ")[1].split()[0]
    result = runner.invoke(app, ["resume", run_id, "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    assert "Resumed" in result.output


# ─── fix ──────────────────────────────────────────────────────────────────────

def test_fix_unknown_run(tmp_path):
    result = runner.invoke(app, [
        "fix", "no_such", "--task", "s/t",
        "--output", str(tmp_path / "x.json"),
        "--workspace", str(tmp_path),
    ])
    assert result.exit_code != 0


def test_fix_output_on_completed_task(tmp_path):
    import json as _json
    yaml_p = _make_yaml(tmp_path, "fix_pipe")
    runner.invoke(app, ["load", str(yaml_p), "--workspace", str(tmp_path)])
    run_result = runner.invoke(app, ["start", "fix_pipe", "--workspace", str(tmp_path), "--wait"])
    run_id = run_result.output.split("Started: ")[1].split()[0]

    out_file = tmp_path / "fixed.json"
    out_file.write_text(_json.dumps({"ok": True}))
    result = runner.invoke(app, [
        "fix", run_id, "--task", "step_a/t1",
        "--output", str(out_file),
        "--workspace", str(tmp_path),
    ])
    assert result.exit_code == 0
    assert "FIXED" in result.output


# ─── start / run rename ───────────────────────────────────────────────────────

def test_start_renamed_from_run(tmp_path):
    yaml_p = _make_yaml(tmp_path, "start_pipe")
    runner.invoke(app, ["load", str(yaml_p), "--workspace", str(tmp_path)])
    result = runner.invoke(app, ["start", "start_pipe", "--workspace", str(tmp_path), "--wait"])
    assert result.exit_code == 0
    assert "start_pipe" in result.output


def test_run_command_no_longer_exists(tmp_path):
    result = runner.invoke(app, ["run", "any_pipe", "--workspace", str(tmp_path)])
    assert result.exit_code != 0


# ─── list --pipeline / --instance ─────────────────────────────────────────────

def test_list_pipeline_shows_type(tmp_path):
    yaml_p = _make_yaml(tmp_path, "type_pipe")
    runner.invoke(app, ["load", str(yaml_p), "--workspace", str(tmp_path)])
    result = runner.invoke(app, ["list", "--pipeline", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    assert "type_pipe" in result.output
    assert "测试" in result.output


def test_list_instance_shows_status(tmp_path):
    yaml_p = _make_yaml(tmp_path, "inst_pipe")
    runner.invoke(app, ["load", str(yaml_p), "--workspace", str(tmp_path)])
    runner.invoke(app, ["start", "inst_pipe", "--workspace", str(tmp_path), "--wait"])
    result = runner.invoke(app, ["list", "--instance", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    assert "inst_pipe" in result.output
    assert "success" in result.output


# ─── max_parallelism validation ───────────────────────────────────────────────

def test_lint_invalid_max_parallelism(tmp_path):
    import textwrap
    bad = tmp_path / "bad_para.yaml"
    bad.write_text(textwrap.dedent("""\
        version: "1.0"
        pipeline:
          id: bad_pipe
          name: "Bad"
          type: "测试"
          max_parallelism: 0
        steps:
          - id: s1
            tasks:
              - id: t1
                plugin: some.Task
    """))
    result = runner.invoke(app, ["lint", str(bad)])
    assert result.exit_code != 0
