"""Unit/integration tests for CLI subcommands via typer CliRunner.

All invocations use --no-autoload (global flag, before subcommand name) to
isolate tests from the real ./pipelines directory. Assertions parse the JSON
output envelope rather than checking plain text.
"""
from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest
from typer.testing import CliRunner

from pipeline_engine.cli import app

runner = CliRunner()

# Convenience: insert global flag before subcommand
_NA = "--no-autoload"


def _invoke(args: list[str]) -> "Result":
    """Invoke CLI with --no-autoload inserted as first arg (global flag)."""
    return runner.invoke(app, [_NA] + args)


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


# ── stub task used by CLI tests ──────────────────────────────────────────────
from pipeline_engine.core.base_task import BaseTask


class EchoTask(BaseTask):
    async def execute(self, inputs, progress):
        await progress(100)
        return {"ok": True}


def _j(result) -> dict:
    """Parse the JSON output of a CLI invocation."""
    return json.loads(result.output)


# ─── lint ─────────────────────────────────────────────────────────────────────

def test_lint_valid_pipeline(tmp_path):
    yaml_p = _make_yaml(tmp_path, "lint_ok")
    result = _invoke(["lint", str(yaml_p)])
    assert result.exit_code == 0
    payload = _j(result)
    assert payload["ok"] is True
    assert payload["command"] == "lint"
    assert payload["pipeline_id"] == "lint_ok"
    assert payload["valid"] is True


def test_lint_missing_file(tmp_path):
    result = _invoke(["lint", str(tmp_path / "no_such.yaml")])
    assert result.exit_code != 0
    payload = _j(result)
    assert payload["ok"] is False
    assert payload["command"] == "lint"


def test_lint_invalid_yaml(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("{not: [valid: yaml")
    result = _invoke(["lint", str(bad)])
    assert result.exit_code != 0
    payload = _j(result)
    assert payload["ok"] is False


# ─── load ─────────────────────────────────────────────────────────────────────

def test_load_single_pipeline(tmp_path):
    yaml_p = _make_yaml(tmp_path, "loadme")
    result = _invoke(["load", str(yaml_p), "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    payload = _j(result)
    assert payload["ok"] is True
    assert payload["command"] == "load"
    assert any(item["pipeline_id"] == "loadme" for item in payload["loaded"])


def test_load_multiple_pipelines(tmp_path):
    y1 = _make_yaml(tmp_path, "load_a")
    y2 = _make_yaml(tmp_path, "load_b")
    result = _invoke(["load", str(y1), str(y2), "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    payload = _j(result)
    assert payload["ok"] is True
    pids = [item["pipeline_id"] for item in payload["loaded"]]
    assert "load_a" in pids
    assert "load_b" in pids


# ─── list ─────────────────────────────────────────────────────────────────────

def test_list_after_load(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "listed_pipe")), "--workspace", str(tmp_path)])
    result = _invoke(["list", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    payload = _j(result)
    assert payload["ok"] is True
    assert payload["scope"] == "pipeline"
    assert any(p["pipeline_id"] == "listed_pipe" for p in payload["pipelines"])


def test_list_empty_workspace(tmp_path):
    result = _invoke(["list", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    payload = _j(result)
    assert payload["ok"] is True
    assert payload["pipelines"] == []


# ─── start ────────────────────────────────────────────────────────────────────

def test_run_starts_pipeline(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "run_pipe")), "--workspace", str(tmp_path)])
    result = _invoke(["start", "run_pipe", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    payload = _j(result)
    assert payload["ok"] is True
    assert payload["command"] == "start"
    assert any(r["pipeline_id"] == "run_pipe" for r in payload["runs"])


def test_run_unknown_pipeline(tmp_path):
    result = _invoke(["start", "no_such_pipe", "--workspace", str(tmp_path)])
    assert result.exit_code != 0
    payload = _j(result)
    assert payload["ok"] is False
    assert payload["command"] == "start"


def test_run_with_wait_shows_status(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "waiter_pipe")), "--workspace", str(tmp_path)])
    result = _invoke(["start", "waiter_pipe", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    payload = _j(result)
    assert payload["ok"] is True
    run = payload["runs"][0]
    assert run["run_id"] is not None
    assert "final_status" in run


# ─── status & inspect ─────────────────────────────────────────────────────────

def test_status_unknown_run(tmp_path):
    result = _invoke(["status", "bad_ref", "--workspace", str(tmp_path)])
    assert result.exit_code != 0
    payload = _j(result)
    assert payload["ok"] is False


def test_inspect_unknown_run(tmp_path):
    result = _invoke(["inspect", "bad_ref", "--workspace", str(tmp_path)])
    assert result.exit_code != 0
    payload = _j(result)
    assert payload["ok"] is False


# ─── stop ─────────────────────────────────────────────────────────────────────

def test_stop_unknown_run(tmp_path):
    result = _invoke(["stop", "no_such_run", "--workspace", str(tmp_path)])
    assert result.exit_code != 0
    payload = _j(result)
    assert payload["ok"] is False


def test_stop_known_run(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "stop_pipe")), "--workspace", str(tmp_path)])
    run_result = _invoke(["start", "stop_pipe", "--workspace", str(tmp_path)])
    run_id = _j(run_result)["runs"][0]["run_id"]
    result = _invoke(["stop", run_id, "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    payload = _j(result)
    assert payload["ok"] is True
    assert payload["stopped"] == run_id


# ─── resume ───────────────────────────────────────────────────────────────────

def test_resume_unknown_run(tmp_path):
    result = _invoke(["resume", "no_such_run", "--workspace", str(tmp_path)])
    assert result.exit_code != 0
    payload = _j(result)
    assert payload["ok"] is False


def test_resume_completed_run(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "res_pipe")), "--workspace", str(tmp_path)])
    run_result = _invoke(["start", "res_pipe", "--workspace", str(tmp_path)])
    run_id = _j(run_result)["runs"][0]["run_id"]
    result = _invoke(["resume", run_id, "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    payload = _j(result)
    assert payload["ok"] is True
    assert payload["resumed"] == run_id
    assert "final_status" in payload


# ─── fix ──────────────────────────────────────────────────────────────────────

def test_fix_unknown_run(tmp_path):
    result = _invoke([
        "fix", "no_such", "--task", "s/t",
        "--output", str(tmp_path / "x.json"),
        "--workspace", str(tmp_path),
    ])
    assert result.exit_code != 0
    payload = _j(result)
    assert payload["ok"] is False


def test_fix_output_on_completed_task(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "fix_pipe")), "--workspace", str(tmp_path)])
    run_result = _invoke(["start", "fix_pipe", "--workspace", str(tmp_path)])
    run_id = _j(run_result)["runs"][0]["run_id"]

    out_file = tmp_path / "fixed.json"
    out_file.write_text(json.dumps({"ok": True}))
    result = _invoke([
        "fix", run_id, "--task", "step_a/t1",
        "--output", str(out_file),
        "--workspace", str(tmp_path),
    ])
    assert result.exit_code == 0
    payload = _j(result)
    assert payload["ok"] is True
    assert payload["mode"] == "output"
    assert payload["new_status"] == "fixed"


# ─── start / run rename ───────────────────────────────────────────────────────

def test_start_renamed_from_run(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "start_pipe")), "--workspace", str(tmp_path)])
    result = _invoke(["start", "start_pipe", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    payload = _j(result)
    assert payload["ok"] is True
    assert any(r["pipeline_id"] == "start_pipe" for r in payload["runs"])


def test_run_command_no_longer_exists(tmp_path):
    result = _invoke(["run", "any_pipe", "--workspace", str(tmp_path)])
    assert result.exit_code != 0


# ─── list --pipeline / --instance ─────────────────────────────────────────────

def test_list_pipeline_shows_type(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "type_pipe")), "--workspace", str(tmp_path)])
    result = _invoke(["list", "--pipeline", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    payload = _j(result)
    assert payload["ok"] is True
    pipeline = next(p for p in payload["pipelines"] if p["pipeline_id"] == "type_pipe")
    assert pipeline["type"] == "测试"


def test_list_instance_shows_status(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "inst_pipe")), "--workspace", str(tmp_path)])
    _invoke(["start", "inst_pipe", "--workspace", str(tmp_path)])
    result = _invoke(["list", "--instance", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    payload = _j(result)
    assert payload["ok"] is True
    assert payload["scope"] == "instance"
    assert any(inst["pipeline_id"] == "inst_pipe" for inst in payload["instances"])
    assert any(inst["status"] == "success" for inst in payload["instances"])


# ─── max_parallelism validation ───────────────────────────────────────────────

def test_lint_invalid_max_parallelism(tmp_path):
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
    result = _invoke(["lint", str(bad)])
    assert result.exit_code != 0
    payload = _j(result)
    assert payload["ok"] is False
