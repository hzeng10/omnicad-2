"""Tests for CLI JSON output envelope: shape, ok/error fields, per-command payload."""
from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest
from typer.testing import CliRunner

from pipeline_engine.cli import app

runner = CliRunner()

_NA = "--no-autoload"


def _invoke(args: list[str]):
    return runner.invoke(app, [_NA] + args)


def _j(result) -> dict:
    return json.loads(result.output)


def _make_yaml(tmp_path: Path, pid: str) -> Path:
    plugin = "tests.unit.test_cli_json_output.QuickTask"
    p = tmp_path / f"{pid}.yaml"
    p.write_text(textwrap.dedent(f"""\
        version: "1.0"
        pipeline:
          id: {pid}
          name: "{pid} test"
          type: "test"
        steps:
          - id: s1
            tasks:
              - id: t1
                plugin: {plugin}
    """))
    return p


from pipeline_engine.core.base_task import BaseTask


class QuickTask(BaseTask):
    async def execute(self, inputs, progress):
        await progress(100)
        return {"result": "done"}


# ─── load ─────────────────────────────────────────────────────────────────────

def test_load_success_envelope(tmp_path):
    yaml_p = _make_yaml(tmp_path, "load_ok")
    result = _invoke(["load", str(yaml_p), "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    assert p["command"] == "load"
    assert isinstance(p["loaded"], list)
    assert p["loaded"][0]["pipeline_id"] == "load_ok"


def test_load_missing_file_error_envelope(tmp_path):
    result = _invoke(["load", str(tmp_path / "nope.yaml"), "--workspace", str(tmp_path)])
    assert result.exit_code != 0
    p = _j(result)
    assert p["ok"] is False
    assert p["command"] == "load"
    assert "error" in p or "loaded" in p  # partial load envelope includes loaded list


# ─── lint ─────────────────────────────────────────────────────────────────────

def test_lint_success_envelope(tmp_path):
    yaml_p = _make_yaml(tmp_path, "lint_pipe")
    result = _invoke(["lint", str(yaml_p)])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    assert p["command"] == "lint"
    assert p["pipeline_id"] == "lint_pipe"
    assert p["valid"] is True
    assert "path" in p


def test_lint_error_envelope(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("not: [valid: yaml")
    result = _invoke(["lint", str(bad)])
    assert result.exit_code != 0
    p = _j(result)
    assert p["ok"] is False
    assert p["command"] == "lint"
    assert "error" in p
    assert "message" in p["error"]


# ─── list ─────────────────────────────────────────────────────────────────────

def test_list_pipeline_empty(tmp_path):
    result = _invoke(["list", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    assert p["scope"] == "pipeline"
    assert p["pipelines"] == []


def test_list_pipeline_nonempty(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "lp")), "--workspace", str(tmp_path)])
    result = _invoke(["list", "--workspace", str(tmp_path)])
    p = _j(result)
    assert p["ok"] is True
    entry = p["pipelines"][0]
    assert "pipeline_id" in entry and "type" in entry and "name" in entry


def test_list_instance_empty(tmp_path):
    result = _invoke(["list", "--instance", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    assert p["scope"] == "instance"
    assert p["instances"] == []


def test_list_instance_nonempty(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "ip")), "--workspace", str(tmp_path)])
    _invoke(["start", "ip", "--workspace", str(tmp_path), "--wait"])
    result = _invoke(["list", "--instance", "--workspace", str(tmp_path)])
    p = _j(result)
    assert p["ok"] is True
    entry = p["instances"][0]
    assert "pipeline_id" in entry and "instance_id" in entry and "status" in entry


# ─── start ────────────────────────────────────────────────────────────────────

def test_start_wait_has_final_status(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "sp")), "--workspace", str(tmp_path)])
    result = _invoke(["start", "sp", "--workspace", str(tmp_path), "--wait"])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    run = p["runs"][0]
    assert "pipeline_id" in run and "run_id" in run and run["ok"] is True
    assert "final_status" in run


def test_start_unknown_pipeline_error_envelope(tmp_path):
    result = _invoke(["start", "ghost", "--workspace", str(tmp_path)])
    assert result.exit_code != 0
    p = _j(result)
    assert p["ok"] is False
    assert p["command"] == "start"
    assert "error" in p


# ─── stop ─────────────────────────────────────────────────────────────────────

def test_stop_success_envelope(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "stop_j")), "--workspace", str(tmp_path)])
    run_r = _invoke(["start", "stop_j", "--workspace", str(tmp_path), "--wait"])
    run_id = _j(run_r)["runs"][0]["run_id"]
    result = _invoke(["stop", run_id, "--workspace", str(tmp_path)])
    p = _j(result)
    assert p["ok"] is True
    assert p["command"] == "stop"
    assert p["stopped"] == run_id


def test_stop_unknown_error_envelope(tmp_path):
    result = _invoke(["stop", "ghost_id", "--workspace", str(tmp_path)])
    assert result.exit_code != 0
    p = _j(result)
    assert p["ok"] is False


# ─── resume ───────────────────────────────────────────────────────────────────

def test_resume_success_envelope(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "res_j")), "--workspace", str(tmp_path)])
    run_r = _invoke(["start", "res_j", "--workspace", str(tmp_path), "--wait"])
    run_id = _j(run_r)["runs"][0]["run_id"]
    result = _invoke(["resume", run_id, "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    assert p["resumed"] == run_id
    assert "final_status" in p


def test_resume_unknown_error_envelope(tmp_path):
    result = _invoke(["resume", "ghost_id", "--workspace", str(tmp_path)])
    assert result.exit_code != 0
    p = _j(result)
    assert p["ok"] is False


# ─── fix ──────────────────────────────────────────────────────────────────────

def test_fix_output_envelope(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "fix_j")), "--workspace", str(tmp_path)])
    run_r = _invoke(["start", "fix_j", "--workspace", str(tmp_path), "--wait"])
    run_id = _j(run_r)["runs"][0]["run_id"]
    out = tmp_path / "fix_out.json"
    out.write_text('{"result": "manual"}')
    result = _invoke([
        "fix", run_id, "--task", "s1/t1",
        "--output", str(out), "--workspace", str(tmp_path),
    ])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    assert p["mode"] == "output"
    assert p["new_status"] == "fixed"


def test_fix_input_envelope(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "fix_in_j")), "--workspace", str(tmp_path)])
    run_r = _invoke(["start", "fix_in_j", "--workspace", str(tmp_path), "--wait"])
    run_id = _j(run_r)["runs"][0]["run_id"]
    inp = tmp_path / "fix_inp.json"
    inp.write_text("{}")
    result = _invoke([
        "fix", run_id, "--task", "s1/t1",
        "--input", str(inp), "--workspace", str(tmp_path),
    ])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    assert p["mode"] == "input"
    assert p["new_status"] == "new"


# ─── status ───────────────────────────────────────────────────────────────────

def test_status_envelope_contains_state(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "stat_j")), "--workspace", str(tmp_path)])
    run_r = _invoke(["start", "stat_j", "--workspace", str(tmp_path), "--wait"])
    run_id = _j(run_r)["runs"][0]["run_id"]
    result = _invoke(["status", run_id, "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    assert p["command"] == "status"
    state = p["state"]
    assert state["run_id"] == run_id
    assert state["status"] == "success"
    assert "steps" in state


# ─── inspect ──────────────────────────────────────────────────────────────────

def test_inspect_no_step_returns_state(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "insp_j")), "--workspace", str(tmp_path)])
    run_r = _invoke(["start", "insp_j", "--workspace", str(tmp_path), "--wait"])
    run_id = _j(run_r)["runs"][0]["run_id"]
    result = _invoke(["inspect", run_id, "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    assert "state" in p  # falls back to status-like output


def test_inspect_step_returns_tasks(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "insp2_j")), "--workspace", str(tmp_path)])
    run_r = _invoke(["start", "insp2_j", "--workspace", str(tmp_path), "--wait"])
    run_id = _j(run_r)["runs"][0]["run_id"]
    result = _invoke([
        "inspect", run_id, "--step", "s1", "--workspace", str(tmp_path),
    ])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    assert "tasks" in p
    assert p["step_id"] == "s1"


def test_inspect_step_task_returns_task_detail(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "insp3_j")), "--workspace", str(tmp_path)])
    run_r = _invoke(["start", "insp3_j", "--workspace", str(tmp_path), "--wait"])
    run_id = _j(run_r)["runs"][0]["run_id"]
    result = _invoke([
        "inspect", run_id, "--step", "s1", "--task", "t1",
        "--workspace", str(tmp_path),
    ])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    task = p["task"]
    assert task["id"] == "t1"
    assert "status" in task
    assert "progress" in task
    assert "output" in task  # inline JSON or null


# ─── log ──────────────────────────────────────────────────────────────────────

def test_log_no_file_returns_empty_lines(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "log_j")), "--workspace", str(tmp_path)])
    run_r = _invoke(["start", "log_j", "--workspace", str(tmp_path), "--wait"])
    run_id = _j(run_r)["runs"][0]["run_id"]
    # Run has completed; run.log should exist from the run
    result = _invoke(["log", run_id, "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    assert p["command"] == "log"
    assert "lines" in p
    assert "total" in p
    assert "log_path" in p


def test_log_tail_limits(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "logtail_j")), "--workspace", str(tmp_path)])
    run_r = _invoke(["start", "logtail_j", "--workspace", str(tmp_path), "--wait"])
    run_id = _j(run_r)["runs"][0]["run_id"]
    result = _invoke(["log", run_id, "--tail", "2", "--workspace", str(tmp_path)])
    p = _j(result)
    assert p["ok"] is True
    assert len(p["lines"]) <= 2


def test_log_errors_only(tmp_path):
    _invoke(["load", str(_make_yaml(tmp_path, "logerr_j")), "--workspace", str(tmp_path)])
    run_r = _invoke(["start", "logerr_j", "--workspace", str(tmp_path), "--wait"])
    run_id = _j(run_r)["runs"][0]["run_id"]
    result = _invoke(["log", run_id, "--errors-only", "--workspace", str(tmp_path)])
    p = _j(result)
    assert p["ok"] is True
    for line in p["lines"]:
        assert line.get("level") == "ERROR" or " ERROR " in line.get("raw", "")


def test_log_unknown_run(tmp_path):
    result = _invoke(["log", "ghost_run", "--workspace", str(tmp_path)])
    assert result.exit_code != 0
    p = _j(result)
    assert p["ok"] is False
