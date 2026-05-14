"""Integration tests: status / inspect / log do NOT modify state.json on disk.

Root cause fixed: restore_runs_from_disk(write_back=False) skips demote_orphans_sync,
so querying a live run from a separate CLI process no longer corrupts state.json.
"""
from __future__ import annotations

import json
import os
import textwrap
import time
from pathlib import Path

import pytest
from typer.testing import CliRunner

from pipeline_engine.cli import app

runner = CliRunner()
_NA = "--no-autoload"


def _j(result) -> dict:
    import re
    m = re.search(r"^\{", result.output, re.MULTILINE)
    if m:
        return json.loads(result.output[m.start():])
    raise ValueError(f"No JSON in output:\n{result.output!r}")


def _make_yaml(base: Path, pid: str) -> Path:
    plugin = "tests.integration.test_status_no_writeback.QuickTask"
    p = base / f"{pid}.yaml"
    p.write_text(textwrap.dedent(f"""\
        version: "1.0"
        pipeline:
          id: {pid}
          name: "{pid}"
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
        return {"result": "ok"}


# ─── tests ────────────────────────────────────────────────────────────────────

def test_status_does_not_modify_state_json(tmp_path):
    """Calling 'status' must not alter state.json (mtime and content unchanged)."""
    yaml_p = _make_yaml(tmp_path, "nowb_status")
    runner.invoke(app, [_NA, "load", str(yaml_p), "--workspace", str(tmp_path)])
    run_r = runner.invoke(app, [_NA, "start", "nowb_status", "--workspace", str(tmp_path)])
    run_id = _j(run_r)["runs"][0]["run_id"]

    # Locate state.json
    state_files = list(tmp_path.glob(f"**/{run_id}/state.json"))
    assert state_files, "state.json not found"
    state_path = state_files[0]

    before_mtime = state_path.stat().st_mtime
    before_content = state_path.read_bytes()

    runner.invoke(app, [_NA, "status", run_id, "--workspace", str(tmp_path)])

    assert state_path.read_bytes() == before_content, "state.json content was modified by status"
    assert state_path.stat().st_mtime == before_mtime, "state.json mtime was modified by status"


def test_inspect_does_not_modify_state_json(tmp_path):
    """Calling 'inspect' must not alter state.json."""
    yaml_p = _make_yaml(tmp_path, "nowb_inspect")
    runner.invoke(app, [_NA, "load", str(yaml_p), "--workspace", str(tmp_path)])
    run_r = runner.invoke(app, [_NA, "start", "nowb_inspect", "--workspace", str(tmp_path)])
    run_id = _j(run_r)["runs"][0]["run_id"]

    state_files = list(tmp_path.glob(f"**/{run_id}/state.json"))
    assert state_files
    state_path = state_files[0]

    before_content = state_path.read_bytes()

    runner.invoke(app, [_NA, "inspect", run_id, "--workspace", str(tmp_path)])

    assert state_path.read_bytes() == before_content, "state.json content was modified by inspect"


def test_log_does_not_modify_state_json(tmp_path):
    """Calling 'log' must not alter state.json."""
    yaml_p = _make_yaml(tmp_path, "nowb_log")
    runner.invoke(app, [_NA, "load", str(yaml_p), "--workspace", str(tmp_path)])
    run_r = runner.invoke(app, [_NA, "start", "nowb_log", "--workspace", str(tmp_path)])
    run_id = _j(run_r)["runs"][0]["run_id"]

    state_files = list(tmp_path.glob(f"**/{run_id}/state.json"))
    assert state_files
    state_path = state_files[0]

    before_content = state_path.read_bytes()

    runner.invoke(app, [_NA, "log", run_id, "--workspace", str(tmp_path)])

    assert state_path.read_bytes() == before_content, "state.json content was modified by log"


def test_status_reflects_actual_state_not_demoted(tmp_path):
    """status must report the real state (success), not a demoted one."""
    yaml_p = _make_yaml(tmp_path, "nowb_real")
    runner.invoke(app, [_NA, "load", str(yaml_p), "--workspace", str(tmp_path)])
    run_r = runner.invoke(app, [_NA, "start", "nowb_real", "--workspace", str(tmp_path)])
    run_id = _j(run_r)["runs"][0]["run_id"]

    result = runner.invoke(app, [_NA, "status", run_id, "--workspace", str(tmp_path)])
    p = _j(result)
    assert p["ok"] is True
    assert p["state"]["status"] == "success"
