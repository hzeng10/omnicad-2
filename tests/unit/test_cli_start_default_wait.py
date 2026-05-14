"""Tests for 'start' command default wait behavior.

'start' defaults to --wait (blocks until run completes).
'start --no-wait' returns immediately with a warning field.
"""
from __future__ import annotations

import json
import re
import textwrap
from pathlib import Path

import pytest
from typer.testing import CliRunner

from pipeline_engine.cli import app

runner = CliRunner()
_NA = "--no-autoload"


def _j(result) -> dict:
    m = re.search(r"^\{", result.output, re.MULTILINE)
    if m:
        return json.loads(result.output[m.start():])
    raise ValueError(f"No JSON in output:\n{result.output!r}")


def _make_yaml(base: Path, pid: str) -> Path:
    plugin = "tests.unit.test_cli_start_default_wait.QuickTask"
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
        return {"result": "done"}


# ─── tests ────────────────────────────────────────────────────────────────────

def test_start_default_blocks_and_returns_final_status(tmp_path):
    """Default 'start' (no --wait flag) should block and include final_status."""
    yaml_p = _make_yaml(tmp_path, "sw_default")
    runner.invoke(app, [_NA, "load", str(yaml_p), "--workspace", str(tmp_path)])

    result = runner.invoke(app, [_NA, "start", "sw_default", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    run = p["runs"][0]
    assert "final_status" in run, "default start should wait and return final_status"
    assert run["final_status"] == "success"


def test_start_explicit_wait_returns_final_status(tmp_path):
    """Explicit --wait should also block and return final_status."""
    yaml_p = _make_yaml(tmp_path, "sw_explicit")
    runner.invoke(app, [_NA, "load", str(yaml_p), "--workspace", str(tmp_path)])

    result = runner.invoke(app, [_NA, "start", "sw_explicit", "--wait", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    assert "final_status" in p["runs"][0]


def test_start_no_wait_returns_warning(tmp_path):
    """--no-wait should return immediately with a 'warning' field."""
    yaml_p = _make_yaml(tmp_path, "sw_nowait")
    runner.invoke(app, [_NA, "load", str(yaml_p), "--workspace", str(tmp_path)])

    result = runner.invoke(app, [_NA, "start", "sw_nowait", "--no-wait", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    assert "warning" in p, "--no-wait response should contain a 'warning' field"
    assert "cancelled" in p["warning"].lower() or "cancel" in p["warning"].lower()


def test_start_no_wait_does_not_have_final_status(tmp_path):
    """--no-wait entries should NOT contain final_status."""
    yaml_p = _make_yaml(tmp_path, "sw_nowait2")
    runner.invoke(app, [_NA, "load", str(yaml_p), "--workspace", str(tmp_path)])

    result = runner.invoke(app, [_NA, "start", "sw_nowait2", "--no-wait", "--workspace", str(tmp_path)])
    p = _j(result)
    assert p["ok"] is True
    run = p["runs"][0]
    assert "final_status" not in run, "--no-wait run entry should not have final_status"
