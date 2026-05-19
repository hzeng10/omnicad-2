"""Tests for 'start' command wait behavior.

'start' always blocks until run completes; --no-wait is not supported.
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

def test_start_blocks_and_returns_final_status(tmp_path):
    """'start' should block and include final_status in each run entry."""
    yaml_p = _make_yaml(tmp_path, "sw_default")
    runner.invoke(app, [_NA, "load", str(yaml_p), "--workspace", str(tmp_path)])

    result = runner.invoke(app, [_NA, "start", "sw_default", "--workspace", str(tmp_path)])
    assert result.exit_code == 0
    p = _j(result)
    assert p["ok"] is True
    run = p["runs"][0]
    assert "final_status" in run, "start should wait and return final_status"
    assert run["final_status"] == "success"


def test_start_no_wait_option_rejected(tmp_path):
    """--no-wait is not a valid option; typer should reject it with a non-zero exit."""
    yaml_p = _make_yaml(tmp_path, "sw_reject")
    runner.invoke(app, [_NA, "load", str(yaml_p), "--workspace", str(tmp_path)])

    result = runner.invoke(app, [_NA, "start", "sw_reject", "--no-wait", "--workspace", str(tmp_path)])
    assert result.exit_code != 0, "--no-wait should be rejected as an unknown option"
