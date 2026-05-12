"""Integration tests for REPL command dispatch."""
from __future__ import annotations

import asyncio
import textwrap
from pathlib import Path

import pytest

from pipeline_engine.core.base_task import BaseTask
from pipeline_engine.core.run_manager import RunManager
from pipeline_engine.repl import _dispatch


class QuickTask(BaseTask):
    async def execute(self, inputs, progress):
        await progress(100)
        return {"done": True}


class SlowTask(BaseTask):
    async def execute(self, inputs, progress):
        await asyncio.sleep(0.3)
        await progress(100)
        return {"done": True}


def _write_yaml(tmp_path: Path, pid: str, plugin: str) -> Path:
    content = textwrap.dedent(f"""\
        version: "1.0"
        pipeline:
          id: {pid}
          name: "Test {pid}"
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


# ─── tests ────────────────────────────────────────────────────────────────────

async def test_load_command(tmp_path):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp1", "tests.integration.test_repl_commands.QuickTask")
    await _dispatch(rm, f"load {yaml_p}")
    assert "rp1" in {p["pipeline_id"] for p in rm.list_pipelines()}


async def test_run_command_starts_run(tmp_path):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp2", "tests.integration.test_repl_commands.QuickTask")
    await rm.load(yaml_p)
    await _dispatch(rm, "start rp2")
    assert len(rm._runs) == 1


async def test_status_command_during_active_run(tmp_path, capsys):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp3", "tests.integration.test_repl_commands.SlowTask")
    await rm.load(yaml_p)
    run_id = await rm.start_run("rp3")

    # status should not raise while run is active
    await _dispatch(rm, f"status {run_id}")

    ctx = rm._runs[run_id]
    await ctx.main_task


async def test_stop_command_sets_abort(tmp_path):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp4", "tests.integration.test_repl_commands.SlowTask")
    await rm.load(yaml_p)
    run_id = await rm.start_run("rp4")
    await asyncio.sleep(0.05)

    await _dispatch(rm, f"stop {run_id}")
    ctx = rm._runs[run_id]
    await ctx.main_task
    assert ctx.abort_event.is_set()


async def test_help_command_does_not_raise(tmp_path):
    rm = RunManager(tmp_path)
    await _dispatch(rm, "help")


async def test_unknown_command_does_not_raise(tmp_path):
    rm = RunManager(tmp_path)
    await _dispatch(rm, "unknowncmd foo bar")


async def test_list_command(tmp_path):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp5", "tests.integration.test_repl_commands.QuickTask")
    await rm.load(yaml_p)
    # should print without error
    await _dispatch(rm, "list")
    await _dispatch(rm, "list --runs")


async def test_list_instance_command(tmp_path):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp7", "tests.integration.test_repl_commands.QuickTask")
    await rm.load(yaml_p)
    run_id = await rm.start_run("rp7")
    ctx = rm._runs[run_id]
    await ctx.main_task
    # --instance should not raise and should show the run
    await _dispatch(rm, "list --instance")


async def test_stop_ignores_extra_args(tmp_path):
    """stop only accepts instance_id — extra flags are silently ignored (REPL just uses args[0])."""
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp8", "tests.integration.test_repl_commands.SlowTask")
    await rm.load(yaml_p)
    run_id = await rm.start_run("rp8")
    await asyncio.sleep(0.05)
    # Extra flag ignored — only the instance_id matters
    await _dispatch(rm, f"stop {run_id} --step step_a")
    ctx = rm._runs[run_id]
    await ctx.main_task
    assert ctx.abort_event.is_set()


async def test_run_and_inspect_after_completion(tmp_path):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp6", "tests.integration.test_repl_commands.QuickTask")
    await rm.load(yaml_p)
    run_id = await rm.start_run("rp6")
    ctx = rm._runs[run_id]
    await ctx.main_task  # wait for completion

    # inspect should show completed state
    await _dispatch(rm, f"inspect {run_id} --step step_a --task t1")
