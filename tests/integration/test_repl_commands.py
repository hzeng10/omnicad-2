"""Integration tests for REPL command dispatch."""
from __future__ import annotations

import asyncio
import textwrap
from pathlib import Path

import pytest

from pipeline_engine.core.base_task import BaseTask
from pipeline_engine.core.run_manager import RunManager
from pipeline_engine.repl import _dispatch


def _make_svc(rm: RunManager):
    from pipeline_engine.service import PipelineService
    return PipelineService(rm, no_autoload=True)


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
    await _dispatch(_make_svc(rm), f"load {yaml_p}")
    assert "rp1" in {p["pipeline_id"] for p in rm.list_pipelines()}


async def test_run_command_starts_run(tmp_path):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp2", "tests.integration.test_repl_commands.QuickTask")
    await rm.load(yaml_p)
    await _dispatch(_make_svc(rm), "start rp2")
    assert len(rm._runs) == 1


async def test_status_command_during_active_run(tmp_path, capsys):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp3", "tests.integration.test_repl_commands.SlowTask")
    await rm.load(yaml_p)
    run_id = await rm.start_run("rp3")

    # status should not raise while run is active
    await _dispatch(_make_svc(rm), f"status {run_id}")

    ctx = rm._runs[run_id]
    await ctx.main_task


async def test_stop_command_sets_abort(tmp_path):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp4", "tests.integration.test_repl_commands.SlowTask")
    await rm.load(yaml_p)
    run_id = await rm.start_run("rp4")
    await asyncio.sleep(0.05)

    await _dispatch(_make_svc(rm), f"stop {run_id}")
    ctx = rm._runs[run_id]
    await ctx.main_task
    assert ctx.abort_event.is_set()


async def test_help_command_does_not_raise(tmp_path):
    rm = RunManager(tmp_path)
    await _dispatch(_make_svc(rm), "help")


async def test_unknown_command_does_not_raise(tmp_path):
    rm = RunManager(tmp_path)
    await _dispatch(_make_svc(rm), "unknowncmd foo bar")


async def test_list_command(tmp_path):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp5", "tests.integration.test_repl_commands.QuickTask")
    await rm.load(yaml_p)
    # should print without error
    await _dispatch(_make_svc(rm), "list")
    await _dispatch(_make_svc(rm), "list --runs")


async def test_list_instance_command(tmp_path):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp7", "tests.integration.test_repl_commands.QuickTask")
    await rm.load(yaml_p)
    run_id = await rm.start_run("rp7")
    ctx = rm._runs[run_id]
    await ctx.main_task
    # --instance should not raise and should show the run
    await _dispatch(_make_svc(rm), "list --instance")


async def test_stop_ignores_extra_args(tmp_path):
    """stop only accepts instance_id — extra flags are silently ignored (REPL just uses args[0])."""
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp8", "tests.integration.test_repl_commands.SlowTask")
    await rm.load(yaml_p)
    run_id = await rm.start_run("rp8")
    await asyncio.sleep(0.05)
    # Extra flag ignored — only the instance_id matters
    await _dispatch(_make_svc(rm), f"stop {run_id} --step step_a")
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
    await _dispatch(_make_svc(rm), f"inspect {run_id} --step step_a --task t1")


# ─── P2: status --all shows real Status values (H1) ──────────────────────────

async def test_status_all_shows_status_values(tmp_path, capsys):
    """status --all must show real Status strings (RUNNING/SUCCESS/etc.), not bool."""
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp9", "tests.integration.test_repl_commands.QuickTask")
    await rm.load(yaml_p)
    run_id = await rm.start_run("rp9")
    ctx = rm._runs[run_id]
    await ctx.main_task

    await _dispatch(_make_svc(rm), "status --all")
    out = capsys.readouterr().out
    # Must show a real status value, not just "是"/"否"
    assert any(s in out for s in ("success", "failed", "running", "paused", "new"))


# ─── P2: resume message non-blocking (M3) ────────────────────────────────────

async def test_resume_message_non_blocking(tmp_path, capsys):
    """REPL resume must print 已开始恢复（运行中）, not 已恢复."""
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp10", "tests.integration.test_repl_commands.SlowTask")
    await rm.load(yaml_p)
    run_id = await rm.start_run("rp10")
    ctx = rm._runs[run_id]
    await asyncio.sleep(0.05)
    await rm.stop(run_id)
    await ctx.main_task

    await _dispatch(_make_svc(rm), f"resume {run_id}")
    out = capsys.readouterr().out
    assert "运行中" in out
    ctx2 = rm._runs[run_id]
    await ctx2.main_task


# ─── P3: load goes through PipelineService (C1) ───────────────────────────────

async def test_load_via_service(tmp_path):
    """After P3, load command uses svc.cmd_load which validates and registers."""
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp11", "tests.integration.test_repl_commands.QuickTask")
    svc = _make_svc(rm)
    await _dispatch(svc, f"load {yaml_p}")
    assert "rp11" in {p["pipeline_id"] for p in rm.list_pipelines()}


# ─── P3: start goes through PipelineService (C1) ─────────────────────────────

async def test_start_via_service(tmp_path):
    """After P3, start command routes through svc.cmd_start(wait=False)."""
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rp12", "tests.integration.test_repl_commands.QuickTask")
    await rm.load(yaml_p)
    await _dispatch(_make_svc(rm), "start rp12")
    assert len(rm._runs) == 1


# ─── Bug fix: REPL must not restore run instances from previous sessions ───────

async def test_repl_bootstrap_does_not_restore_old_runs(tmp_path):
    """REPL bootstrap (restore_runs=False) must not expose runs from prior sessions."""
    from pipeline_engine.core import storage
    from pipeline_engine.core.run_manager import RunManager as RM
    from pipeline_engine.models.runtime_state import PipelineRunState, Status
    from pipeline_engine.service import PipelineService

    # Simulate a completed run written to disk by a previous CLI session
    pid = "old_pipe"
    run_id = f"{pid}_20260520-000000_0001"
    run_dir = storage.get_run_dir(tmp_path, pid, run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    storage.persist_state(PipelineRunState(
        pipeline_id=pid, run_id=run_id, workspace=str(run_dir), status=Status.SUCCESS
    ))

    rm = RM(tmp_path)
    svc = PipelineService(rm, no_autoload=True)
    await svc.bootstrap(restore_runs=False, restore_writeback=False)

    assert run_id not in rm._runs
    instances = await rm.list_instances()
    assert not any(i["instance_id"] == run_id for i in instances)
