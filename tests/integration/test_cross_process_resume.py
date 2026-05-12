"""Tests that restore_runs_from_disk correctly handles orphaned RUNNING state (A1 fix)."""
from __future__ import annotations

import asyncio
import json
import textwrap
from pathlib import Path

import pytest

from pipeline_engine.core.run_manager import RunManager
from pipeline_engine.models.runtime_state import Status


def _make_yaml(tmp_path: Path, pid: str) -> Path:
    content = textwrap.dedent(f"""\
        version: "1.0"
        pipeline:
          id: {pid}
          name: "Cross-process test"
        steps:
          - id: step_a
            tasks:
              - id: t1
                plugin: tests.unit.test_cli.EchoTask
    """)
    p = tmp_path / f"{pid}.yaml"
    p.write_text(content)
    return p


async def test_orphan_running_demoted(tmp_path):
    """Orphaned RUNNING tasks are demoted to FAILED when restored from disk."""
    yaml_p = _make_yaml(tmp_path, "orphan_pipe")

    # Run the pipeline to SUCCESS so we have a completed run on disk.
    rm1 = RunManager(tmp_path)
    await rm1.load(yaml_p)
    run_id = await rm1.start_run("orphan_pipe")
    await rm1._runs[run_id].main_task

    # Grab the persisted state and artificially set a task to RUNNING
    # (simulating a process that was killed mid-execution).
    from pipeline_engine.core import storage
    state = storage.load_state(tmp_path, "orphan_pipe", run_id)
    task_state = state.steps["step_a"].tasks["t1"]
    task_state.status = Status.RUNNING
    task_state.error = None
    state.steps["step_a"].status = Status.RUNNING
    state.status = Status.RUNNING
    storage.persist_state(state)

    # In a new process: create a fresh RunManager, restore from disk.
    rm2 = RunManager(tmp_path)
    await rm2.load(yaml_p)
    rm2.restore_runs_from_disk()

    # Orphaned RUNNING should have been demoted to FAILED.
    ctx = rm2._runs[run_id]
    restored_state = await ctx.state_manager.get_run_state()
    assert restored_state.steps["step_a"].tasks["t1"].status == Status.FAILED
    assert "interrupted" in (restored_state.steps["step_a"].tasks["t1"].error or "")
    assert restored_state.status == Status.FAILED


async def test_orphan_run_can_be_resumed_after_restore(tmp_path):
    """After orphan demotion, resume correctly reschedules the failed task."""
    yaml_p = _make_yaml(tmp_path, "resume_orphan")

    rm1 = RunManager(tmp_path)
    await rm1.load(yaml_p)
    run_id = await rm1.start_run("resume_orphan")
    await rm1._runs[run_id].main_task

    # Force task back to RUNNING on disk.
    from pipeline_engine.core import storage
    state = storage.load_state(tmp_path, "resume_orphan", run_id)
    state.steps["step_a"].tasks["t1"].status = Status.RUNNING
    state.steps["step_a"].tasks["t1"].error = None
    state.steps["step_a"].status = Status.RUNNING
    state.status = Status.RUNNING
    storage.persist_state(state)

    # Restore and resume in a fresh RunManager.
    rm2 = RunManager(tmp_path)
    await rm2.load(yaml_p)
    rm2.restore_runs_from_disk()

    new_run_id = await rm2.resume(run_id)
    await rm2._runs[new_run_id].main_task

    final_state = await rm2.get_run_state(new_run_id)
    assert final_state.status == Status.SUCCESS
    assert final_state.steps["step_a"].tasks["t1"].status == Status.SUCCESS
