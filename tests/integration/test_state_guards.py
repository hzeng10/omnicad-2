"""Tests for state-machine transition guards in StateManager (B5 / B2 fixes)."""
from __future__ import annotations

import asyncio
import pytest

from pipeline_engine.core.state_manager import StateManager
from pipeline_engine.models.pipeline_spec import PipelineMeta, PipelineSpec, StepSpec, TaskSpec
from pipeline_engine.models.runtime_state import PipelineRunState, Status


def _make_sm(tmp_path) -> StateManager:
    run_state = PipelineRunState(
        pipeline_id="guard_pipe",
        run_id="r1",
        workspace=str(tmp_path / "run"),
    )
    (tmp_path / "run").mkdir(parents=True, exist_ok=True)
    return StateManager(run_state)


async def _init(sm: StateManager, step="s1", task="t1") -> None:
    await sm.init_step(step, [task])


# ─── finish_task guards ───────────────────────────────────────────────────────

async def test_finish_task_from_running_succeeds(tmp_path):
    sm = _make_sm(tmp_path)
    await _init(sm)
    await sm.start_step("s1")
    await sm.start_task("s1", "t1")
    await sm.finish_task("s1", "t1")
    state = await sm.get_run_state()
    assert state.steps["s1"].tasks["t1"].status == Status.SUCCESS


async def test_finish_task_from_paused_is_noop(tmp_path):
    """B2 fix: finish_task must not overwrite a PAUSED task (thread race)."""
    sm = _make_sm(tmp_path)
    await _init(sm)
    await sm.start_step("s1")
    await sm.start_task("s1", "t1")
    await sm.pause_task("s1", "t1")           # task paused (e.g. abort signalled)
    await sm.finish_task("s1", "t1")          # late result from still-running thread
    state = await sm.get_run_state()
    assert state.steps["s1"].tasks["t1"].status == Status.PAUSED  # preserved


async def test_finish_task_from_recovered_is_noop(tmp_path):
    """finish_task must not overwrite a RECOVERED task."""
    sm = _make_sm(tmp_path)
    await _init(sm)
    await sm.start_step("s1")
    await sm.start_task("s1", "t1")
    await sm.fail_task("s1", "t1", error="oops")
    await sm.recover_task("s1", "t1", output_path="/x/out.json", recovered_by="tester")
    await sm.finish_task("s1", "t1")
    state = await sm.get_run_state()
    assert state.steps["s1"].tasks["t1"].status == Status.RECOVERED


async def test_finish_task_from_success_is_noop(tmp_path):
    """Calling finish_task twice must be idempotent."""
    sm = _make_sm(tmp_path)
    await _init(sm)
    await sm.start_step("s1")
    await sm.start_task("s1", "t1")
    await sm.finish_task("s1", "t1")
    await sm.finish_task("s1", "t1")          # second call — no-op
    state = await sm.get_run_state()
    assert state.steps["s1"].tasks["t1"].status == Status.SUCCESS


# ─── fail_task guards ─────────────────────────────────────────────────────────

async def test_fail_task_from_running_marks_failed(tmp_path):
    sm = _make_sm(tmp_path)
    await _init(sm)
    await sm.start_step("s1")
    await sm.start_task("s1", "t1")
    await sm.fail_task("s1", "t1", error="boom")
    state = await sm.get_run_state()
    assert state.steps["s1"].tasks["t1"].status == Status.FAILED


async def test_fail_task_from_success_is_noop(tmp_path):
    sm = _make_sm(tmp_path)
    await _init(sm)
    await sm.start_step("s1")
    await sm.start_task("s1", "t1")
    await sm.finish_task("s1", "t1")
    await sm.fail_task("s1", "t1", error="late error")
    state = await sm.get_run_state()
    assert state.steps["s1"].tasks["t1"].status == Status.SUCCESS


async def test_fail_task_from_paused_is_noop(tmp_path):
    sm = _make_sm(tmp_path)
    await _init(sm)
    await sm.start_step("s1")
    await sm.start_task("s1", "t1")
    await sm.pause_task("s1", "t1")
    await sm.fail_task("s1", "t1", error="late error")
    state = await sm.get_run_state()
    assert state.steps["s1"].tasks["t1"].status == Status.PAUSED


async def test_fail_task_from_recovered_is_noop(tmp_path):
    sm = _make_sm(tmp_path)
    await _init(sm)
    await sm.start_step("s1")
    await sm.start_task("s1", "t1")
    await sm.fail_task("s1", "t1", error="first fail")
    await sm.recover_task("s1", "t1", output_path="/x/out.json", recovered_by="tester")
    await sm.fail_task("s1", "t1", error="re-fail attempt")
    state = await sm.get_run_state()
    assert state.steps["s1"].tasks["t1"].status == Status.RECOVERED


# ─── update_progress guards ───────────────────────────────────────────────────

async def test_update_progress_only_while_running(tmp_path):
    sm = _make_sm(tmp_path)
    await _init(sm)
    await sm.start_step("s1")
    await sm.start_task("s1", "t1")
    await sm.pause_task("s1", "t1")
    await sm.update_progress("s1", "t1", 50)   # must be silently ignored
    state = await sm.get_run_state()
    assert state.steps["s1"].tasks["t1"].progress == 0  # unchanged


# ─── demote_orphans_sync ──────────────────────────────────────────────────────

def test_demote_orphans_sync_resets_running(tmp_path):
    """A1 fix: orphaned RUNNING tasks must be demoted to FAILED on restore."""
    import json
    from pipeline_engine.core import storage

    run_dir = tmp_path / ".pipeline_runs" / "p1" / "r1"
    run_dir.mkdir(parents=True)
    run_state = PipelineRunState(
        pipeline_id="p1",
        run_id="r1",
        workspace=str(run_dir),
        status=Status.RUNNING,
    )
    sm = StateManager(run_state)
    # Manually set a step and task to RUNNING (simulating a crashed process)
    asyncio.run(sm.init_step("s1", ["t1"]))
    asyncio.run(sm.start_step("s1"))
    asyncio.run(sm.start_task("s1", "t1"))

    # Now restore as if in a new process
    saved_state = storage.load_state(tmp_path, "p1", "r1")
    sm2 = StateManager(saved_state)
    sm2.demote_orphans_sync()

    assert sm2._state.steps["s1"].tasks["t1"].status == Status.FAILED
    assert "interrupted" in sm2._state.steps["s1"].tasks["t1"].error
    assert sm2._state.steps["s1"].status == Status.FAILED
    assert sm2._state.status == Status.FAILED


def test_demote_orphans_sync_leaves_terminal_states(tmp_path):
    """demote_orphans_sync must not touch SUCCESS/FAILED/RECOVERED tasks."""
    import json
    from pipeline_engine.core import storage

    run_dir = tmp_path / ".pipeline_runs" / "p2" / "r1"
    run_dir.mkdir(parents=True)
    run_state = PipelineRunState(
        pipeline_id="p2",
        run_id="r1",
        workspace=str(run_dir),
    )
    sm = StateManager(run_state)
    asyncio.run(sm.init_step("s1", ["t1", "t2"]))
    asyncio.run(sm.start_step("s1"))
    asyncio.run(sm.start_task("s1", "t1"))
    asyncio.run(sm.finish_task("s1", "t1"))          # t1 = SUCCESS
    asyncio.run(sm.start_task("s1", "t2"))
    asyncio.run(sm.fail_task("s1", "t2", error="err"))  # t2 = FAILED

    saved_state = storage.load_state(tmp_path, "p2", "r1")
    sm2 = StateManager(saved_state)
    sm2.demote_orphans_sync()  # no RUNNING tasks → no changes

    assert sm2._state.steps["s1"].tasks["t1"].status == Status.SUCCESS
    assert sm2._state.steps["s1"].tasks["t2"].status == Status.FAILED
