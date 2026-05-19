"""Tests for StateManager (state transitions and concurrency safety)."""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from pipeline_engine.core.state_manager import StateManager
from pipeline_engine.models.runtime_state import PipelineRunState, Status


def _make_state(tmp_path: Path) -> PipelineRunState:
    run_dir = tmp_path / ".pipeline_runs" / "pipe1" / "run1"
    run_dir.mkdir(parents=True)
    return PipelineRunState(
        pipeline_id="pipe1",
        run_id="run1",
        workspace=str(run_dir),
    )


@pytest.fixture
def sm(tmp_path) -> StateManager:
    state = _make_state(tmp_path)
    return StateManager(state)


async def test_init_step(sm):
    await sm.init_step("s1", ["t1", "t2"])
    step = await sm.get_step_state("s1")
    assert step.status == Status.NEW
    assert set(step.tasks.keys()) == {"t1", "t2"}


async def test_start_and_finish_task(sm):
    await sm.init_step("s1", ["t1"])
    await sm.start_task("s1", "t1")
    ts = await sm.get_task_state("s1", "t1")
    assert ts.status == Status.RUNNING
    assert ts.started_at is not None

    await sm.finish_task("s1", "t1", output_path="/fake/output.json")
    ts = await sm.get_task_state("s1", "t1")
    assert ts.status == Status.SUCCESS
    assert ts.progress == 100
    assert ts.output_path == "/fake/output.json"


async def test_fail_task(sm):
    await sm.init_step("s1", ["t1"])
    await sm.start_task("s1", "t1")
    await sm.fail_task("s1", "t1", error="boom")
    ts = await sm.get_task_state("s1", "t1")
    assert ts.status == Status.FAILED
    assert ts.error == "boom"


async def test_pause_running_task(sm):
    await sm.init_step("s1", ["t1"])
    await sm.start_task("s1", "t1")
    await sm.pause_task("s1", "t1")
    ts = await sm.get_task_state("s1", "t1")
    assert ts.status == Status.PAUSED


async def test_pause_pending_task_transitions_to_paused(sm):
    await sm.init_step("s1", ["t1"])
    # task is PENDING, pause should mark it PAUSED (e.g. on abort before dispatch)
    await sm.pause_task("s1", "t1")
    ts = await sm.get_task_state("s1", "t1")
    assert ts.status == Status.PAUSED


async def test_pause_success_task_is_noop(sm):
    await sm.init_step("s1", ["t1"])
    await sm.start_task("s1", "t1")
    await sm.finish_task("s1", "t1")
    # completed task should not be paused
    await sm.pause_task("s1", "t1")
    ts = await sm.get_task_state("s1", "t1")
    assert ts.status == Status.SUCCESS


async def test_update_progress(sm):
    await sm.init_step("s1", ["t1"])
    await sm.start_task("s1", "t1")
    await sm.update_progress("s1", "t1", 42)
    ts = await sm.get_task_state("s1", "t1")
    assert ts.progress == 42


async def test_progress_clamped(sm):
    await sm.init_step("s1", ["t1"])
    await sm.start_task("s1", "t1")   # must be RUNNING for update_progress to apply
    await sm.update_progress("s1", "t1", 999)
    ts = await sm.get_task_state("s1", "t1")
    assert ts.progress == 100
    await sm.update_progress("s1", "t1", -5)
    ts = await sm.get_task_state("s1", "t1")
    assert ts.progress == 0


async def test_recover_task(sm):
    await sm.init_step("s1", ["t1"])
    await sm.start_task("s1", "t1")
    await sm.fail_task("s1", "t1", error="oops")
    await sm.recover_task("s1", "t1", output_path="/out.json", fixed_by="test_user@ts")
    ts = await sm.get_task_state("s1", "t1")
    assert ts.status == Status.FIXED
    assert ts.output_path == "/out.json"
    assert ts.fixed_by == "test_user@ts"
    assert ts.error is None


async def test_reset_for_resume_failed(sm):
    await sm.init_step("s1", ["t1"])
    await sm.fail_task("s1", "t1", error="err")
    reset = await sm.reset_for_resume("s1", "t1")
    assert reset is True
    ts = await sm.get_task_state("s1", "t1")
    assert ts.status == Status.NEW
    assert ts.error is None


async def test_reset_for_resume_paused_excluded_by_default(sm):
    await sm.init_step("s1", ["t1"])
    await sm.start_task("s1", "t1")
    await sm.pause_task("s1", "t1")
    reset = await sm.reset_for_resume("s1", "t1", include_paused=False)
    assert reset is False
    ts = await sm.get_task_state("s1", "t1")
    assert ts.status == Status.PAUSED


async def test_reset_for_resume_paused_included(sm):
    await sm.init_step("s1", ["t1"])
    await sm.start_task("s1", "t1")
    await sm.pause_task("s1", "t1")
    reset = await sm.reset_for_resume("s1", "t1", include_paused=True)
    assert reset is True
    ts = await sm.get_task_state("s1", "t1")
    assert ts.status == Status.NEW


async def test_concurrent_writes_dont_lose_updates(sm):
    """Multiple concurrent updates to different tasks must all be reflected."""
    task_ids = [f"t{i}" for i in range(20)]
    await sm.init_step("s1", task_ids)

    async def update_task(tid: str) -> None:
        await sm.start_task("s1", tid)
        await sm.update_progress("s1", tid, 50)
        await sm.finish_task("s1", tid)

    await asyncio.gather(*[update_task(tid) for tid in task_ids])

    state = await sm.get_run_state()
    for tid in task_ids:
        assert state.steps["s1"].tasks[tid].status == Status.SUCCESS


async def test_state_persisted_after_mutation(tmp_path):
    run_dir = tmp_path / ".pipeline_runs" / "pipe1" / "run1"
    run_dir.mkdir(parents=True)
    state = PipelineRunState(
        pipeline_id="pipe1", run_id="run1", workspace=str(run_dir)
    )
    sm = StateManager(state)
    await sm.init_step("s1", ["t1"])
    await sm.start_pipeline()

    import json  # noqa: PLC0415
    state_file = run_dir / "state.json"
    assert state_file.exists()
    saved = json.loads(state_file.read_text())
    assert saved["status"] == "running"


# ── new guard tests (audit fixes) ────────────────────────────────────────────

async def test_recover_task_blocks_running(sm):
    """H7: recover_task raises PipelineError if task is RUNNING."""
    from pipeline_engine.core.errors import PipelineError
    await sm.init_step("s1", ["t1"])
    await sm.start_task("s1", "t1")  # status = RUNNING
    with pytest.raises(PipelineError, match="RUNNING"):
        await sm.recover_task("s1", "t1", output_path="/out.json", fixed_by="test")


async def test_replace_task_input_blocks_running(sm):
    """H7: replace_task_input raises PipelineError if task is RUNNING."""
    from pipeline_engine.core.errors import PipelineError
    await sm.init_step("s1", ["t1"])
    await sm.start_task("s1", "t1")  # status = RUNNING
    with pytest.raises(PipelineError, match="RUNNING"):
        await sm.replace_task_input("s1", "t1")


async def test_unsubscribe_idempotent(sm):
    """C1: unsubscribe of an already-removed queue does not raise."""
    q = sm.subscribe()
    sm.unsubscribe(q)
    sm.unsubscribe(q)  # second call must be a no-op, not ValueError


# ── H10 regression tests ─────────────────────────────────────────────────────

async def test_reset_pipeline_status_clears_timestamps(sm):
    """H10: reset_pipeline_status(NEW) clears both finished_at and started_at."""
    await sm.start_pipeline()          # sets started_at
    await sm.finish_pipeline(success=False)  # sets finished_at

    state = await sm.get_run_state()
    assert state.finished_at is not None
    assert state.started_at is not None

    await sm.reset_pipeline_status(Status.NEW)

    state = await sm.get_run_state()
    assert state.status == Status.NEW
    assert state.finished_at is None, "finished_at must be cleared on resume"
    assert state.started_at is None, "started_at must be cleared on resume"


async def test_reset_for_resume_clears_all_stale_task_fields(sm):
    """H10: reset_for_resume clears error, stack_trace, timestamps and progress."""
    await sm.init_step("s1", ["t1"])
    await sm.start_task("s1", "t1")
    await sm.fail_task("s1", "t1", error="boom")

    ts = await sm.get_task_state("s1", "t1")
    assert ts.error == "boom"
    assert ts.finished_at is not None
    assert ts.started_at is not None

    await sm.reset_for_resume("s1", "t1")

    ts = await sm.get_task_state("s1", "t1")
    assert ts.status == Status.NEW
    assert ts.error is None
    assert ts.stack_trace is None
    assert ts.finished_at is None
    assert ts.started_at is None
    assert ts.progress == 0


# ── M3 regression tests ──────────────────────────────────────────────────────

async def test_start_pipeline_blocks_non_new(sm):
    """M3: start_pipeline raises PipelineError if status is not NEW."""
    from pipeline_engine.core.errors import PipelineError
    await sm.start_pipeline()
    await sm.finish_pipeline(success=True)  # now SUCCESS

    with pytest.raises(PipelineError, match="new"):
        await sm.start_pipeline()


async def test_start_pipeline_blocks_running(sm):
    """M3: start_pipeline raises PipelineError if already RUNNING (double-start)."""
    from pipeline_engine.core.errors import PipelineError
    await sm.start_pipeline()  # NEW → RUNNING

    with pytest.raises(PipelineError, match="new"):
        await sm.start_pipeline()  # RUNNING → should raise


async def test_start_step_blocks_non_new(sm):
    """M3: start_step raises PipelineError if step is not NEW."""
    from pipeline_engine.core.errors import PipelineError
    await sm.init_step("s1", ["t1"])
    await sm.start_step("s1")
    await sm.finish_step("s1", success=True)  # now SUCCESS

    with pytest.raises(PipelineError, match="new"):
        await sm.start_step("s1")


async def test_start_pipeline_allowed_after_reset(sm):
    """M3: start_pipeline succeeds after reset_pipeline_status(NEW) clears a terminal state."""
    await sm.start_pipeline()
    await sm.finish_pipeline(success=True)  # SUCCESS

    await sm.reset_pipeline_status(Status.NEW)  # reset by resume()
    await sm.start_pipeline()  # must not raise

    state = await sm.get_run_state()
    assert state.status == Status.RUNNING


async def test_notify_drops_full_queue(sm):
    """M1: _notify silently drops events for a full queue without raising."""
    from unittest.mock import patch
    q = sm.subscribe()
    # Fill the queue to capacity
    for i in range(256):
        q.put_nowait({"i": i})
    warned = []
    with patch("pipeline_engine.core.state_manager._logger") as mock_log:
        mock_log.warning.side_effect = lambda *a, **kw: warned.append(a)
        await sm.init_step("s1", ["t1"])
        await sm.start_pipeline()  # triggers _notify; queue is full → drop + warn
    assert warned, "expected at least one warning for dropped event"
    assert any("dropped" in str(w) for w in warned)
