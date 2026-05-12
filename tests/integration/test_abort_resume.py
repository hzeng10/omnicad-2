"""Integration tests for abort/resume lifecycle."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from pipeline_engine.core.base_task import BaseTask
from pipeline_engine.core.scheduler import AsyncScheduler
from pipeline_engine.core.state_manager import StateManager
from pipeline_engine.models.pipeline_spec import (
    PipelineMeta,
    PipelineSpec,
    StepSpec,
    TaskSpec,
)
from pipeline_engine.models.runtime_state import PipelineRunState, Status


# ─── helpers & stub tasks ─────────────────────────────────────────────────────

class QuickTask(BaseTask):
    async def execute(self, inputs, progress):
        await asyncio.sleep(0.01)
        await progress(100)
        return {"ok": True}


class LatchedTask(BaseTask):
    """Blocks until an external event is set, then finishes."""
    _go_event: asyncio.Event | None = None

    async def execute(self, inputs, progress):
        if self._go_event:
            await self._go_event.wait()
        await progress(100)
        return {"latched": True}


def _build_spec(pipeline_id: str = "abort_pipe") -> PipelineSpec:
    return PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id=pipeline_id, name="T"),
        steps=[
            StepSpec(id="s1", tasks=[
                TaskSpec(id="t1", plugin=f"{__name__}.QuickTask"),
                TaskSpec(id="t2", plugin=f"{__name__}.QuickTask"),
            ]),
            StepSpec(id="s2", tasks=[
                TaskSpec(id="t3", plugin=f"{__name__}.QuickTask"),
            ]),
        ],
    )


def _make_context(spec: PipelineSpec, tmp_path: Path, run_id: str = "run1"):
    run_dir = tmp_path / ".pipeline_runs" / spec.pipeline.id / run_id
    run_dir.mkdir(parents=True)
    run_state = PipelineRunState(
        pipeline_id=spec.pipeline.id,
        run_id=run_id,
        workspace=str(run_dir),
    )
    sm = StateManager(run_state)
    abort = asyncio.Event()
    sem = asyncio.Semaphore(4)
    sched = AsyncScheduler(spec, sm, tmp_path, abort, sem)
    return sched, sm, abort


# ─── tests ────────────────────────────────────────────────────────────────────

async def test_full_pipeline_succeeds(tmp_path):
    spec = _build_spec()
    sched, sm, _ = _make_context(spec, tmp_path)
    await sched.run()
    state = await sm.get_run_state()
    assert state.status == Status.SUCCESS


async def test_abort_mid_run_pauses_pending_tasks(tmp_path):
    """When abort fires, tasks not yet dispatched are left PAUSED."""
    abort_ev = asyncio.Event()
    LatchedTask._go_event = None  # don't block for this test

    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="abort_pipe", name="T"),
        steps=[
            StepSpec(id="s1", tasks=[
                TaskSpec(id="t1", plugin=f"{__name__}.QuickTask"),
                TaskSpec(id="t2", plugin=f"{__name__}.QuickTask"),
            ]),
        ],
    )

    run_dir = tmp_path / ".pipeline_runs" / "abort_pipe" / "run1"
    run_dir.mkdir(parents=True)
    run_state = PipelineRunState(
        pipeline_id="abort_pipe", run_id="run1", workspace=str(run_dir)
    )
    sm = StateManager(run_state)
    sem = asyncio.Semaphore(1)  # only 1 concurrent task — forces sequential

    sched = AsyncScheduler(spec, sm, tmp_path, abort_ev, sem)
    # Abort before running
    abort_ev.set()
    await sched.run()

    state = await sm.get_run_state()
    # Both tasks should be PAUSED (abort was set before dispatch)
    for tid in ("t1", "t2"):
        assert state.steps["s1"].tasks[tid].status == Status.PAUSED


async def test_resume_failed_tasks_only_by_default(tmp_path):
    """resume resets Failed tasks; Paused tasks stay Paused without --include-paused."""
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="resume_pipe", name="T"),
        steps=[
            StepSpec(id="s1", tasks=[
                TaskSpec(id="t_fail", plugin=f"{__name__}.QuickTask"),
                TaskSpec(id="t_paused", plugin=f"{__name__}.QuickTask"),
            ]),
        ],
    )
    run_dir = tmp_path / ".pipeline_runs" / "resume_pipe" / "run1"
    run_dir.mkdir(parents=True)
    run_state = PipelineRunState(
        pipeline_id="resume_pipe", run_id="run1", workspace=str(run_dir)
    )
    sm = StateManager(run_state)
    await sm.init_step("s1", ["t_fail", "t_paused"])
    await sm.fail_task("s1", "t_fail", error="err")
    await sm.start_task("s1", "t_paused")
    await sm.pause_task("s1", "t_paused")

    reset_fail = await sm.reset_for_resume("s1", "t_fail", include_paused=False)
    reset_paused = await sm.reset_for_resume("s1", "t_paused", include_paused=False)

    assert reset_fail is True
    assert reset_paused is False
    assert (await sm.get_task_state("s1", "t_fail")).status == Status.PENDING
    assert (await sm.get_task_state("s1", "t_paused")).status == Status.PAUSED


async def test_resume_include_paused(tmp_path):
    """--include-paused resets both Failed and Paused tasks."""
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="resume_pipe", name="T"),
        steps=[StepSpec(id="s1", tasks=[
            TaskSpec(id="t1", plugin=f"{__name__}.QuickTask"),
        ])],
    )
    run_dir = tmp_path / ".pipeline_runs" / "resume_pipe" / "run1"
    run_dir.mkdir(parents=True)
    run_state = PipelineRunState(
        pipeline_id="resume_pipe", run_id="run1", workspace=str(run_dir)
    )
    sm = StateManager(run_state)
    await sm.init_step("s1", ["t1"])
    await sm.start_task("s1", "t1")
    await sm.pause_task("s1", "t1")

    reset = await sm.reset_for_resume("s1", "t1", include_paused=True)
    assert reset is True
    assert (await sm.get_task_state("s1", "t1")).status == Status.PENDING


async def test_run_id_stable_across_resume(tmp_path):
    """After partial run + resume, run_id must not change."""
    spec = _build_spec("stable_run")
    sched, sm, abort = _make_context(spec, tmp_path)

    # abort immediately
    abort.set()
    await sched.run()
    run_state = await sm.get_run_state()
    original_run_id = run_state.run_id

    # Resume (reset all paused tasks and run again)
    for step_id, step_state in run_state.steps.items():
        for tid in step_state.tasks:
            await sm.reset_for_resume(step_id, tid, include_paused=True)

    abort2 = asyncio.Event()
    sem2 = asyncio.Semaphore(4)
    sched2 = AsyncScheduler(spec, sm, tmp_path, abort2, sem2)
    await sched2.run()

    final_state = await sm.get_run_state()
    assert final_state.run_id == original_run_id
    assert final_state.status == Status.SUCCESS
