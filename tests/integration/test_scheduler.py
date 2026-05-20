"""Integration tests for AsyncScheduler: parallelism, dependencies, data injection."""
from __future__ import annotations

import asyncio
import time
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


# ─── shared helpers ───────────────────────────────────────────────────────────

def _make_run_state(tmp_path: Path, pipeline_id: str = "test_pipe") -> PipelineRunState:
    run_dir = tmp_path / ".pipeline_runs" / pipeline_id / "run1"
    run_dir.mkdir(parents=True)
    return PipelineRunState(
        pipeline_id=pipeline_id,
        run_id="run1",
        workspace=str(run_dir),
    )


def _make_scheduler(
    spec: PipelineSpec,
    tmp_path: Path,
    *,
    pipeline_id: str = "test_pipe",
) -> tuple[AsyncScheduler, StateManager]:
    run_state = _make_run_state(tmp_path, pipeline_id)
    sm = StateManager(run_state)
    abort = asyncio.Event()
    sem = asyncio.Semaphore(8)
    sched = AsyncScheduler(spec, sm, tmp_path, abort, sem)
    return sched, sm


# ─── stub tasks ───────────────────────────────────────────────────────────────

class InstantTask(BaseTask):
    async def execute(self, inputs, progress):
        await progress(100)
        return {"done": True, "task_id": self.task_id}


class SlowTask(BaseTask):
    """Sleeps for config['sleep'] seconds."""
    async def execute(self, inputs, progress):
        await asyncio.sleep(self.config.get("sleep", 0.1))
        await progress(100)
        return {"task_id": self.task_id, "ts": time.time()}


class EchoTask(BaseTask):
    """Returns its inputs merged with task_id."""
    async def execute(self, inputs, progress):
        await progress(100)
        return {"task_id": self.task_id, "inputs_received": inputs}


class FailTask(BaseTask):
    async def execute(self, inputs, progress):
        raise ValueError("intentional failure")


class OrderedTask(BaseTask):
    _order: list[str] = []  # shared mutable list — tests reset before use

    async def execute(self, inputs, progress):
        self._order.append(self.task_id)
        await asyncio.sleep(0.01)
        await progress(100)
        return {"n": self.task_id}


class BlockingThenAbortTask(BaseTask):
    """Sets abort_event when it starts, then completes normally."""
    _abort_event: asyncio.Event | None = None

    async def execute(self, inputs, progress):
        if self._abort_event:
            self._abort_event.set()
        await asyncio.sleep(0.05)
        await progress(100)
        return {}


# ─── tests ────────────────────────────────────────────────────────────────────

async def test_single_task_runs(tmp_path):
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="test_pipe", name="T", type="测试"),
        steps=[
            StepSpec(id="s1", tasks=[
                TaskSpec(id="t1", plugin=f"{__name__}.InstantTask"),
            ]),
        ],
    )
    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()

    state = await sm.get_run_state()
    assert state.status == Status.SUCCESS
    assert state.steps["s1"].tasks["t1"].status == Status.SUCCESS


async def test_parallel_tasks_run_concurrently(tmp_path):
    """Three independent slow tasks in same step must run in parallel."""
    sleep_s = 0.3
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="test_pipe", name="T", max_parallelism=4, type="测试"),
        steps=[
            StepSpec(id="s1", tasks=[
                TaskSpec(id=f"t{i}", plugin=f"{__name__}.SlowTask",
                         config={"sleep": sleep_s})
                for i in range(3)
            ]),
        ],
    )
    sched, sm = _make_scheduler(spec, tmp_path)
    start = time.time()
    await sched.run()
    elapsed = time.time() - start

    # All three run in parallel: total ≈ sleep_s, not 3*sleep_s
    assert elapsed < sleep_s * 2, f"Expected parallel execution, took {elapsed:.2f}s"
    state = await sm.get_run_state()
    for i in range(3):
        assert state.steps["s1"].tasks[f"t{i}"].status == Status.SUCCESS


async def test_linear_dependency_respected(tmp_path):
    """Tasks must execute in depends_on order."""
    OrderedTask._order = []
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="test_pipe", name="T", type="测试"),
        steps=[
            StepSpec(id="s1", tasks=[
                TaskSpec(id="t1", plugin=f"{__name__}.OrderedTask"),
                TaskSpec(id="t2", plugin=f"{__name__}.OrderedTask", depends_on=["t1"]),
                TaskSpec(id="t3", plugin=f"{__name__}.OrderedTask", depends_on=["t2"]),
            ]),
        ],
    )
    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()
    assert OrderedTask._order == ["t1", "t2", "t3"]


async def test_cross_step_dependency_injected(tmp_path):
    """Downstream step task must receive upstream step output in its inputs."""
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="test_pipe", name="T", type="测试"),
        steps=[
            StepSpec(id="produce", tasks=[
                TaskSpec(id="source", plugin=f"{__name__}.InstantTask"),
            ]),
            StepSpec(id="consume", tasks=[
                TaskSpec(
                    id="sink",
                    plugin=f"{__name__}.EchoTask",
                    depends_on_steps=["produce"],
                ),
            ]),
        ],
    )
    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()

    from pipeline_engine.core import storage
    output = storage.load_task_output(tmp_path, "test_pipe", "run1", "consume", "sink")
    # "produce" step outputs are injected under inputs["produce"]
    assert "produce" in output["inputs_received"]
    assert "source" in output["inputs_received"]["produce"]


async def test_failed_task_marks_step_failed(tmp_path):
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="test_pipe", name="T", type="测试"),
        steps=[
            StepSpec(id="s1", tasks=[
                TaskSpec(id="t1", plugin=f"{__name__}.FailTask"),
            ]),
        ],
    )
    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()

    state = await sm.get_run_state()
    assert state.steps["s1"].tasks["t1"].status == Status.FAILED
    assert state.steps["s1"].status == Status.FAILED
    assert state.status == Status.FAILED


async def test_abort_stops_new_tasks(tmp_path):
    """Setting abort_event prevents new tasks from being dispatched."""
    abort = asyncio.Event()
    BlockingThenAbortTask._abort_event = abort
    run_state = _make_run_state(tmp_path)
    sm = StateManager(run_state)
    sem = asyncio.Semaphore(4)

    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="test_pipe", name="T", type="测试"),
        steps=[
            StepSpec(id="s1", tasks=[
                TaskSpec(id="t1", plugin=f"{__name__}.BlockingThenAbortTask"),
                TaskSpec(id="t2", plugin=f"{__name__}.InstantTask", depends_on=["t1"]),
            ]),
        ],
    )
    sched = AsyncScheduler(spec, sm, tmp_path, abort, sem)
    await sched.run()

    state = await sm.get_run_state()
    # t2 should have been paused because abort fired after t1
    assert state.steps["s1"].tasks["t2"].status == Status.PAUSED


async def test_within_step_output_injected(tmp_path):
    """Downstream task within same step receives upstream task output."""
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="test_pipe", name="T", type="测试"),
        steps=[
            StepSpec(id="s1", tasks=[
                TaskSpec(id="producer", plugin=f"{__name__}.InstantTask"),
                TaskSpec(id="consumer", plugin=f"{__name__}.EchoTask",
                         depends_on=["producer"]),
            ]),
        ],
    )
    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()

    from pipeline_engine.core import storage
    out = storage.load_task_output(tmp_path, "test_pipe", "run1", "s1", "consumer")
    assert "producer" in out["inputs_received"]
    assert out["inputs_received"]["producer"]["done"] is True


# ─── P1: run_step / run_task pipeline status ──────────────────────────────────

def _single_step_spec(plugin: str) -> PipelineSpec:
    return PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="test_pipe", name="T", type="测试"),
        steps=[StepSpec(id="step_a", tasks=[TaskSpec(id="t1", plugin=plugin)])],
    )


async def test_run_step_pipeline_status_success(tmp_path):
    """run_step() must advance pipeline from NEW → RUNNING → SUCCESS (not stay NEW)."""
    spec = _single_step_spec(f"{__name__}.InstantTask")
    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run_step("step_a")
    state = await sm.get_run_state()
    assert state.status == Status.SUCCESS


async def test_run_step_pipeline_status_failed_on_task_error(tmp_path):
    """If the step's task fails, run_step() must mark pipeline FAILED (not stay NEW)."""
    spec = _single_step_spec(f"{__name__}.FailTask")
    sched, sm = _make_scheduler(spec, tmp_path)
    # Task failures are caught internally; run_step() itself does not raise.
    await sched.run_step("step_a")
    state = await sm.get_run_state()
    assert state.status == Status.FAILED


async def test_run_task_pipeline_status_success(tmp_path):
    """run_task() must advance pipeline from NEW → RUNNING → SUCCESS (not stay NEW)."""
    spec = _single_step_spec(f"{__name__}.InstantTask")
    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run_task("step_a", "t1")
    state = await sm.get_run_state()
    assert state.status == Status.SUCCESS


async def test_run_task_pipeline_status_failed_on_task_error(tmp_path):
    """If the dispatched task fails, run_task() must mark pipeline FAILED (not stay NEW)."""
    spec = _single_step_spec(f"{__name__}.FailTask")
    sched, sm = _make_scheduler(spec, tmp_path)
    # Task failures are caught internally; run_task() itself does not raise.
    await sched.run_task("step_a", "t1")
    state = await sm.get_run_state()
    assert state.status == Status.FAILED
