"""Integration tests for skip=true step handling."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from pipeline_engine.core.base_task import BaseTask
from pipeline_engine.core.errors import PipelineError
from pipeline_engine.core.scheduler import AsyncScheduler
from pipeline_engine.core.state_manager import StateManager
from pipeline_engine.models.pipeline_spec import (
    PipelineMeta,
    PipelineSpec,
    StepSpec,
    TaskSpec,
)
from pipeline_engine.models.runtime_state import PipelineRunState, Status


def _make_scheduler(spec: PipelineSpec, tmp_path: Path):
    run_dir = tmp_path / ".pipeline_runs" / spec.pipeline.id / "run1"
    run_dir.mkdir(parents=True)
    run_state = PipelineRunState(
        pipeline_id=spec.pipeline.id,
        run_id="run1",
        workspace=str(run_dir),
    )
    sm = StateManager(run_state)
    abort = asyncio.Event()
    sem = asyncio.Semaphore(4)
    return AsyncScheduler(spec, sm, tmp_path, abort, sem), sm


class ConsumerTask(BaseTask):
    """Reads the upstream step output from inputs and echoes it."""
    async def execute(self, inputs, progress):
        await progress(100)
        return {"received": inputs}


def _write_manual_data(tmp_path: Path, step_id: str, data: dict) -> None:
    p = tmp_path / "manual_data" / step_id / "output.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data))


# ─── tests ────────────────────────────────────────────────────────────────────

async def test_skip_step_with_manual_data(tmp_path):
    """A skipped step loads manual_data and marks itself SKIPPED."""
    _write_manual_data(tmp_path, "s1", {"manual": True})

    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="skip_pipe", name="T", type="测试"),
        steps=[
            StepSpec(id="s1", skip=True, tasks=[
                TaskSpec(id="t1", plugin="tests.integration.test_scheduler.InstantTask"),
            ]),
        ],
    )
    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()

    state = await sm.get_run_state()
    assert state.steps["s1"].status == Status.SKIPPED


async def test_skip_step_missing_manual_data_raises(tmp_path):
    """A skipped step with no manual_data must raise PipelineError."""
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="skip_pipe", name="T", type="测试"),
        steps=[
            StepSpec(id="s1", skip=True, tasks=[
                TaskSpec(id="t1", plugin="tests.integration.test_scheduler.InstantTask"),
            ]),
        ],
    )
    sched, sm = _make_scheduler(spec, tmp_path)
    with pytest.raises(PipelineError, match="manual_data"):
        await sched.run()


async def test_skipped_step_feeds_downstream(tmp_path):
    """A2 fix: downstream task with depends_on_steps receives manual_data content."""
    manual = {"config_key": "from_manual", "count": 7}
    _write_manual_data(tmp_path, "s1", manual)

    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="skip_feed_pipe", name="T", type="测试"),
        steps=[
            StepSpec(id="s1", skip=True, tasks=[
                TaskSpec(id="t1", plugin="tests.integration.test_scheduler.InstantTask"),
            ]),
            StepSpec(id="s2", tasks=[
                TaskSpec(
                    id="consumer",
                    plugin=f"{__name__}.ConsumerTask",
                    depends_on_steps=["s1"],
                ),
            ]),
        ],
    )
    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()

    from pipeline_engine.core import storage
    run_state = await sm.get_run_state()
    assert run_state.steps["s1"].status == Status.SKIPPED
    assert run_state.steps["s2"].tasks["consumer"].status == Status.SUCCESS

    # Consumer must have received the manual_data as inputs["s1"]
    consumer_state = run_state.steps["s2"].tasks["consumer"]
    task_dir = storage.get_task_dir(tmp_path, "skip_feed_pipe", "run1", "s2", "consumer")
    output = storage.read_json(task_dir / "output.json")
    # ConsumerTask returns {"received": inputs}, so inputs["s1"] == manual
    assert output["received"].get("s1") == manual


async def test_downstream_step_runs_after_skip(tmp_path):
    """The step after a skipped step runs normally."""
    _write_manual_data(tmp_path, "s1", {"value": 99})

    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="skip_pipe", name="T", type="测试"),
        steps=[
            StepSpec(id="s1", skip=True, tasks=[
                TaskSpec(id="t1", plugin="tests.integration.test_scheduler.InstantTask"),
            ]),
            StepSpec(id="s2", tasks=[
                TaskSpec(id="reader", plugin=f"{__name__}.ConsumerTask"),
            ]),
        ],
    )
    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()

    state = await sm.get_run_state()
    assert state.steps["s1"].status == Status.SKIPPED
    assert state.steps["s2"].tasks["reader"].status == Status.SUCCESS
    assert state.status == Status.SUCCESS
