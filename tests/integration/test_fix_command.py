"""Integration tests for fix --output / fix --input recovery mechanism."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from pydantic import BaseModel

from pipeline_engine.core import storage
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


# ─── helpers & stubs ──────────────────────────────────────────────────────────

class SuccessTask(BaseTask):
    async def execute(self, inputs, progress):
        await progress(100)
        return {"ok": True}


class FailingTask(BaseTask):
    async def execute(self, inputs, progress):
        raise RuntimeError("task failed intentionally")


class OutputModel(BaseModel):
    value: int


class ValidatedTask(BaseTask):
    OutputModel = OutputModel

    async def execute(self, inputs, progress):
        await progress(100)
        return {"value": 42}


class EchoInputsTask(BaseTask):
    async def execute(self, inputs, progress):
        await progress(100)
        return {"received": inputs}


def _setup(tmp_path: Path, spec: PipelineSpec, run_id: str = "run1"):
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
    return sched, sm


# ─── fix --output tests ───────────────────────────────────────────────────────

async def test_fix_output_recovers_failed_task(tmp_path):
    """fix --output: write output.json + set RECOVERED, then downstream runs."""
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="fix_pipe", name="T"),
        steps=[
            StepSpec(id="s1", tasks=[
                TaskSpec(id="t_fail", plugin=f"{__name__}.FailingTask"),
            ]),
            StepSpec(id="s2", tasks=[
                TaskSpec(id="t_ok", plugin=f"{__name__}.EchoInputsTask",
                         depends_on_steps=["s1"]),
            ]),
        ],
    )
    sched, sm = _setup(tmp_path, spec)
    await sched.run()  # t_fail fails → pipeline fails

    state = await sm.get_run_state()
    assert state.steps["s1"].tasks["t_fail"].status == Status.FAILED

    # fix --output: supply a recovered output
    recover_data = {"recovered": True}
    recover_file = tmp_path / "recover.json"
    recover_file.write_text(json.dumps(recover_data))
    dest = storage.fix_output(
        tmp_path, "fix_pipe", "run1", "s1", "t_fail", recover_file
    )
    await sm.recover_task(
        "s1", "t_fail",
        output_path=str(dest),
        recovered_by="test@now",
    )

    ts = await sm.get_task_state("s1", "t_fail")
    assert ts.status == Status.RECOVERED
    assert ts.recovered_by == "test@now"

    # Resume: scheduler sees output.json → dispatches s2
    abort2 = asyncio.Event()
    sem2 = asyncio.Semaphore(4)

    # Reset s1 step status to allow scheduler to proceed to s2
    async with sm._lock:
        sm._state.steps["s1"].status = Status.SUCCESS
        sm._state.status = Status.PENDING
        sm._persist()

    sched2 = AsyncScheduler(spec, sm, tmp_path, abort2, sem2)
    await sched2._run_step(spec.steps[1])  # run s2 directly

    ts2 = await sm.get_task_state("s2", "t_ok")
    assert ts2.status == Status.SUCCESS
    # s2 received s1's outputs (which contains t_fail's recovered output)
    out2 = storage.load_task_output(tmp_path, "fix_pipe", "run1", "s2", "t_ok")
    assert "s1" in out2["received"]
    assert out2["received"]["s1"]["t_fail"]["recovered"] is True


async def test_fix_output_validates_schema(tmp_path):
    """fix_output rejects invalid JSON, but schema validation is task-level."""
    bad_json = tmp_path / "bad.json"
    bad_json.write_text("{not json")
    with pytest.raises(PipelineError, match="not valid JSON"):
        storage.fix_output(tmp_path, "p", "r", "s", "t", bad_json)


# ─── fix --input tests ────────────────────────────────────────────────────────

async def test_fix_input_writes_input_json(tmp_path):
    """fix --input: write input.json into task directory."""
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="fix_pipe", name="T"),
        steps=[
            StepSpec(id="s1", tasks=[
                TaskSpec(id="t1", plugin=f"{__name__}.SuccessTask"),
            ]),
        ],
    )
    sched, sm = _setup(tmp_path, spec)
    await sm.init_step("s1", ["t1"])

    # Simulate writing input via fix --input
    input_data = {"override_key": "override_val"}
    input_file = tmp_path / "new_input.json"
    input_file.write_text(json.dumps(input_data))

    task_dir = storage.init_task_dir(tmp_path, "fix_pipe", "run1", "s1", "t1")
    storage.atomic_write_json(task_dir / "input.json", input_data)

    # task should still be PENDING (fix --input doesn't change status)
    ts = await sm.get_task_state("s1", "t1")
    assert ts.status == Status.PENDING

    # Verify input was written
    inp = storage.read_json(task_dir / "input.json")
    assert inp == input_data


async def test_fix_output_then_downstream_dependency_satisfied(tmp_path):
    """After fix --output, the output.json file exists → downstream can proceed."""
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="fix_pipe", name="T"),
        steps=[
            StepSpec(id="s1", tasks=[
                TaskSpec(id="producer", plugin=f"{__name__}.FailingTask"),
            ]),
            StepSpec(id="s2", tasks=[
                TaskSpec(id="consumer", plugin=f"{__name__}.EchoInputsTask",
                         depends_on_steps=["s1"]),
            ]),
        ],
    )
    sched, sm = _setup(tmp_path, spec)
    await sched.run()

    # fix --output on the failing task
    recover_file = tmp_path / "r.json"
    recover_file.write_text(json.dumps({"fixed": 1}))
    storage.fix_output(tmp_path, "fix_pipe", "run1", "s1", "producer", recover_file)

    # Now the output.json exists → dependency is satisfied
    assert storage.task_output_exists(tmp_path, "fix_pipe", "run1", "s1", "producer")
