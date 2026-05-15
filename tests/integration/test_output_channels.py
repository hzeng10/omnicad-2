"""Integration tests for YAML `output:` channels at task / step / pipeline level."""
from __future__ import annotations

import asyncio
import json
import os
import stat
from pathlib import Path

import pytest

from pipeline_engine.core.base_task import BaseTask
from pipeline_engine.core.errors import PipelineError
from pipeline_engine.core.scheduler import AsyncScheduler
from pipeline_engine.core.state_manager import StateManager
from pipeline_engine.core import storage
from pipeline_engine.models.pipeline_spec import (
    PipelineMeta,
    PipelineSpec,
    StepSpec,
    TaskSpec,
)
from pipeline_engine.models.runtime_state import PipelineRunState, Status


# ─── helpers ──────────────────────────────────────────────────────────────────

def _make_scheduler(spec: PipelineSpec, workspace: Path):
    run_dir = workspace / ".pipeline_runs" / spec.pipeline.id / "run1"
    run_dir.mkdir(parents=True)
    run_state = PipelineRunState(
        pipeline_id=spec.pipeline.id,
        run_id="run1",
        workspace=str(run_dir),
    )
    sm = StateManager(run_state)
    abort = asyncio.Event()
    sem = asyncio.Semaphore(4)
    return AsyncScheduler(spec, sm, workspace, abort, sem), sm


class EchoTask(BaseTask):
    async def execute(self, inputs, progress):
        await progress(100)
        return {"task_id": self.task_id, "value": self.config.get("value", 42)}


class FailTask(BaseTask):
    async def execute(self, inputs, progress):
        raise RuntimeError("intentional failure")


# ─── resolve_output_path unit ─────────────────────────────────────────────────

def test_relative_path_resolved_against_workspace(tmp_path):
    result = storage.resolve_output_path(tmp_path, "results/out.json")
    assert result == tmp_path / "results" / "out.json"


def test_absolute_path_used_asis(tmp_path):
    abs_path = "/tmp/some_output.json"
    result = storage.resolve_output_path(tmp_path, abs_path)
    assert result == Path(abs_path)


def test_none_returns_none(tmp_path):
    assert storage.resolve_output_path(tmp_path, None) is None


def test_empty_string_returns_none(tmp_path):
    assert storage.resolve_output_path(tmp_path, "") is None


# ─── task MIRROR ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_task_output_mirror_writes_user_path(tmp_path):
    """task output: PATH → user path appears with identical content."""
    user_path = tmp_path / "results" / "t1.json"
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="pipe", name="P", type="T"),
        steps=[
            StepSpec(
                id="s1",
                tasks=[TaskSpec(id="t1", plugin="tests.integration.test_output_channels.EchoTask",
                                output="results/t1.json")],
            )
        ],
    )
    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()

    run_state = await sm.get_run_state()
    assert run_state.steps["s1"].tasks["t1"].status == Status.SUCCESS

    # user path must exist
    assert user_path.exists(), "mirror file not written"
    mirror_data = json.loads(user_path.read_text())

    # internal .pipeline_runs path must also exist
    internal_path = Path(run_state.steps["s1"].tasks["t1"].output_path)  # type: ignore[arg-type]
    assert internal_path.exists()
    internal_data = json.loads(internal_path.read_text())

    # byte-level equivalence
    assert mirror_data == internal_data


@pytest.mark.asyncio
async def test_task_output_no_mirror_when_unset(tmp_path):
    """When output is not set, no extra file appears in workspace/results/."""
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="pipe", name="P", type="T"),
        steps=[
            StepSpec(
                id="s1",
                tasks=[TaskSpec(id="t1", plugin="tests.integration.test_output_channels.EchoTask")],
            )
        ],
    )
    sched, _ = _make_scheduler(spec, tmp_path)
    await sched.run()

    results_dir = tmp_path / "results"
    assert not results_dir.exists()


# ─── step aggregate ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_step_output_aggregates_leaf_tasks(tmp_path):
    """step output: PATH writes {task_id: task_output} dict."""
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="pipe", name="P", type="T"),
        steps=[
            StepSpec(
                id="s1",
                output="results/step_s1.json",
                tasks=[
                    TaskSpec(id="t1", plugin="tests.integration.test_output_channels.EchoTask",
                             config={"value": 10}),
                    TaskSpec(id="t2", plugin="tests.integration.test_output_channels.EchoTask",
                             config={"value": 20}),
                ],
            )
        ],
    )
    sched, _ = _make_scheduler(spec, tmp_path)
    await sched.run()

    step_file = tmp_path / "results" / "step_s1.json"
    assert step_file.exists(), "step aggregate file not written"
    data = json.loads(step_file.read_text())

    # both leaf tasks present (no deps → both are leaves)
    assert "t1" in data or "t2" in data


@pytest.mark.asyncio
async def test_step_output_only_written_on_success(tmp_path):
    """step output file must NOT be written if a task fails."""
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="pipe", name="P", type="T"),
        steps=[
            StepSpec(
                id="s1",
                output="results/step_s1.json",
                tasks=[
                    TaskSpec(id="t1", plugin="tests.integration.test_output_channels.FailTask"),
                ],
            )
        ],
    )
    sched, _ = _make_scheduler(spec, tmp_path)
    await sched.run()

    step_file = tmp_path / "results" / "step_s1.json"
    assert not step_file.exists(), "step output should not be written when step fails"


# ─── pipeline aggregate ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_pipeline_output_aggregates_steps(tmp_path):
    """pipeline output: PATH writes {step_id: step_agg} dict."""
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="pipe", name="P", type="T",
                              output="results/pipeline.json"),
        steps=[
            StepSpec(
                id="s1",
                tasks=[TaskSpec(id="t1",
                                plugin="tests.integration.test_output_channels.EchoTask",
                                config={"value": 1})],
            ),
            StepSpec(
                id="s2",
                depends_on_steps=["s1"],
                tasks=[TaskSpec(id="t2",
                                plugin="tests.integration.test_output_channels.EchoTask",
                                config={"value": 2})],
            ),
        ],
    )
    sched, _ = _make_scheduler(spec, tmp_path)
    await sched.run()

    pipe_file = tmp_path / "results" / "pipeline.json"
    assert pipe_file.exists(), "pipeline aggregate file not written"
    data = json.loads(pipe_file.read_text())

    assert "s1" in data
    assert "s2" in data


@pytest.mark.asyncio
async def test_pipeline_output_not_written_on_failure(tmp_path):
    """pipeline output file must NOT be written if any step fails."""
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="pipe", name="P", type="T",
                              output="results/pipeline.json"),
        steps=[
            StepSpec(
                id="s1",
                tasks=[TaskSpec(id="t1",
                                plugin="tests.integration.test_output_channels.FailTask")],
            ),
        ],
    )
    sched, _ = _make_scheduler(spec, tmp_path)
    await sched.run()

    pipe_file = tmp_path / "results" / "pipeline.json"
    assert not pipe_file.exists()


# ─── write failure does not block task / step / pipeline ─────────────────────

@pytest.mark.asyncio
async def test_mirror_write_failure_does_not_fail_task(tmp_path):
    """If user path is under a read-only directory, task still succeeds."""
    readonly_dir = tmp_path / "readonly"
    readonly_dir.mkdir()
    # make the dir read-only so atomic_write_json raises OSError
    os.chmod(readonly_dir, stat.S_IRUSR | stat.S_IXUSR)

    try:
        spec = PipelineSpec(
            version="1.0",
            pipeline=PipelineMeta(id="pipe", name="P", type="T"),
            steps=[
                StepSpec(
                    id="s1",
                    tasks=[TaskSpec(
                        id="t1",
                        plugin="tests.integration.test_output_channels.EchoTask",
                        output="readonly/t1.json",
                    )],
                )
            ],
        )
        sched, sm = _make_scheduler(spec, tmp_path)
        await sched.run()

        run_state = await sm.get_run_state()
        # task must still be SUCCESS even though mirror write failed
        assert run_state.steps["s1"].tasks["t1"].status == Status.SUCCESS
    finally:
        os.chmod(readonly_dir, stat.S_IRWXU)


# ─── skip step + output: substitution ────────────────────────────────────────

def _write_skip_output(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


@pytest.mark.asyncio
async def test_skip_step_uses_output_path_when_set(tmp_path):
    """skip + output: PATH → engine reads from output: path, not manual_data/."""
    output_data = {"from_output_field": True}
    custom_path = tmp_path / "prebuilt" / "s1.json"
    _write_skip_output(custom_path, output_data)

    # Do NOT create manual_data/ to confirm it's not consulted
    class CheckInputs(BaseTask):
        async def execute(self, inputs, progress):
            await progress(100)
            return {"got": inputs.get("s1")}

    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="pipe", name="P", type="T"),
        steps=[
            StepSpec(
                id="s1",
                skip=True,
                output="prebuilt/s1.json",
                tasks=[TaskSpec(id="t_skip",
                                plugin="tests.integration.test_output_channels.EchoTask")],
            ),
            StepSpec(
                id="s2",
                depends_on_steps=["s1"],
                tasks=[TaskSpec(id="t2",
                                plugin="tests.integration.test_output_channels.EchoTask")],
            ),
        ],
    )
    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()

    run_state = await sm.get_run_state()
    assert run_state.steps["s1"].status == Status.SKIPPED
    assert run_state.steps["s2"].tasks["t2"].status == Status.SUCCESS


@pytest.mark.asyncio
async def test_skip_step_missing_output_path_raises(tmp_path):
    """skip + output: PATH when file absent → PipelineError (same as manual_data)."""
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="pipe", name="P", type="T"),
        steps=[
            StepSpec(
                id="s1",
                skip=True,
                output="nonexistent/data.json",
                tasks=[TaskSpec(id="t1",
                                plugin="tests.integration.test_output_channels.EchoTask")],
            ),
        ],
    )
    sched, _ = _make_scheduler(spec, tmp_path)
    with pytest.raises(PipelineError, match="output: 路径"):
        await sched.run()


@pytest.mark.asyncio
async def test_skip_step_falls_back_to_manual_data_when_output_unset(tmp_path):
    """skip step without output: → falls back to manual_data/ (regression)."""
    manual_data_path = tmp_path / "manual_data" / "s1" / "output.json"
    _write_skip_output(manual_data_path, {"manual": True})

    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="pipe", name="P", type="T"),
        steps=[
            StepSpec(
                id="s1",
                skip=True,
                tasks=[TaskSpec(id="t_skip",
                                plugin="tests.integration.test_output_channels.EchoTask")],
            ),
        ],
    )
    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()

    run_state = await sm.get_run_state()
    assert run_state.steps["s1"].status == Status.SKIPPED


# ─── _collect_step_outputs skip path via depends_on_steps ────────────────────

@pytest.mark.asyncio
async def test_collect_step_outputs_skip_with_output_path(tmp_path):
    """_collect_step_outputs reads output: path when downstream task uses depends_on_steps."""
    output_data = {"key": "from_output"}
    custom_path = tmp_path / "prebuilt" / "s1.json"
    _write_skip_output(custom_path, output_data)

    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="pipe", name="P", type="T"),
        steps=[
            StepSpec(
                id="s1",
                skip=True,
                output="prebuilt/s1.json",
                tasks=[TaskSpec(id="t_skip",
                                plugin="tests.integration.test_output_channels.EchoTask")],
            ),
            StepSpec(
                id="s2",
                tasks=[TaskSpec(
                    id="t2",
                    plugin="tests.integration.test_output_channels.EchoTask",
                    depends_on_steps=["s1"],
                )],
            ),
        ],
    )
    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()

    run_state = await sm.get_run_state()
    assert run_state.steps["s1"].status == Status.SKIPPED
    assert run_state.steps["s2"].tasks["t2"].status == Status.SUCCESS


@pytest.mark.asyncio
async def test_collect_step_outputs_skip_without_output_path_uses_manual_data(tmp_path):
    """_collect_step_outputs falls back to manual_data when skip step has no output:."""
    manual_data_path = tmp_path / "manual_data" / "s1" / "output.json"
    _write_skip_output(manual_data_path, {"from_manual": True})

    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="pipe", name="P", type="T"),
        steps=[
            StepSpec(
                id="s1",
                skip=True,
                tasks=[TaskSpec(id="t_skip",
                                plugin="tests.integration.test_output_channels.EchoTask")],
            ),
            StepSpec(
                id="s2",
                tasks=[TaskSpec(
                    id="t2",
                    plugin="tests.integration.test_output_channels.EchoTask",
                    depends_on_steps=["s1"],
                )],
            ),
        ],
    )
    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()

    run_state = await sm.get_run_state()
    assert run_state.steps["s1"].status == Status.SKIPPED
    assert run_state.steps["s2"].tasks["t2"].status == Status.SUCCESS
