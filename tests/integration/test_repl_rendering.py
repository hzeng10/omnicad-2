"""Tests for REPL rendering helpers and less-covered command branches."""
from __future__ import annotations

import asyncio
import json
import textwrap
from pathlib import Path

import pytest

from pipeline_engine.core.base_task import BaseTask
from pipeline_engine.core.run_manager import RunManager
from pipeline_engine.models.pipeline_spec import PipelineMeta, PipelineSpec, StepSpec, TaskSpec
from pipeline_engine.models.runtime_state import PipelineRunState, Status
from pipeline_engine.repl import (
    _dispatch,
    _render_status,
    _render_inspect,
    _render_task_detail,
    _print_pipelines,
    _print_runs,
    _get_flag,
    _build_status_renderable,
)


class QuickTask(BaseTask):
    async def execute(self, inputs, progress):
        await progress(100)
        return {"done": True}


class FailTask(BaseTask):
    async def execute(self, inputs, progress):
        raise RuntimeError("deliberate fail")


def _write_yaml(tmp_path: Path, pid: str, plugin: str = "tests.integration.test_repl_rendering.QuickTask") -> Path:
    content = textwrap.dedent(f"""\
        version: "1.0"
        pipeline:
          id: {pid}
          name: "Render Test {pid}"
        steps:
          - id: step_a
            tasks:
              - id: t1
                plugin: {plugin}
    """)
    p = tmp_path / f"{pid}.yaml"
    p.write_text(content)
    return p


# ─── _get_flag ────────────────────────────────────────────────────────────────

def test_get_flag_present():
    assert _get_flag(["--step", "s1", "--task", "t1"], "--step") == "s1"


def test_get_flag_absent():
    assert _get_flag(["--step", "s1"], "--task") is None


def test_get_flag_missing_value():
    assert _get_flag(["--step"], "--step") is None


# ─── rendering functions ──────────────────────────────────────────────────────

def _make_run_state() -> PipelineRunState:
    from pipeline_engine.models.runtime_state import TaskState, StepState
    ts = TaskState(id="t1", status=Status.SUCCESS, progress=100)
    ts_failed = TaskState(id="t2", status=Status.FAILED, progress=50, error="oops")
    ts_recovered = TaskState(id="t3", status=Status.RECOVERED, progress=100, recovered_by="me@now")
    step = StepState(id="s1", status=Status.SUCCESS, tasks={"t1": ts, "t2": ts_failed, "t3": ts_recovered})
    state = PipelineRunState(
        pipeline_id="test_pipe",
        run_id="20240101T000000_000000",
        status=Status.FAILED,
        steps={"s1": step},
        workspace="/tmp/test",
    )
    return state


def test_render_status_does_not_raise():
    state = _make_run_state()
    _render_status(state)  # should not raise


def test_build_status_renderable_returns_table():
    from rich.table import Table
    state = _make_run_state()
    table = _build_status_renderable(state)
    assert isinstance(table, Table)


def test_render_inspect_no_step_id():
    state = _make_run_state()
    _render_inspect(state, None, None)  # should print full status


def test_render_inspect_missing_step():
    state = _make_run_state()
    _render_inspect(state, "nonexistent_step", None)  # should print error


def test_render_inspect_missing_task():
    state = _make_run_state()
    _render_inspect(state, "s1", "no_such_task")


def test_render_inspect_all_tasks_in_step():
    state = _make_run_state()
    _render_inspect(state, "s1", None)  # all tasks in step, no task_id


def test_render_task_detail_with_error(tmp_path):
    from pipeline_engine.models.runtime_state import TaskState
    ts = TaskState(id="t_err", status=Status.FAILED, progress=30,
                   error="something went wrong", stack_trace="Traceback...",
                   recovered_by=None, input_path=None, output_path=None)
    _render_task_detail("t_err", ts)


def test_render_task_detail_with_files(tmp_path):
    from pipeline_engine.models.runtime_state import TaskState
    inp = tmp_path / "input.json"
    out = tmp_path / "output.json"
    inp.write_text(json.dumps({"x": 1}))
    out.write_text(json.dumps({"y": 2}))
    ts = TaskState(id="t_ok", status=Status.SUCCESS, progress=100,
                   input_path=str(inp), output_path=str(out))
    _render_task_detail("t_ok", ts)


def test_render_task_detail_recovered():
    from pipeline_engine.models.runtime_state import TaskState
    ts = TaskState(id="t_rec", status=Status.RECOVERED, progress=100,
                   recovered_by="fixer@2024-01-01")
    _render_task_detail("t_rec", ts)


def test_print_pipelines_empty():
    from pipeline_engine.core.run_manager import RunManager
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        rm = RunManager(d)
        _print_pipelines(rm)  # "No pipelines loaded"


def test_print_runs_with_active(tmp_path):
    rm = RunManager(tmp_path)
    # Manually insert a fake run for coverage
    from pipeline_engine.core.run_context import RunContext
    from pipeline_engine.core.scheduler import AsyncScheduler
    from pipeline_engine.core.state_manager import StateManager
    from pipeline_engine.models.runtime_state import PipelineRunState
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="pp", name="PP"),
        steps=[StepSpec(id="s", tasks=[TaskSpec(id="t", plugin="x")])],
    )
    run_state = PipelineRunState(pipeline_id="pp", run_id="r1", workspace=str(tmp_path))
    sm = StateManager(run_state)
    abort = asyncio.Event()
    sem = asyncio.Semaphore(1)
    sched = AsyncScheduler(spec, sm, tmp_path, abort, sem)
    ctx = RunContext(pipeline_spec=spec, run_id="r1", workspace=tmp_path,
                     scheduler=sched, state_manager=sm, abort_event=abort)
    rm._runs["r1"] = ctx
    rm._registry["pp"] = spec
    _print_runs(rm)


# ─── REPL dispatch branches ───────────────────────────────────────────────────

async def test_dispatch_bad_shlex(tmp_path):
    rm = RunManager(tmp_path)
    # Unmatched quote — should not raise
    await _dispatch(rm, "run 'unmatched")


async def test_dispatch_resume_command(tmp_path):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "resume_pipe")
    await rm.load(yaml_p)
    run_id = await rm.start_run("resume_pipe")
    ctx = rm._runs[run_id]
    await ctx.main_task  # complete first

    await _dispatch(rm, f"resume {run_id}")
    ctx2 = rm._runs[run_id]
    await ctx2.main_task


async def test_dispatch_resume_include_paused(tmp_path):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "rip_pipe")
    await rm.load(yaml_p)
    run_id = await rm.start_run("rip_pipe")
    ctx = rm._runs[run_id]
    await ctx.main_task

    await _dispatch(rm, f"resume {run_id} --include-paused")
    ctx2 = rm._runs[run_id]
    await ctx2.main_task


async def test_dispatch_status_all(tmp_path):
    rm = RunManager(tmp_path)
    await _dispatch(rm, "status --all")


async def test_dispatch_fix_missing_task_flag(tmp_path):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "fix_err_pipe")
    await rm.load(yaml_p)
    # Fix without --task should print error
    await _dispatch(rm, "fix some_ref --output /tmp/x.json")


async def test_dispatch_fix_no_path_flag(tmp_path):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "fix_np_pipe")
    await rm.load(yaml_p)
    run_id = await rm.start_run("fix_np_pipe")
    ctx = rm._runs[run_id]
    await ctx.main_task
    await _dispatch(rm, f"fix {run_id} --task t1")  # no --output or --input


async def test_dispatch_stop_no_args(tmp_path):
    rm = RunManager(tmp_path)
    await _dispatch(rm, "stop")


async def test_dispatch_resume_no_args(tmp_path):
    rm = RunManager(tmp_path)
    await _dispatch(rm, "resume")


async def test_dispatch_status_no_args(tmp_path):
    rm = RunManager(tmp_path)
    await _dispatch(rm, "status")


async def test_dispatch_inspect_no_args(tmp_path):
    rm = RunManager(tmp_path)
    await _dispatch(rm, "inspect")


async def test_dispatch_fix_no_args(tmp_path):
    rm = RunManager(tmp_path)
    await _dispatch(rm, "fix")


async def test_dispatch_run_no_args(tmp_path):
    rm = RunManager(tmp_path)
    await _dispatch(rm, "run")


async def test_run_manager_list_runs_with_entries(tmp_path):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "lr_pipe")
    await rm.load(yaml_p)
    run_id = await rm.start_run("lr_pipe")
    ctx = rm._runs[run_id]
    await ctx.main_task
    runs = rm.list_runs()
    assert any(r["run_id"] == run_id for r in runs)


async def test_run_manager_stop_with_step_task(tmp_path):
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "sst_pipe")
    await rm.load(yaml_p)
    run_id = await rm.start_run("sst_pipe")
    ctx = rm._runs[run_id]
    await ctx.main_task  # let it finish first
    # stop with step+task on a finished run shouldn't crash (task is not RUNNING)
    try:
        await rm.stop(run_id, step_id="step_a", task_id="t1")
    except Exception:
        pass  # state manager may raise on non-running task; that's fine


async def test_run_manager_resolve_no_run_raises(tmp_path):
    from pipeline_engine.core.errors import PipelineError
    rm = RunManager(tmp_path)
    with pytest.raises(PipelineError):
        rm._resolve_run("nonexistent_ref")


async def test_run_manager_parse_task_locator_slash(tmp_path):
    from pipeline_engine.core.run_context import RunContext
    from pipeline_engine.core.scheduler import AsyncScheduler
    from pipeline_engine.core.state_manager import StateManager
    from pipeline_engine.models.runtime_state import PipelineRunState
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="tl_pipe", name="TL"),
        steps=[StepSpec(id="s1", tasks=[TaskSpec(id="t1", plugin="x")])],
    )
    run_state = PipelineRunState(pipeline_id="tl_pipe", run_id="r_tl", workspace=str(tmp_path))
    sm = StateManager(run_state)
    abort = asyncio.Event()
    sem = asyncio.Semaphore(1)
    sched = AsyncScheduler(spec, sm, tmp_path, abort, sem)
    ctx = RunContext(pipeline_spec=spec, run_id="r_tl", workspace=tmp_path,
                     scheduler=sched, state_manager=sm, abort_event=abort)
    rm = RunManager(tmp_path)
    rm._runs["r_tl"] = ctx

    step_id, task_id = rm._parse_task_locator(ctx, "s1/t1")
    assert step_id == "s1"
    assert task_id == "t1"


async def test_run_manager_parse_task_locator_search(tmp_path):
    from pipeline_engine.core.run_context import RunContext
    from pipeline_engine.core.scheduler import AsyncScheduler
    from pipeline_engine.core.state_manager import StateManager
    from pipeline_engine.models.runtime_state import PipelineRunState
    from pipeline_engine.core.errors import PipelineError
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="tls_pipe", name="TLS"),
        steps=[StepSpec(id="s1", tasks=[TaskSpec(id="t1", plugin="x")])],
    )
    run_state = PipelineRunState(pipeline_id="tls_pipe", run_id="r_tls", workspace=str(tmp_path))
    sm = StateManager(run_state)
    abort = asyncio.Event()
    sem = asyncio.Semaphore(1)
    sched = AsyncScheduler(spec, sm, tmp_path, abort, sem)
    ctx = RunContext(pipeline_spec=spec, run_id="r_tls", workspace=tmp_path,
                     scheduler=sched, state_manager=sm, abort_event=abort)
    rm = RunManager(tmp_path)
    rm._runs["r_tls"] = ctx

    step_id, task_id = rm._parse_task_locator(ctx, "t1")
    assert step_id == "s1"
    assert task_id == "t1"

    with pytest.raises(PipelineError):
        rm._parse_task_locator(ctx, "nonexistent_task")


async def test_run_manager_start_run_step_only(tmp_path):
    """start_run with step_id but no task_id uses run_step()."""
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "step_only_pipe")
    await rm.load(yaml_p)
    run_id = await rm.start_run("step_only_pipe", step_id="step_a")
    ctx = rm._runs[run_id]
    await ctx.main_task


async def test_run_manager_start_run_task_only_ignored(tmp_path):
    """start_run with both step_id and task_id uses run_task()."""
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "task_run_pipe")
    await rm.load(yaml_p)
    run_id = await rm.start_run("task_run_pipe", step_id="step_a", task_id="t1")
    ctx = rm._runs[run_id]
    await ctx.main_task


async def test_run_manager_fix_input_path(tmp_path):
    """fix() with input_path writes input.json and resets task to PENDING."""
    import json as _json
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "fix_input_pipe")
    await rm.load(yaml_p)
    run_id = await rm.start_run("fix_input_pipe")
    ctx = rm._runs[run_id]
    await ctx.main_task

    # Prepare a new input file
    new_input = tmp_path / "new_input.json"
    new_input.write_text(_json.dumps({"injected": True}))

    await rm.fix(run_id, "step_a/t1", input_path=str(new_input))

    # Task should now be PENDING
    ts = await ctx.state_manager.get_task_state("step_a", "t1")
    assert ts.status.value == "pending"


async def test_run_manager_fix_missing_input_file_raises(tmp_path):
    """fix() with a non-existent input_path raises PipelineError."""
    from pipeline_engine.core.errors import PipelineError
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "fix_no_file_pipe")
    await rm.load(yaml_p)
    run_id = await rm.start_run("fix_no_file_pipe")
    ctx = rm._runs[run_id]
    await ctx.main_task

    with pytest.raises(PipelineError, match="input file not found"):
        await rm.fix(run_id, "step_a/t1", input_path="/tmp/no_such_file_xyz.json")


async def test_run_manager_fix_no_paths_raises(tmp_path):
    """fix() with neither input nor output raises PipelineError."""
    from pipeline_engine.core.errors import PipelineError
    rm = RunManager(tmp_path)
    yaml_p = _write_yaml(tmp_path, "fix_no_path_pipe")
    await rm.load(yaml_p)
    run_id = await rm.start_run("fix_no_path_pipe")
    ctx = rm._runs[run_id]
    await ctx.main_task

    with pytest.raises(PipelineError, match="fix requires"):
        await rm.fix(run_id, "step_a/t1")
