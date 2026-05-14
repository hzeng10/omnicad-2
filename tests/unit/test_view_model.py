"""Tests for pipeline_engine.view_model — transparency invariant and builder API."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipeline_engine.models.runtime_state import (
    PipelineRunState,
    Status,
    StepState,
    TaskState,
)
from pipeline_engine.view_model import (
    TaskDetailView,
    TaskStatusView,
    StepStatusView,
    PipelineStatusView,
    build_task_status_view,
    build_step_status_view,
    build_pipeline_status_view,
    build_task_detail_view,
)


# ── fixtures ──────────────────────────────────────────────────────────────────

def _make_task(task_id: str = "t1", **kwargs) -> TaskState:
    return TaskState(id=task_id, **kwargs)


def _make_step(step_id: str = "s1", tasks: dict | None = None) -> StepState:
    if tasks is None:
        tasks = {"t1": _make_task("t1")}
    return StepState(id=step_id, tasks=tasks)


def _make_run(run_id: str = "pipe_20260101-000000_0000", workspace: str = "/tmp") -> PipelineRunState:
    return PipelineRunState(
        pipeline_id="test_pipe",
        run_id=run_id,
        steps={"s1": _make_step("s1")},
        workspace=workspace,
    )


# ── transparency: TaskStatusView == TaskState ─────────────────────────────────

def test_task_status_view_transparency_simple():
    ts = _make_task("t1")
    view = build_task_status_view(ts)
    assert view.model_dump(mode="json") == ts.model_dump(mode="json")


def test_task_status_view_transparency_full():
    ts = TaskState(
        id="t2",
        status=Status.SUCCESS,
        progress=100,
        started_at=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
        error=None,
        stack_trace=None,
        input_path="/tmp/t2/input.json",
        output_path="/tmp/t2/output.json",
        log_path="/tmp/t2/log.txt",
        fixed_by=None,
    )
    view = build_task_status_view(ts)
    assert view.model_dump(mode="json") == ts.model_dump(mode="json")


def test_task_status_view_transparency_failed():
    ts = TaskState(
        id="t3",
        status=Status.FAILED,
        progress=42,
        error="something broke",
        stack_trace="Traceback:\n  line 1",
        fixed_by="fix-output@2026-01-01",
    )
    view = build_task_status_view(ts)
    assert view.model_dump(mode="json") == ts.model_dump(mode="json")


# ── transparency: StepStatusView == StepState ─────────────────────────────────

def test_step_status_view_transparency():
    ss = _make_step("s1", tasks={
        "t1": _make_task("t1", status=Status.SUCCESS, progress=100),
        "t2": _make_task("t2", status=Status.FAILED, error="oops"),
    })
    view = build_step_status_view(ss)
    assert view.model_dump(mode="json") == ss.model_dump(mode="json")


# ── transparency: PipelineStatusView == PipelineRunState ─────────────────────

def test_pipeline_status_view_transparency():
    state = _make_run()
    view = build_pipeline_status_view(state)
    assert view.model_dump(mode="json") == state.model_dump(mode="json")


def test_pipeline_status_view_transparency_multi_step():
    state = PipelineRunState(
        pipeline_id="multi",
        run_id="multi_20260101-000000_1234",
        status=Status.SUCCESS,
        steps={
            "s1": StepState(
                id="s1",
                status=Status.SUCCESS,
                tasks={"t1": TaskState(id="t1", status=Status.SUCCESS, progress=100)},
            ),
            "s2": StepState(
                id="s2",
                status=Status.FAILED,
                tasks={"t2": TaskState(id="t2", status=Status.FAILED, error="err")},
            ),
        },
        started_at=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc),
        workspace="/tmp/multi",
    )
    view = build_pipeline_status_view(state)
    assert view.model_dump(mode="json") == state.model_dump(mode="json")


# ── TaskDetailView field order ─────────────────────────────────────────────────

def test_task_detail_view_field_order():
    ts = _make_task("t1")
    detail = build_task_detail_view(ts, log_tail_size=100)
    keys = list(detail.model_dump(mode="json").keys())
    # TaskStatusView fields (mirroring TaskState) come first, then derived fields
    assert keys == [
        "id", "status", "progress", "started_at", "finished_at",
        "error", "stack_trace", "input_path", "output_path",
        "log_path", "fixed_by",
        "input", "output", "log_tail",
    ]


# ── TaskDetailView read-through ────────────────────────────────────────────────

def test_task_detail_view_reads_input_output(tmp_path: Path):
    input_p = tmp_path / "input.json"
    output_p = tmp_path / "output.json"
    input_p.write_text('{"x": 1}')
    output_p.write_text('{"y": 2}')

    ts = TaskState(
        id="t1",
        status=Status.SUCCESS,
        progress=100,
        input_path=str(input_p),
        output_path=str(output_p),
    )
    view = build_task_detail_view(ts, log_tail_size=100)
    assert view.input == {"x": 1}
    assert view.output == {"y": 2}


def test_task_detail_view_reads_log_tail(tmp_path: Path):
    log_p = tmp_path / "run.log"
    lines = [f"line {i}" for i in range(20)]
    log_p.write_text("\n".join(lines))

    ts = TaskState(id="t1", log_path=str(log_p))
    view100 = build_task_detail_view(ts, log_tail_size=100)
    assert view100.log_tail == lines  # all 20, since 100 > 20

    view5 = build_task_detail_view(ts, log_tail_size=5)
    assert view5.log_tail == lines[-5:]


def test_task_detail_view_log_tail_size_cli_vs_repl(tmp_path: Path):
    """CLI path uses size=100, REPL path uses size=200."""
    log_p = tmp_path / "run.log"
    log_p.write_text("\n".join([f"L{i}" for i in range(250)]))
    ts = TaskState(id="t1", log_path=str(log_p))

    cli_view = build_task_detail_view(ts, log_tail_size=100)
    repl_view = build_task_detail_view(ts, log_tail_size=200)
    assert len(cli_view.log_tail) == 100
    assert len(repl_view.log_tail) == 200


# ── TaskDetailView handles None paths ─────────────────────────────────────────

def test_task_detail_view_none_paths():
    ts = _make_task("t1")  # all paths are None by default
    view = build_task_detail_view(ts)
    assert view.input is None
    assert view.output is None
    assert view.log_tail == []


def test_task_detail_view_nonexistent_paths(tmp_path: Path):
    ts = TaskState(
        id="t1",
        input_path=str(tmp_path / "nonexistent_input.json"),
        output_path=str(tmp_path / "nonexistent_output.json"),
        log_path=str(tmp_path / "nonexistent.log"),
    )
    view = build_task_detail_view(ts)
    assert view.input is None
    assert view.output is None
    assert view.log_tail == []


# ── TaskDetailView carries TaskState fields untouched ─────────────────────────

def test_task_detail_view_carries_all_task_state_fields():
    ts = TaskState(
        id="t99",
        status=Status.FIXED,
        progress=80,
        error="original error",
        stack_trace="trace here",
        fixed_by="fix-output@ts",
        input_path="/in",
        output_path="/out",
        log_path="/log",
    )
    view = build_task_detail_view(ts)
    assert view.id == "t99"
    assert view.status == Status.FIXED
    assert view.progress == 80
    assert view.error == "original error"
    assert view.stack_trace == "trace here"
    assert view.fixed_by == "fix-output@ts"
    assert view.input_path == "/in"
    assert view.output_path == "/out"
    assert view.log_path == "/log"
