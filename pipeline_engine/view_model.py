"""Shared presentation layer for CLI JSON output and REPL rendering.

Transparency invariant
----------------------
For status views::

    build_pipeline_status_view(s).model_dump(mode="json") == s.model_dump(mode="json")

For task detail views, the field set is a superset of TaskState: all TaskState fields
are present with identical values, plus the read-through fields (input, output, log_tail).

Both CLI and REPL must go through these builders rather than calling
``state.model_dump()`` or reading task files directly.  This ensures that
any future change to the projection logic is made once and takes effect
in both surfaces.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel

from pipeline_engine.cli_json import read_json_file, read_log_tail
from pipeline_engine.models.runtime_state import (
    PipelineRunState,
    StepState,
    Status,
    TaskState,
)


# ── Summary views (status command / REPL table) ───────────────────────────────

class TaskStatusView(BaseModel):
    """Mirrors TaskState field-for-field; used as the presentation type."""

    id: str
    status: Status = Status.NEW
    progress: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    stack_trace: str | None = None
    input_path: str | None = None
    output_path: str | None = None
    log_path: str | None = None
    fixed_by: str | None = None


class StepStatusView(BaseModel):
    """Mirrors StepState field-for-field."""

    id: str
    status: Status = Status.NEW
    tasks: dict[str, TaskStatusView] = {}
    started_at: datetime | None = None
    finished_at: datetime | None = None


class PipelineStatusView(BaseModel):
    """Mirrors PipelineRunState field-for-field."""

    pipeline_id: str
    run_id: str
    status: Status = Status.NEW
    steps: dict[str, StepStatusView] = {}
    started_at: datetime | None = None
    finished_at: datetime | None = None
    workspace: str


# ── Detail view (inspect --step --task / REPL task detail) ────────────────────

class TaskDetailView(TaskStatusView):
    """Extends TaskStatusView with read-through disk fields.

    Field order: all TaskStatusView (= TaskState) fields first, then the three
    derived fields appended at the end.
    """

    input: Any | None = None
    output: Any | None = None
    log_tail: list[str] = []


# ── Builders ──────────────────────────────────────────────────────────────────

def build_task_status_view(ts: TaskState) -> TaskStatusView:
    return TaskStatusView.model_validate(ts.model_dump())


def build_step_status_view(ss: StepState) -> StepStatusView:
    return StepStatusView.model_validate(ss.model_dump())


def build_pipeline_status_view(state: PipelineRunState) -> PipelineStatusView:
    return PipelineStatusView.model_validate(state.model_dump())


def build_task_detail_view(ts: TaskState, log_tail_size: int = 100) -> TaskDetailView:
    """Build a task detail view with disk read-through fields.

    Args:
        ts: Source TaskState.
        log_tail_size: Number of log lines to include (CLI uses 100, REPL uses 200).
    """
    return TaskDetailView.model_validate({
        **ts.model_dump(),
        "input": read_json_file(ts.input_path),
        "output": read_json_file(ts.output_path),
        "log_tail": read_log_tail(ts.log_path, log_tail_size),
    })
