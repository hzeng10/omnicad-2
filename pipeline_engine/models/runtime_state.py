from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class Status(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"    # step-level skip=true
    RECOVERED = "recovered"  # manually supplied via fix --output


class TaskState(BaseModel):
    id: str
    status: Status = Status.PENDING
    progress: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    stack_trace: str | None = None
    input_path: str | None = None
    output_path: str | None = None
    log_path: str | None = None
    recovered_by: str | None = None  # audit trail for fix --output


class StepState(BaseModel):
    id: str
    status: Status = Status.PENDING
    tasks: dict[str, TaskState] = {}
    started_at: datetime | None = None
    finished_at: datetime | None = None


class PipelineRunState(BaseModel):
    pipeline_id: str
    run_id: str
    status: Status = Status.PENDING
    steps: dict[str, StepState] = {}
    started_at: datetime | None = None
    finished_at: datetime | None = None
    workspace: str  # absolute path to the run's root directory
