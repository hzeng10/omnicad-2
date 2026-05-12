from __future__ import annotations

import asyncio
import traceback
from datetime import datetime, timezone
from typing import Any

from pipeline_engine.core import storage
from pipeline_engine.core.errors import PipelineError
from pipeline_engine.models.runtime_state import (
    PipelineRunState,
    Status,
    StepState,
    TaskState,
)


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


class StateManager:
    """Thread-safe (asyncio.Lock) owner of a single PipelineRunState.

    All mutations go through this class; each mutation persists state.json
    before returning.
    """

    def __init__(self, run_state: PipelineRunState) -> None:
        self._state = run_state
        self._lock = asyncio.Lock()

    # ─── read API ─────────────────────────────────────────────────────────────

    async def get_run_state(self) -> PipelineRunState:
        async with self._lock:
            # Return a deep copy so callers can't mutate internal state
            return self._state.model_copy(deep=True)

    async def get_task_state(self, step_id: str, task_id: str) -> TaskState:
        async with self._lock:
            return self._task(step_id, task_id).model_copy(deep=True)

    async def get_step_state(self, step_id: str) -> StepState:
        async with self._lock:
            return self._step(step_id).model_copy(deep=True)

    # ─── initialisation ───────────────────────────────────────────────────────

    async def init_step(self, step_id: str, task_ids: list[str]) -> None:
        async with self._lock:
            existing = self._state.steps.get(step_id)
            tasks = {}
            for tid in task_ids:
                if existing and tid in existing.tasks:
                    # Preserve terminal states (SUCCESS, RECOVERED, SKIPPED) across re-runs
                    tasks[tid] = existing.tasks[tid]
                else:
                    tasks[tid] = TaskState(id=tid)
            self._state.steps[step_id] = StepState(id=step_id, tasks=tasks)
            self._persist()

    # ─── pipeline-level mutations ─────────────────────────────────────────────

    async def start_pipeline(self) -> None:
        async with self._lock:
            self._state.status = Status.RUNNING
            self._state.started_at = _now()
            self._persist()

    async def finish_pipeline(self, success: bool) -> None:
        async with self._lock:
            self._state.status = Status.SUCCESS if success else Status.FAILED
            self._state.finished_at = _now()
            self._persist()

    # ─── step-level mutations ─────────────────────────────────────────────────

    async def start_step(self, step_id: str) -> None:
        async with self._lock:
            step = self._step(step_id)
            step.status = Status.RUNNING
            step.started_at = _now()
            self._persist()

    async def finish_step(self, step_id: str, success: bool) -> None:
        async with self._lock:
            step = self._step(step_id)
            step.status = Status.SUCCESS if success else Status.FAILED
            step.finished_at = _now()
            self._persist()

    async def skip_step(self, step_id: str) -> None:
        async with self._lock:
            step = self._step(step_id)
            step.status = Status.SKIPPED
            step.started_at = _now()
            step.finished_at = _now()
            self._persist()

    # ─── task-level mutations ─────────────────────────────────────────────────

    async def start_task(self, step_id: str, task_id: str) -> None:
        async with self._lock:
            task = self._task(step_id, task_id)
            task.status = Status.RUNNING
            task.started_at = _now()
            self._persist()

    async def finish_task(
        self,
        step_id: str,
        task_id: str,
        *,
        input_path: str | None = None,
        output_path: str | None = None,
        log_path: str | None = None,
    ) -> None:
        async with self._lock:
            task = self._task(step_id, task_id)
            # Guard: only transition from RUNNING; discard if task was paused/already terminal.
            if task.status != Status.RUNNING:
                return
            task.status = Status.SUCCESS
            task.progress = 100
            task.finished_at = _now()
            if input_path:
                task.input_path = input_path
            if output_path:
                task.output_path = output_path
            if log_path:
                task.log_path = log_path
            self._persist()

    async def fail_task(
        self,
        step_id: str,
        task_id: str,
        error: str,
        exc: BaseException | None = None,
    ) -> None:
        async with self._lock:
            task = self._task(step_id, task_id)
            # Guard: only RUNNING/PENDING tasks can fail; discard if already terminal or paused.
            if task.status not in (Status.RUNNING, Status.PENDING):
                return
            task.status = Status.FAILED
            task.finished_at = _now()
            task.error = error
            if exc is not None:
                task.stack_trace = traceback.format_exc()
            self._persist()

    async def pause_task(self, step_id: str, task_id: str) -> None:
        """Pause a RUNNING or PENDING task (e.g. on abort)."""
        async with self._lock:
            task = self._task(step_id, task_id)
            if task.status in (Status.RUNNING, Status.PENDING):
                task.status = Status.PAUSED
                self._persist()

    async def update_progress(
        self, step_id: str, task_id: str, progress: int
    ) -> None:
        async with self._lock:
            task = self._task(step_id, task_id)
            # Guard: only update progress while task is still running.
            if task.status != Status.RUNNING:
                return
            task.progress = max(0, min(100, progress))
            self._persist()

    async def recover_task(
        self,
        step_id: str,
        task_id: str,
        output_path: str,
        recovered_by: str,
    ) -> None:
        """Mark a task as RECOVERED after fix --output."""
        async with self._lock:
            task = self._task(step_id, task_id)
            task.status = Status.RECOVERED
            task.output_path = output_path
            task.recovered_by = recovered_by
            task.finished_at = _now()
            task.error = None
            task.stack_trace = None
            self._persist()

    async def reset_for_resume(
        self, step_id: str, task_id: str, include_paused: bool = False
    ) -> bool:
        """Reset a Failed (or optionally Paused) task to Pending for rescheduling.

        Returns True if the task was reset.
        """
        async with self._lock:
            task = self._task(step_id, task_id)
            eligible = {Status.FAILED}
            if include_paused:
                eligible.add(Status.PAUSED)
            if task.status in eligible:
                task.status = Status.PENDING
                task.error = None
                task.stack_trace = None
                task.started_at = None
                task.finished_at = None
                task.progress = 0
                self._persist()
                return True
            return False

    def demote_orphans_sync(self) -> None:
        """Reset any RUNNING tasks/steps/pipeline to FAILED (no-lock; safe to call during restore).

        Called by restore_runs_from_disk before the RunContext is registered, so
        no concurrent access is possible yet.
        """
        changed = False
        for step in self._state.steps.values():
            for task in step.tasks.values():
                if task.status == Status.RUNNING:
                    task.status = Status.FAILED
                    task.error = "interrupted: process exited before completion"
                    task.finished_at = _now()
                    changed = True
            if step.status == Status.RUNNING:
                step.status = Status.FAILED
                step.finished_at = _now()
                changed = True
        if self._state.status == Status.RUNNING:
            self._state.status = Status.FAILED
            self._state.finished_at = _now()
            changed = True
        if changed:
            self._persist()

    # ─── helpers ──────────────────────────────────────────────────────────────

    def _step(self, step_id: str) -> StepState:
        try:
            return self._state.steps[step_id]
        except KeyError:
            raise PipelineError(
                f"step '{step_id}' not found in run state",
                pipeline_id=self._state.pipeline_id,
                step_id=step_id,
            )

    def _task(self, step_id: str, task_id: str) -> TaskState:
        step = self._step(step_id)
        try:
            return step.tasks[task_id]
        except KeyError:
            raise PipelineError(
                f"task '{task_id}' not found in step '{step_id}'",
                pipeline_id=self._state.pipeline_id,
                step_id=step_id,
                task_id=task_id,
            )

    def _persist(self) -> None:
        storage.persist_state(self._state)
