from __future__ import annotations

import asyncio
import multiprocessing
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline_engine.core import storage
from pipeline_engine.core.dag_validator import validate_pipeline
from pipeline_engine.core.errors import PipelineError
from pipeline_engine.core.run_context import RunContext
from pipeline_engine.core.scheduler import AsyncScheduler
from pipeline_engine.core.state_manager import StateManager
from pipeline_engine.core.yaml_parser import load_pipeline_spec
from pipeline_engine.models.pipeline_spec import PipelineSpec
from pipeline_engine.models.runtime_state import PipelineRunState, Status


def _new_run_id() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S_%f")


class RunManager:
    """Process-level singleton that coordinates all loaded pipelines and active runs.

    Thread safety: all mutations guarded by asyncio.Lock.
    Multi-run parallelism: each run gets its own asyncio.Task; a shared Semaphore
    caps total concurrent thread-pool workers across all runs.
    """

    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace)
        self._registry: dict[str, PipelineSpec] = {}       # pipeline_id → spec
        self._runs: dict[str, RunContext] = {}              # run_id → ctx
        self._lock = asyncio.Lock()
        cpu = multiprocessing.cpu_count()
        self._global_sem = asyncio.Semaphore(cpu)

    # ─── load ─────────────────────────────────────────────────────────────────

    async def load(self, yaml_path: str | Path) -> str:
        """Parse, validate and register a pipeline YAML. Returns pipeline_id."""
        spec = load_pipeline_spec(yaml_path)
        validate_pipeline(spec)
        async with self._lock:
            self._registry[spec.pipeline.id] = spec
            reg = storage.load_registry(self.workspace)
            reg[spec.pipeline.id] = {
                "yaml_path": str(Path(yaml_path).resolve()),
                "loaded_at": datetime.now(tz=timezone.utc).isoformat(),
                "name": spec.pipeline.name,
            }
            storage.save_registry(self.workspace, reg)
        return spec.pipeline.id

    # ─── run ──────────────────────────────────────────────────────────────────

    async def start_run(
        self,
        pipeline_id: str,
        *,
        step_id: str | None = None,
        task_id: str | None = None,
    ) -> str:
        """Start a new run for pipeline_id in the background. Returns run_id."""
        async with self._lock:
            spec = self._get_spec(pipeline_id)
            run_id = _new_run_id()
            run_dir = storage.init_run_dir(self.workspace, pipeline_id, run_id)
            run_state = PipelineRunState(
                pipeline_id=pipeline_id,
                run_id=run_id,
                workspace=str(run_dir),
            )
            sm = StateManager(run_state)
            abort_event = asyncio.Event()
            sched = AsyncScheduler(spec, sm, self.workspace, abort_event, self._global_sem)
            ctx = RunContext(
                pipeline_spec=spec,
                run_id=run_id,
                workspace=run_dir,
                scheduler=sched,
                state_manager=sm,
                abort_event=abort_event,
            )
            self._runs[run_id] = ctx

        if step_id and task_id:
            coro = ctx.scheduler.run_task(step_id, task_id)
        elif step_id:
            coro = ctx.scheduler.run_step(step_id)
        else:
            coro = ctx.scheduler.run()

        ctx.main_task = asyncio.create_task(coro, name=f"run-{run_id}")
        return run_id

    # ─── stop ─────────────────────────────────────────────────────────────────

    async def stop(
        self,
        ref: str,
        *,
        step_id: str | None = None,
        task_id: str | None = None,
    ) -> None:
        """Trigger orderly abort of a run (or a specific step/task within it)."""
        ctx = self._resolve_run(ref)
        if task_id and step_id:
            await ctx.state_manager.pause_task(step_id, task_id)
        else:
            ctx.abort_event.set()

    # ─── resume ───────────────────────────────────────────────────────────────

    async def resume(self, ref: str, *, include_paused: bool = False) -> str:
        """Resume a failed/paused run.

        Resets eligible tasks to PENDING, creates a new asyncio.Task for the
        same RunContext (preserving run_id), and returns run_id.
        """
        ctx = self._resolve_run(ref)
        sm = ctx.state_manager
        run_state = await sm.get_run_state()

        for step_state in run_state.steps.values():
            for task_id in step_state.tasks:
                await sm.reset_for_resume(step_state.id, task_id, include_paused=include_paused)

        # Reset pipeline-level state to allow re-execution
        async with sm._lock:
            sm._state.status = Status.PENDING
            sm._persist()

        # Create a fresh abort event so this resumed run can be stopped again
        ctx.abort_event = asyncio.Event()
        ctx.scheduler._abort_event = ctx.abort_event

        ctx.main_task = asyncio.create_task(
            ctx.scheduler.run(), name=f"run-{ctx.run_id}-resume"
        )
        return ctx.run_id

    # ─── fix ──────────────────────────────────────────────────────────────────

    async def fix(
        self,
        ref: str,
        task_locator: str,  # "step_id/task_id" or just "task_id" (searched)
        *,
        input_path: str | Path | None = None,
        output_path: str | Path | None = None,
    ) -> None:
        """Manually supply input or output for a failed/stuck task.

        task_locator format: "step_id/task_id"
        """
        ctx = self._resolve_run(ref)
        step_id, task_id = self._parse_task_locator(ctx, task_locator)
        pipeline_id = ctx.pipeline_id
        run_id = ctx.run_id

        if output_path:
            dest = storage.fix_output(
                self.workspace, pipeline_id, run_id, step_id, task_id, output_path
            )
            recovered_by = f"fix-output@{datetime.now(tz=timezone.utc).isoformat()}"
            await ctx.state_manager.recover_task(
                step_id, task_id,
                output_path=str(dest),
                recovered_by=recovered_by,
            )
        elif input_path:
            src = Path(input_path)
            if not src.exists():
                raise PipelineError(f"input file not found: {src}")
            task_dir = storage.init_task_dir(
                self.workspace, pipeline_id, run_id, step_id, task_id
            )
            storage.atomic_write_json(task_dir / "input.json", storage.read_json(src))
            # input fix just resets the task to Pending; user then calls resume
            run_state = await ctx.state_manager.get_run_state()
            ts = run_state.steps.get(step_id, {})
            # Reset status to PENDING so it can be rescheduled
            async with ctx.state_manager._lock:
                task = ctx.state_manager._task(step_id, task_id)
                task.status = Status.PENDING
                task.error = None
                ctx.state_manager._persist()
        else:
            raise PipelineError("fix requires --input or --output")

    # ─── query ────────────────────────────────────────────────────────────────

    def list_pipelines(self) -> list[dict[str, Any]]:
        return [
            {"pipeline_id": pid, "name": spec.pipeline.name}
            for pid, spec in self._registry.items()
        ]

    def list_runs(self) -> list[dict[str, Any]]:
        result = []
        for run_id, ctx in self._runs.items():
            result.append({
                "run_id": run_id,
                "pipeline_id": ctx.pipeline_id,
                "active": ctx.is_active(),
            })
        return result

    async def get_run_state(self, ref: str) -> PipelineRunState:
        ctx = self._resolve_run(ref)
        return await ctx.state_manager.get_run_state()

    # ─── helpers ──────────────────────────────────────────────────────────────

    def _get_spec(self, pipeline_id: str) -> PipelineSpec:
        if pipeline_id not in self._registry:
            raise PipelineError(
                f"pipeline '{pipeline_id}' not loaded — use 'load <path>' first",
                pipeline_id=pipeline_id,
            )
        return self._registry[pipeline_id]

    def restore_runs_from_disk(self) -> None:
        """Reconstruct RunContext objects for all persisted runs found on disk.

        Called by CLI one-shot commands so that stop/resume/fix can address
        runs that were started in a previous process.  Active asyncio Tasks are
        not restored (main_task stays None), which is correct: the original
        process is gone.  State and scheduler are fully reconstructed so that
        fix / resume can work normally.
        """
        runs_root = storage.get_runs_root(self.workspace)
        if not runs_root.exists():
            return
        for pid_dir in runs_root.iterdir():
            if not pid_dir.is_dir() or pid_dir.name == "registry.json":
                continue
            pipeline_id = pid_dir.name
            if pipeline_id not in self._registry:
                continue
            spec = self._registry[pipeline_id]
            for run_dir in pid_dir.iterdir():
                if not run_dir.is_dir():
                    continue
                run_id = run_dir.name
                if run_id in self._runs:
                    continue
                try:
                    run_state = storage.load_state(self.workspace, pipeline_id, run_id)
                except PipelineError:
                    continue
                sm = StateManager(run_state)
                # Demote orphaned RUNNING tasks left by a previous crashed process.
                sm.demote_orphans_sync()
                abort_event = asyncio.Event()
                sched = AsyncScheduler(spec, sm, self.workspace, abort_event, self._global_sem)
                ctx = RunContext(
                    pipeline_spec=spec,
                    run_id=run_id,
                    workspace=run_dir,
                    scheduler=sched,
                    state_manager=sm,
                    abort_event=abort_event,
                )
                self._runs[run_id] = ctx

    def _resolve_run(self, ref: str) -> RunContext:
        """Resolve ref as run_id first, then as pipeline_id."""
        if ref in self._runs:
            return self._runs[ref]
        # Try pipeline_id: must be unambiguous
        matching = [ctx for ctx in self._runs.values() if ctx.pipeline_id == ref]
        if len(matching) == 1:
            return matching[0]
        if len(matching) > 1:
            run_ids = [ctx.run_id for ctx in matching]
            raise PipelineError(
                f"ambiguous: '{ref}' matches multiple active runs {run_ids}. "
                "Please use run_id instead of pipeline_id.",
                pipeline_id=ref,
            )
        raise PipelineError(f"no run found for ref '{ref}'")

    def _parse_task_locator(self, ctx: RunContext, locator: str) -> tuple[str, str]:
        """Parse 'step_id/task_id' or search for task_id across all steps."""
        if "/" in locator:
            parts = locator.split("/", 1)
            return parts[0], parts[1]
        # Search all steps for the task_id
        for step in ctx.pipeline_spec.steps:
            for task in step.tasks:
                if task.id == locator:
                    return step.id, task.id
        raise PipelineError(
            f"task '{locator}' not found in pipeline '{ctx.pipeline_id}'",
            pipeline_id=ctx.pipeline_id,
        )
