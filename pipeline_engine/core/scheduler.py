from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pipeline_engine.core import storage
from pipeline_engine.core.dag_validator import build_task_graph, build_step_graph
from pipeline_engine.core.errors import PipelineError
from pipeline_engine.core.plugin_loader import instantiate_task
from pipeline_engine.models.runtime_state import Status

if TYPE_CHECKING:
    from pipeline_engine.core.state_manager import StateManager
    from pipeline_engine.models.pipeline_spec import PipelineSpec, StepSpec

logger = logging.getLogger(__name__)


class AsyncScheduler:
    """Drives the execution of a single PipelineRunState.

    Architecture:
    - Iterates steps in topological order.
    - Within each step, dispatches ready tasks (no pending upstream) as asyncio Tasks.
    - Dependency readiness is determined by output.json file existence, not status fields.
    - A process-level Semaphore (injected) caps total concurrent threads across all runs.
    - An abort_event signals orderly shutdown: no new tasks dispatched; in-flight tasks
      are allowed to finish naturally.
    """

    def __init__(
        self,
        spec: "PipelineSpec",
        state_manager: "StateManager",
        workspace: str | Path,
        abort_event: asyncio.Event,
        global_semaphore: asyncio.Semaphore,
    ) -> None:
        self._spec = spec
        self._sm = state_manager
        self._workspace = Path(workspace)
        self._abort_event = abort_event
        self._global_sem = global_semaphore

    @property
    def _pipeline_id(self) -> str:
        return self._spec.pipeline.id

    @property
    def _run_id(self) -> str:
        return self._sm._state.run_id  # type: ignore[attr-defined]

    # ─── public API ───────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Execute the full pipeline end-to-end."""
        await self._sm.start_pipeline()
        try:
            import networkx as nx
            step_graph = build_step_graph(self._spec)
            for step_gen in nx.topological_generations(step_graph):
                for step_id in step_gen:
                    step_spec = self._get_step(step_id)
                    await self._run_step(step_spec)
            # Check overall outcome
            run_state = await self._sm.get_run_state()
            all_ok = all(
                step.status in (Status.SUCCESS, Status.SKIPPED, Status.RECOVERED)
                for step in run_state.steps.values()
            )
            await self._sm.finish_pipeline(success=all_ok)
        except Exception as exc:
            await self._sm.finish_pipeline(success=False)
            raise

    async def run_step(self, step_id: str) -> None:
        """Execute a single step (for 'run --step' mode)."""
        step_spec = self._get_step(step_id)
        await self._run_step(step_spec)

    async def run_task(self, step_id: str, task_id: str) -> None:
        """Execute a single task (for 'run --task' mode)."""
        step_spec = self._get_step(step_id)
        task_spec = next((t for t in step_spec.tasks if t.id == task_id), None)
        if task_spec is None:
            raise PipelineError(
                f"task '{task_id}' not found in step '{step_id}'",
                pipeline_id=self._pipeline_id,
                step_id=step_id,
            )
        await self._sm.init_step(step_id, [task_id])
        await self._dispatch_task(step_spec, task_spec)

    # ─── step execution ───────────────────────────────────────────────────────

    async def _run_step(self, step: "StepSpec") -> None:
        task_ids = [t.id for t in step.tasks]
        await self._sm.init_step(step.id, task_ids)

        # If abort was already set when we reach this step, pause all its tasks
        if self._abort_event.is_set():
            for tid in task_ids:
                await self._sm.pause_task(step.id, tid)
            await self._sm.finish_step(step.id, success=False)
            return

        if step.skip:
            await self._handle_skip(step)
            return

        await self._sm.start_step(step.id)
        step_sem = asyncio.Semaphore(
            step.max_parallelism or self._spec.pipeline.max_parallelism
        )
        import networkx as nx
        task_graph = build_task_graph(step)

        # Tasks to skip: only RECOVERED (via fix --output) and SKIPPED (manual data).
        # SUCCESS tasks are re-run so they can consume corrected upstream outputs.
        _skip_statuses = (Status.RECOVERED, Status.SKIPPED)
        pre_state = await self._sm.get_run_state()
        pre_step = pre_state.steps.get(step.id)
        already_done: set[str] = {
            t.id for t in step.tasks
            if pre_step and pre_step.tasks.get(t.id) and pre_step.tasks[t.id].status in _skip_statuses
        }

        # Track completion events per task so downstream can wait
        completion_events: dict[str, asyncio.Event] = {
            t.id: asyncio.Event() for t in step.tasks
        }

        async def run_with_sem(task_spec) -> None:
            # Wait for all within-step dependencies to complete
            for dep_id in task_spec.depends_on:
                await completion_events[dep_id].wait()

            if self._abort_event.is_set():
                await self._sm.pause_task(step.id, task_spec.id)
                completion_events[task_spec.id].set()
                return

            # Skip tasks already in a good terminal state (RECOVERED via fix, or previous SUCCESS)
            if task_spec.id in already_done:
                completion_events[task_spec.id].set()
                return

            async with step_sem:
                async with self._global_sem:
                    await self._dispatch_task(step, task_spec)
            completion_events[task_spec.id].set()

        await asyncio.gather(*[run_with_sem(t) for t in step.tasks])

        run_state = await self._sm.get_run_state()
        step_state = run_state.steps[step.id]
        success = all(
            ts.status in (Status.SUCCESS, Status.RECOVERED)
            for ts in step_state.tasks.values()
        )
        await self._sm.finish_step(step.id, success=success)

    async def _handle_skip(self, step: "StepSpec") -> None:
        """For skip=true steps: load manual_data and mark as SKIPPED."""
        try:
            storage.load_manual_data(self._workspace, step.id)
        except PipelineError:
            raise PipelineError(
                f"step '{step.id}' is marked skip=true but "
                f"manual_data/{step.id}/output.json is missing",
                pipeline_id=self._pipeline_id,
                step_id=step.id,
            )
        await self._sm.skip_step(step.id)

    # ─── task execution ───────────────────────────────────────────────────────

    async def _dispatch_task(self, step: "StepSpec", task_spec) -> None:
        step_id = step.id
        task_id = task_spec.id

        inputs = await self._build_inputs(step, task_spec)
        task_dir = storage.init_task_dir(
            self._workspace, self._pipeline_id, self._run_id, step_id, task_id
        )
        input_path = task_dir / "input.json"
        output_path = task_dir / "output.json"
        log_path = task_dir / "log.txt"

        storage.atomic_write_json(input_path, inputs)
        await self._sm.start_task(step_id, task_id)

        log_lines: list[str] = []

        async def progress_cb(value: int) -> None:
            await self._sm.update_progress(step_id, task_id, value)

        try:
            task_instance = instantiate_task(task_spec.plugin, task_id, task_spec.config)
            validated_inputs = task_instance.validate_input(inputs)
            output = await task_instance.execute(validated_inputs, progress_cb)
            validated_output = task_instance.validate_output(output)
            storage.atomic_write_json(output_path, validated_output)
            await self._sm.finish_task(
                step_id,
                task_id,
                input_path=str(input_path),
                output_path=str(output_path),
                log_path=str(log_path) if log_path.exists() else None,
            )
        except PipelineError:
            raise
        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}"
            await self._sm.fail_task(step_id, task_id, error=error_msg, exc=exc)
            logger.error("Task %s/%s failed: %s", step_id, task_id, error_msg)

    # ─── input assembly ───────────────────────────────────────────────────────

    async def _build_inputs(self, step: "StepSpec", task_spec) -> dict[str, Any]:
        """Merge static inputs + within-step depends_on outputs + cross-step depends_on_steps outputs."""
        inputs: dict[str, Any] = dict(task_spec.inputs)

        # Within-step task dependencies
        for dep_task_id in task_spec.depends_on:
            if storage.task_output_exists(
                self._workspace, self._pipeline_id, self._run_id, step.id, dep_task_id
            ):
                inputs[dep_task_id] = storage.load_task_output(
                    self._workspace, self._pipeline_id, self._run_id, step.id, dep_task_id
                )

        # Cross-step dependencies
        for dep_step_id in task_spec.depends_on_steps:
            step_outputs = self._collect_step_outputs(dep_step_id)
            if step_outputs:
                inputs[dep_step_id] = step_outputs

        return inputs

    def _collect_step_outputs(self, step_id: str) -> dict[str, Any]:
        """Collect outputs from a completed step.

        For skip=true steps the manual_data dict is returned directly (no task
        output files exist on disk).  For normal steps, leaf-task output.json
        files are aggregated by task_id.
        """
        dep_step_spec = self._get_step(step_id)

        if dep_step_spec.skip:
            try:
                return storage.load_manual_data(self._workspace, step_id)
            except PipelineError:
                return {}

        import networkx as nx
        g = build_task_graph(dep_step_spec)
        # Leaf tasks: nodes with no outgoing edges
        leaf_ids = [n for n in g.nodes if g.out_degree(n) == 0]
        if not leaf_ids:
            leaf_ids = [t.id for t in dep_step_spec.tasks]

        result: dict[str, Any] = {}
        for tid in leaf_ids:
            if storage.task_output_exists(
                self._workspace, self._pipeline_id, self._run_id, step_id, tid
            ):
                result[tid] = storage.load_task_output(
                    self._workspace, self._pipeline_id, self._run_id, step_id, tid
                )
        return result

    def _get_step(self, step_id: str):
        for s in self._spec.steps:
            if s.id == step_id:
                return s
        raise PipelineError(
            f"step '{step_id}' not found in pipeline",
            pipeline_id=self._pipeline_id,
        )
