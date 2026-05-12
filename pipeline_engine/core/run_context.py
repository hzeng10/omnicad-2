from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pipeline_engine.core.scheduler import AsyncScheduler
    from pipeline_engine.core.state_manager import StateManager
    from pipeline_engine.models.pipeline_spec import PipelineSpec
    from pipeline_engine.models.runtime_state import PipelineRunState


class RunContext:
    """Container for a single pipeline run: scheduler + state + workspace.

    Each run is fully isolated — no shared mutable state between runs.
    """

    def __init__(
        self,
        pipeline_spec: "PipelineSpec",
        run_id: str,
        workspace: Path,
        scheduler: "AsyncScheduler",
        state_manager: "StateManager",
        abort_event: asyncio.Event,
    ) -> None:
        self.pipeline_spec = pipeline_spec
        self.run_id = run_id
        self.workspace = workspace
        self.scheduler = scheduler
        self.state_manager = state_manager
        self.abort_event = abort_event
        self.main_task: asyncio.Task | None = None

    @property
    def pipeline_id(self) -> str:
        return self.pipeline_spec.pipeline.id

    def is_active(self) -> bool:
        return self.main_task is not None and not self.main_task.done()
