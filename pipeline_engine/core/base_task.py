from __future__ import annotations

import asyncio
import concurrent.futures
from abc import ABC
from typing import Any, Awaitable, Callable

from pipeline_engine.core.errors import PipelineError

# Async progress callback: caller awaits progress(int)
ProgressCallback = Callable[[int], Awaitable[None]]


class _SyncProgressAdapter:
    """Wraps an async ProgressCallback for use from a thread-pool worker.

    Uses asyncio.run_coroutine_threadsafe to post the coroutine back onto the
    event loop that owns the callback, then blocks until it completes.
    """

    def __init__(self, async_cb: ProgressCallback, loop: asyncio.AbstractEventLoop) -> None:
        self._cb = async_cb
        self._loop = loop

    def __call__(self, value: int) -> None:
        future = asyncio.run_coroutine_threadsafe(self._cb(value), self._loop)
        future.result()  # block the worker thread until the coroutine finishes


class BaseTask(ABC):
    """Abstract base for all user-defined tasks.

    Subclass either:
    - async def execute(self, inputs, progress) -> dict  — for async/IO-bound work
    - def run_sync(self, inputs, progress) -> dict       — for CPU-bound / legacy sync work
      (engine auto-wraps via asyncio.to_thread)
    """

    # Subclasses may set these to Pydantic model classes for automatic I/O validation
    InputModel: type | None = None
    OutputModel: type | None = None

    def __init__(self, task_id: str, config: dict[str, Any]) -> None:
        self.task_id = task_id
        self.config = config

    async def execute(self, inputs: dict[str, Any], progress: ProgressCallback) -> dict[str, Any]:
        """Default: if run_sync is overridden, delegate to it via asyncio.to_thread."""
        if type(self).run_sync is not BaseTask.run_sync:
            loop = asyncio.get_running_loop()
            adapter = _SyncProgressAdapter(progress, loop)
            return await asyncio.to_thread(self.run_sync, inputs, adapter)
        raise NotImplementedError(
            f"{type(self).__name__} must implement execute() or run_sync()"
        )

    def run_sync(self, inputs: dict[str, Any], progress: Any) -> dict[str, Any]:
        """Override for CPU-bound tasks; called from a thread pool by execute()."""
        raise NotImplementedError

    def validate_input(self, inputs: dict[str, Any]) -> dict[str, Any]:
        """Validate inputs against InputModel if declared."""
        if self.InputModel is None:
            return inputs
        try:
            return self.InputModel.model_validate(inputs).model_dump()
        except Exception as exc:
            raise PipelineError(
                f"input validation failed for task '{self.task_id}': {exc}",
                task_id=self.task_id,
            ) from exc

    def validate_output(self, output: dict[str, Any]) -> dict[str, Any]:
        """Validate output against OutputModel if declared."""
        if self.OutputModel is None:
            return output
        try:
            return self.OutputModel.model_validate(output).model_dump()
        except Exception as exc:
            raise PipelineError(
                f"output validation failed for task '{self.task_id}': {exc}",
                task_id=self.task_id,
            ) from exc
