"""Tests for BaseTask interface: dual-entrypoint, progress adapter, I/O validation."""
from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

from pipeline_engine.core.base_task import BaseTask
from pipeline_engine.core.errors import PipelineError


# ─── concrete implementations for testing ────────────────────────────────────

class AsyncTask(BaseTask):
    async def execute(self, inputs, progress):
        await progress(50)
        await progress(100)
        return {"result": inputs.get("x", 0) * 2}


class SyncTask(BaseTask):
    def run_sync(self, inputs, progress):
        progress(50)
        progress(100)
        return {"result": "sync_done"}


class NeitherTask(BaseTask):
    pass  # implements neither execute nor run_sync


class InputModel(BaseModel):
    x: int


class OutputModel(BaseModel):
    result: int


class ValidatedTask(BaseTask):
    InputModel = InputModel
    OutputModel = OutputModel

    async def execute(self, inputs, progress):
        await progress(100)
        return {"result": inputs["x"] + 1}


# ─── tests ────────────────────────────────────────────────────────────────────

async def test_async_task_execute():
    task = AsyncTask("t1", {})
    progress_calls = []

    async def cb(v):
        progress_calls.append(v)

    result = await task.execute({"x": 3}, cb)
    assert result == {"result": 6}
    assert progress_calls == [50, 100]


async def test_sync_task_via_to_thread():
    task = SyncTask("t1", {})
    progress_calls = []

    async def cb(v):
        progress_calls.append(v)

    result = await task.execute({}, cb)
    assert result == {"result": "sync_done"}
    assert progress_calls == [50, 100]


async def test_neither_raises():
    task = NeitherTask("t1", {})
    with pytest.raises(NotImplementedError):
        await task.execute({}, lambda v: None)


async def test_sync_progress_delivered_to_event_loop():
    """The sync adapter must deliver progress back on the event loop."""
    received: list[int] = []

    async def cb(v: int) -> None:
        received.append(v)

    task = SyncTask("t1", {})
    await task.execute({}, cb)
    assert len(received) == 2


async def test_input_validation_passes():
    task = ValidatedTask("t1", {})
    validated = task.validate_input({"x": 5})
    assert validated["x"] == 5


async def test_input_validation_fails():
    task = ValidatedTask("t1", {})
    with pytest.raises(PipelineError, match="input validation failed"):
        task.validate_input({"x": "not_an_int"})


async def test_output_validation_passes():
    task = ValidatedTask("t1", {})
    out = task.validate_output({"result": 7})
    assert out["result"] == 7


async def test_output_validation_fails():
    task = ValidatedTask("t1", {})
    with pytest.raises(PipelineError, match="output validation failed"):
        task.validate_output({"result": "not_an_int"})


def test_config_stored():
    task = AsyncTask("my_task", {"key": "val"})
    assert task.task_id == "my_task"
    assert task.config == {"key": "val"}
