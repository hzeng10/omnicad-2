"""Tests for shared-file safety: output_mode validation, accumulate semantics,
per-path locking, and BaseTask.shared_json() context manager.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from pipeline_engine.models.pipeline_spec import StepSpec, TaskSpec
from pipeline_engine.core.base_task import BaseTask
from pipeline_engine.core import storage


# ── Part 1: YAML validation ───────────────────────────────────────────────────

def test_duplicate_output_path_without_accumulate_raises():
    """Two tasks sharing an output path without output_mode: accumulate → ValidationError."""
    with pytest.raises(ValidationError, match="output_mode: accumulate"):
        StepSpec(
            id="recognize",
            tasks=[
                TaskSpec(id="t_a", plugin="mod.TaskA", output="results/shared.json"),
                TaskSpec(id="t_b", plugin="mod.TaskB", output="results/shared.json"),
            ],
        )


def test_duplicate_output_path_with_accumulate_is_valid():
    """Two tasks sharing an output path with output_mode: accumulate → loads fine."""
    step = StepSpec(
        id="recognize",
        tasks=[
            TaskSpec(
                id="t_a", plugin="mod.TaskA",
                output="results/shared.json", output_mode="accumulate",
            ),
            TaskSpec(
                id="t_b", plugin="mod.TaskB",
                output="results/shared.json", output_mode="accumulate",
            ),
        ],
    )
    assert step.tasks[0].output_mode == "accumulate"
    assert step.tasks[1].output_mode == "accumulate"


def test_partial_accumulate_raises():
    """Only one of two sharing tasks has accumulate → still raises."""
    with pytest.raises(ValidationError, match="output_mode: accumulate"):
        StepSpec(
            id="s",
            tasks=[
                TaskSpec(
                    id="t_a", plugin="mod.TaskA",
                    output="shared.json", output_mode="accumulate",
                ),
                TaskSpec(id="t_b", plugin="mod.TaskB", output="shared.json"),
            ],
        )


def test_unique_output_paths_always_valid():
    """Two tasks with distinct output paths need no output_mode declaration."""
    step = StepSpec(
        id="s",
        tasks=[
            TaskSpec(id="t_a", plugin="mod.TaskA", output="results/a.json"),
            TaskSpec(id="t_b", plugin="mod.TaskB", output="results/b.json"),
        ],
    )
    assert len(step.tasks) == 2


def test_output_mode_default_is_overwrite():
    t = TaskSpec(id="t1", plugin="mod.Task")
    assert t.output_mode == "overwrite"


# ── Part 2: accumulate semantics (scheduler._do_mirror_write) ─────────────────

@pytest.mark.asyncio
async def test_accumulate_merges_task_outputs(tmp_path):
    """Two sequential accumulate writes produce {task_a: ..., task_b: ...}."""
    from pipeline_engine.core.scheduler import AsyncScheduler
    from unittest.mock import MagicMock, AsyncMock

    dest = tmp_path / "shared.json"

    spec = MagicMock()
    spec.pipeline.id = "pipe"
    spec.pipeline.output = None

    sm = AsyncMock()
    sm._state.run_id = "run_01"

    scheduler = AsyncScheduler(
        spec=spec,
        state_manager=sm,
        workspace=tmp_path,
        abort_event=asyncio.Event(),
        global_semaphore=asyncio.Semaphore(4),
    )

    task_spec_a = MagicMock()
    task_spec_a.output = str(dest)
    task_spec_a.output_mode = "accumulate"

    task_spec_b = MagicMock()
    task_spec_b.output = str(dest)
    task_spec_b.output_mode = "accumulate"

    await scheduler._do_mirror_write("task_a", task_spec_a, {"x": 1})
    await scheduler._do_mirror_write("task_b", task_spec_b, {"x": 2})

    result = json.loads(dest.read_text())
    assert result == {"task_a": {"x": 1}, "task_b": {"x": 2}}


@pytest.mark.asyncio
async def test_overwrite_replaces_file(tmp_path):
    """overwrite mode writes task output directly (last writer wins, by design)."""
    from pipeline_engine.core.scheduler import AsyncScheduler
    from unittest.mock import MagicMock, AsyncMock

    dest = tmp_path / "result.json"

    spec = MagicMock()
    spec.pipeline.id = "pipe"
    spec.pipeline.output = None

    sm = AsyncMock()
    sm._state.run_id = "run_01"

    scheduler = AsyncScheduler(
        spec=spec,
        state_manager=sm,
        workspace=tmp_path,
        abort_event=asyncio.Event(),
        global_semaphore=asyncio.Semaphore(4),
    )

    task_spec = MagicMock()
    task_spec.output = str(dest)
    task_spec.output_mode = "overwrite"

    await scheduler._do_mirror_write("task_a", task_spec, {"v": 1})
    await scheduler._do_mirror_write("task_b", task_spec, {"v": 2})

    result = json.loads(dest.read_text())
    assert result == {"v": 2}


@pytest.mark.asyncio
async def test_accumulate_concurrent_writes_are_serialized(tmp_path):
    """Concurrent accumulate writes via the same lock produce consistent output."""
    from pipeline_engine.core.scheduler import AsyncScheduler
    from unittest.mock import MagicMock, AsyncMock

    dest = tmp_path / "concurrent.json"

    spec = MagicMock()
    spec.pipeline.id = "pipe"
    spec.pipeline.output = None

    sm = AsyncMock()
    sm._state.run_id = "run_01"

    scheduler = AsyncScheduler(
        spec=spec,
        state_manager=sm,
        workspace=tmp_path,
        abort_event=asyncio.Event(),
        global_semaphore=asyncio.Semaphore(10),
    )

    async def write_task(tid: str, val: int) -> None:
        task_spec = MagicMock()
        task_spec.output = str(dest)
        task_spec.output_mode = "accumulate"
        await scheduler._do_mirror_write(tid, task_spec, {"value": val})

    await asyncio.gather(*[write_task(f"task_{i}", i) for i in range(5)])

    result = json.loads(dest.read_text())
    assert set(result.keys()) == {f"task_{i}" for i in range(5)}
    for i in range(5):
        assert result[f"task_{i}"] == {"value": i}


# ── Part 3: BaseTask.shared_json() context manager ────────────────────────────

class _DummyTask(BaseTask):
    async def execute(self, inputs, progress):
        return {}


@pytest.mark.asyncio
async def test_shared_json_creates_file_if_absent(tmp_path):
    """shared_json yields empty dict when the target file doesn't exist yet."""
    task = _DummyTask("t1", {})
    dest = tmp_path / "new.json"

    async with task.shared_json(dest) as data:
        assert data == {}
        data["key"] = "value"

    assert json.loads(dest.read_text()) == {"key": "value"}


@pytest.mark.asyncio
async def test_shared_json_reads_and_merges_existing(tmp_path):
    """shared_json reads existing content and merges correctly."""
    dest = tmp_path / "existing.json"
    storage.atomic_write_json(dest, {"a": 1})

    task = _DummyTask("t1", {})
    async with task.shared_json(dest) as data:
        data["b"] = 2

    result = json.loads(dest.read_text())
    assert result == {"a": 1, "b": 2}


@pytest.mark.asyncio
async def test_shared_json_serializes_concurrent_access(tmp_path):
    """Two tasks using shared_json on the same path don't interleave."""
    dest = tmp_path / "shared.json"

    task_a = _DummyTask("t_a", {})
    task_b = _DummyTask("t_b", {})
    # Share the same lock registry (simulates same run)
    shared_locks: dict = {}
    task_a._path_locks = shared_locks
    task_b._path_locks = shared_locks

    async def do_write(task: BaseTask, key: str, val: int) -> None:
        async with task.shared_json(dest) as data:
            data[key] = val

    await asyncio.gather(do_write(task_a, "a", 1), do_write(task_b, "b", 2))

    result = json.loads(dest.read_text())
    assert result == {"a": 1, "b": 2}
