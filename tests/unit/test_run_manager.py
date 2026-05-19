"""Unit tests for _new_run_id format and collision retry logic."""
from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from pipeline_engine.core.base_task import BaseTask
from pipeline_engine.core.run_manager import RunManager, _new_run_id
from pipeline_engine.models.pipeline_spec import PipelineMeta, PipelineSpec, StepSpec, TaskSpec
from pipeline_engine.models.runtime_state import Status


# ── stub task used by H9 test ─────────────────────────────────────────────────

class _GatedTask(BaseTask):
    """Blocks until a module-level gate event is set."""
    gate: asyncio.Event | None = None

    async def execute(self, inputs, progress):
        if self.gate:
            await self.gate.wait()
        return {}


def _gated_spec(pipeline_id: str) -> PipelineSpec:
    return PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id=pipeline_id, name="T", type="测试"),
        steps=[StepSpec(id="s", tasks=[
            TaskSpec(id="t", plugin=f"{__name__}._GatedTask"),
        ])],
    )


_FORMAT_RE = re.compile(r"^.+_\d{8}-\d{6}_\d{4}$")


def test_new_run_id_matches_format():
    """run_id must follow <pipeline_id>_yyyyMMdd-HHmmss_4digit pattern."""
    run_id = _new_run_id("my_pipeline")
    assert run_id.startswith("my_pipeline_"), f"unexpected prefix: {run_id}"
    assert _FORMAT_RE.match(run_id), f"format mismatch: {run_id}"


def test_new_run_id_with_underscore_in_pipeline_id():
    """pipeline_id containing underscores must be preserved verbatim."""
    run_id = _new_run_id("cad_identify_cost_estimation")
    assert run_id.startswith("cad_identify_cost_estimation_")
    assert _FORMAT_RE.match(run_id)


def test_new_run_id_suffix_is_four_digits():
    """The trailing suffix must always be exactly 4 decimal digits."""
    run_id = _new_run_id("pipe")
    suffix = run_id.rsplit("_", 1)[-1]
    assert len(suffix) == 4 and suffix.isdigit(), f"bad suffix: {suffix!r}"


def test_new_run_id_uses_utc_timezone():
    """Timestamp portion must reflect UTC, not local time."""
    fixed_utc = datetime(2026, 5, 13, 9, 30, 24, tzinfo=timezone.utc)
    with patch("pipeline_engine.core.run_manager.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_utc
        run_id = _new_run_id("pipe")
    assert "20260513-093024" in run_id, f"UTC timestamp not found in: {run_id}"


def test_new_run_id_uniqueness_on_repeated_calls():
    """Two consecutive calls must very rarely collide (virtually never with 4-digit suffix)."""
    ids = {_new_run_id("pipe") for _ in range(20)}
    # Allow up to 1 collision in 20 tries (astronomically rare but defensively correct)
    assert len(ids) >= 19, f"too many collisions: only {len(ids)} unique IDs in 20"


def test_new_run_id_collision_retry(tmp_path):
    """start_run must retry up to 3 times when run_id collides with an existing run."""
    import asyncio
    from unittest.mock import MagicMock
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.models.pipeline_spec import (
        PipelineMeta, PipelineSpec, StepSpec, TaskSpec,
    )

    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="test_pipe", name="Test", type="测试"),
        steps=[StepSpec(id="s", tasks=[TaskSpec(id="t", plugin="tests.fake.FakeTask")])],
    )

    call_count = 0
    colliding_id = "test_pipe_20260513-093024_0001"
    unique_id    = "test_pipe_20260513-093024_9999"

    def _fake_new_run_id(pid: str) -> str:
        nonlocal call_count
        call_count += 1
        # First two calls return the colliding ID; third call returns unique
        return colliding_id if call_count <= 2 else unique_id

    async def _run():
        rm = RunManager(tmp_path)
        rm._registry["test_pipe"] = spec
        # Pre-populate _runs with the colliding ID
        existing_ctx = MagicMock()
        existing_ctx.pipeline_id = "test_pipe"
        existing_ctx.is_active.return_value = False
        rm._runs[colliding_id] = existing_ctx

        with patch("pipeline_engine.core.run_manager._new_run_id", side_effect=_fake_new_run_id):
            # Should succeed on the 3rd call
            run_id = await rm.start_run("test_pipe")
        return run_id

    result = asyncio.run(_run())
    assert result == unique_id
    assert call_count == 3  # first 2 collide, 3rd succeeds


def test_prune_terminal_runs_evicts_oldest(tmp_path):
    """H1: _prune_terminal_runs removes oldest terminal runs when _runs exceeds _MAX_RUNS."""
    from unittest.mock import MagicMock, patch
    from pipeline_engine.core.run_manager import RunManager, _MAX_RUNS

    rm = RunManager(tmp_path)
    # Populate _runs with (_MAX_RUNS + 5) fake terminal contexts
    for i in range(_MAX_RUNS + 5):
        ctx = MagicMock()
        ctx.is_active.return_value = False  # all terminal
        rm._runs[f"run_{i:04d}"] = ctx

    assert len(rm._runs) == _MAX_RUNS + 5
    rm._prune_terminal_runs()
    # Should evict exactly 5 (oldest, by insertion order)
    assert len(rm._runs) == _MAX_RUNS
    # The 5 evicted IDs were run_0000..run_0004
    for i in range(5):
        assert f"run_{i:04d}" not in rm._runs
    # run_0005 and beyond are retained
    assert f"run_{5:04d}" in rm._runs


# ── H9 tests ──────────────────────────────────────────────────────────────────

async def test_is_active_true_immediately_after_start_run(tmp_path):
    """H9: main_task assigned inside _lock in start_run → is_active() True at once."""
    _GatedTask.gate = asyncio.Event()  # block so run doesn't finish before we check
    rm = RunManager(tmp_path)
    rm._registry["h9_pipe"] = _gated_spec("h9_pipe")

    run_id = await rm.start_run("h9_pipe")
    ctx = rm._runs[run_id]

    # No await between start_run() and this check — task must already be active
    assert ctx.is_active(), "main_task should be active immediately after start_run()"

    # Unblock and wait for clean teardown
    _GatedTask.gate.set()
    await ctx.main_task


async def test_is_active_true_immediately_after_resume(tmp_path):
    """H9: main_task assigned inside _lock in resume() → is_active() True at once."""
    _GatedTask.gate = asyncio.Event()  # block during first run so we can abort it
    rm = RunManager(tmp_path)
    rm._registry["h9_pipe2"] = _gated_spec("h9_pipe2")

    run_id = await rm.start_run("h9_pipe2")
    ctx = rm._runs[run_id]

    # Abort — _GatedTask will see abort_event and be paused
    await rm.stop(run_id)
    _GatedTask.gate.set()  # unblock so the run actually finishes
    await ctx.main_task

    assert not ctx.is_active()

    # Reset gate so the resumed run blocks long enough for us to inspect
    _GatedTask.gate = asyncio.Event()

    await rm.resume(run_id, include_paused=True)

    # No await between resume() and this check
    assert ctx.is_active(), "main_task should be active immediately after resume()"

    # Unblock and wait for clean teardown
    _GatedTask.gate.set()
    await ctx.main_task


# ── H3 tests ──────────────────────────────────────────────────────────────────

async def test_list_instances_returns_snapshot(tmp_path):
    """H3: list_instances result equals the set of runs present at snapshot time."""
    from unittest.mock import AsyncMock, MagicMock

    rm = RunManager(tmp_path)
    N = 10
    for i in range(N):
        ctx = MagicMock()
        ctx.pipeline_id = f"pipe_{i}"
        ms = MagicMock()
        ms.status.value = "success"
        ctx.state_manager.get_run_state = AsyncMock(return_value=ms)
        rm._runs[f"run_{i:04d}"] = ctx

    instances = await rm.list_instances()

    assert len(instances) == N
    ids = {inst["instance_id"] for inst in instances}
    assert ids == {f"run_{i:04d}" for i in range(N)}


async def test_list_instances_no_runtime_error_on_concurrent_mutation(tmp_path):
    """H3: RuntimeError must not occur when _runs is mutated while list_instances runs.

    Before the fix, iterating self._runs.items() with an await yield in the loop
    body allowed start_run() to insert into _runs mid-iteration, causing
    RuntimeError: dictionary changed size during iteration.
    After the fix the snapshot is taken under _lock; mutations only affect _runs,
    not the already-captured list.
    """
    from unittest.mock import AsyncMock, MagicMock

    rm = RunManager(tmp_path)
    N = 30

    for i in range(N):
        ctx = MagicMock()
        ctx.pipeline_id = f"pipe_{i}"
        ms = MagicMock()
        ms.status.value = "running"
        ctx.state_manager.get_run_state = AsyncMock(return_value=ms)
        rm._runs[f"run_{i:04d}"] = ctx

    async def add_more():
        """Simulates concurrent start_run() inserting entries under _lock."""
        for j in range(N, N + 20):
            async with rm._lock:
                extra = MagicMock()
                extra.pipeline_id = "extra_pipe"
                extra.is_active.return_value = False
                ms2 = MagicMock()
                ms2.status.value = "new"
                extra.state_manager.get_run_state = AsyncMock(return_value=ms2)
                rm._runs[f"run_{j:04d}"] = extra

    # Must complete without RuntimeError regardless of interleaving
    result, _ = await asyncio.gather(rm.list_instances(), add_more())

    assert isinstance(result, list)
    assert len(result) >= N  # at minimum the original N runs


# ── L5 tests: restore_runs_from_disk ─────────────────────────────────────────

from pipeline_engine.core import storage
from pipeline_engine.models.runtime_state import PipelineRunState


def _write_run_state(
    workspace: Path,
    pipeline_id: str,
    run_id: str,
    status: Status,
) -> None:
    """Write a minimal state.json to disk so restore_runs_from_disk can load it."""
    run_dir = storage.get_run_dir(workspace, pipeline_id, run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    state = PipelineRunState(
        pipeline_id=pipeline_id,
        run_id=run_id,
        workspace=str(run_dir),
        status=status,
    )
    storage.persist_state(state)


def _simple_spec(pipeline_id: str) -> PipelineSpec:
    return PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id=pipeline_id, name="T", type="测试"),
        steps=[StepSpec(id="s", tasks=[
            TaskSpec(id="t", plugin=f"{__name__}._GatedTask"),
        ])],
    )


def test_restore_loads_all_runs_from_disk(tmp_path):
    """L5: restore_runs_from_disk loads every run directory that has a state.json."""
    pid = "restore_pipe"
    rm = RunManager(tmp_path)
    rm._registry[pid] = _simple_spec(pid)

    for i in range(3):
        _write_run_state(tmp_path, pid, f"run_{i}", Status.SUCCESS)

    rm.restore_runs_from_disk(write_back=False)

    assert len(rm._runs) == 3
    assert {"run_0", "run_1", "run_2"} == set(rm._runs.keys())


def test_restore_skips_unregistered_pipeline(tmp_path):
    """L5: runs for a pipeline_id not in _registry are silently skipped."""
    rm = RunManager(tmp_path)
    # Do NOT register "unknown_pipe"
    _write_run_state(tmp_path, "unknown_pipe", "run_x", Status.SUCCESS)

    rm.restore_runs_from_disk(write_back=False)

    assert len(rm._runs) == 0


def test_restore_skips_missing_state_json(tmp_path):
    """L5: a run directory with no state.json is silently skipped."""
    pid = "nojson_pipe"
    rm = RunManager(tmp_path)
    rm._registry[pid] = _simple_spec(pid)

    # Create the run directory but don't write state.json
    run_dir = storage.get_run_dir(tmp_path, pid, "run_empty")
    run_dir.mkdir(parents=True, exist_ok=True)

    rm.restore_runs_from_disk(write_back=False)

    assert len(rm._runs) == 0


def test_restore_does_not_double_load_existing_run(tmp_path):
    """L5: a run_id already in _runs is not overwritten by restore."""
    from unittest.mock import MagicMock

    pid = "double_pipe"
    rm = RunManager(tmp_path)
    rm._registry[pid] = _simple_spec(pid)
    _write_run_state(tmp_path, pid, "run_dup", Status.PAUSED)

    # Pre-populate _runs with a sentinel ctx
    sentinel = MagicMock()
    rm._runs["run_dup"] = sentinel

    rm.restore_runs_from_disk(write_back=False)

    # Must still be the sentinel — not overwritten
    assert rm._runs["run_dup"] is sentinel


async def test_restore_write_back_demotes_running_orphan(tmp_path):
    """L5: write_back=True calls demote_orphans_sync, turning RUNNING orphans → FAILED."""
    pid = "orphan_pipe"
    rm = RunManager(tmp_path)
    rm._registry[pid] = _simple_spec(pid)

    # Write a state where pipeline status is RUNNING (crashed mid-run)
    run_dir = storage.get_run_dir(tmp_path, pid, "run_orphan")
    run_dir.mkdir(parents=True, exist_ok=True)
    state = PipelineRunState(
        pipeline_id=pid,
        run_id="run_orphan",
        workspace=str(run_dir),
        status=Status.RUNNING,
    )
    storage.persist_state(state)

    rm.restore_runs_from_disk(write_back=True)

    assert "run_orphan" in rm._runs
    restored = await rm._runs["run_orphan"].state_manager.get_run_state()
    assert restored.status == Status.FAILED


async def test_restore_write_back_false_leaves_running_as_is(tmp_path):
    """L5: write_back=False does not modify a RUNNING orphan on disk."""
    pid = "readonly_pipe"
    rm = RunManager(tmp_path)
    rm._registry[pid] = _simple_spec(pid)

    run_dir = storage.get_run_dir(tmp_path, pid, "run_ro")
    run_dir.mkdir(parents=True, exist_ok=True)
    state = PipelineRunState(
        pipeline_id=pid,
        run_id="run_ro",
        workspace=str(run_dir),
        status=Status.RUNNING,
    )
    storage.persist_state(state)

    rm.restore_runs_from_disk(write_back=False)

    assert "run_ro" in rm._runs
    restored = await rm._runs["run_ro"].state_manager.get_run_state()
    # Status must remain RUNNING — write_back=False must not demote
    assert restored.status == Status.RUNNING


async def test_restore_mixed_statuses_all_loaded(tmp_path):
    """L5: SUCCESS, FAILED, PAUSED, NEW runs are all loaded by restore_runs_from_disk."""
    pid = "mixed_pipe"
    rm = RunManager(tmp_path)
    rm._registry[pid] = _simple_spec(pid)

    statuses = {
        "run_success": Status.SUCCESS,
        "run_failed": Status.FAILED,
        "run_paused": Status.PAUSED,
        "run_new": Status.NEW,
    }
    for run_id, st in statuses.items():
        _write_run_state(tmp_path, pid, run_id, st)

    rm.restore_runs_from_disk(write_back=False)

    assert set(rm._runs.keys()) == set(statuses.keys())
    for run_id, expected_st in statuses.items():
        restored = await rm._runs[run_id].state_manager.get_run_state()
        assert restored.status == expected_st, (
            f"{run_id}: expected {expected_st}, got {restored.status}"
        )
