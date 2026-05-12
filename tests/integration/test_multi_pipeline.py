"""Integration tests for multi-pipeline concurrent execution via RunManager."""
from __future__ import annotations

import asyncio
import json
import textwrap
import time
from pathlib import Path

import pytest

from pipeline_engine.core.base_task import BaseTask
from pipeline_engine.core.errors import PipelineError
from pipeline_engine.core.run_manager import RunManager


# ─── stub tasks ───────────────────────────────────────────────────────────────

class SlowTask(BaseTask):
    async def execute(self, inputs, progress):
        await asyncio.sleep(self.config.get("sleep", 0.1))
        await progress(100)
        return {"done": True}


class InstantTask(BaseTask):
    async def execute(self, inputs, progress):
        await progress(100)
        return {"done": True}


# ─── YAML helpers ─────────────────────────────────────────────────────────────

def _write_yaml(tmp_path: Path, pipeline_id: str, sleep: float = 0.2) -> Path:
    content = textwrap.dedent(f"""\
        version: "1.0"
        pipeline:
          id: {pipeline_id}
          name: "Pipeline {pipeline_id}"
        steps:
          - id: step_a
            tasks:
              - id: t1
                plugin: tests.integration.test_multi_pipeline.SlowTask
                config:
                  sleep: {sleep}
    """)
    p = tmp_path / f"{pipeline_id}.yaml"
    p.write_text(content)
    return p


def _write_instant_yaml(tmp_path: Path, pipeline_id: str) -> Path:
    content = textwrap.dedent(f"""\
        version: "1.0"
        pipeline:
          id: {pipeline_id}
          name: "Pipeline {pipeline_id}"
        steps:
          - id: step_a
            tasks:
              - id: t1
                plugin: tests.integration.test_multi_pipeline.InstantTask
    """)
    p = tmp_path / f"{pipeline_id}.yaml"
    p.write_text(content)
    return p


# ─── tests ────────────────────────────────────────────────────────────────────

async def test_two_pipelines_run_concurrently(tmp_path):
    """Two slow pipelines launched together must finish in parallel time."""
    sleep_s = 0.3
    rm = RunManager(tmp_path)

    p1 = _write_yaml(tmp_path, "pipe_a", sleep=sleep_s)
    p2 = _write_yaml(tmp_path, "pipe_b", sleep=sleep_s)
    await rm.load(p1)
    await rm.load(p2)

    start = time.time()
    run1 = await rm.start_run("pipe_a")
    run2 = await rm.start_run("pipe_b")

    # Wait for both runs to finish
    ctx1 = rm._runs[run1]
    ctx2 = rm._runs[run2]
    await asyncio.gather(ctx1.main_task, ctx2.main_task)
    elapsed = time.time() - start

    assert elapsed < sleep_s * 2, f"Expected concurrent execution, got {elapsed:.2f}s"

    from pipeline_engine.models.runtime_state import Status
    s1 = await rm.get_run_state(run1)
    s2 = await rm.get_run_state(run2)
    assert s1.status == Status.SUCCESS
    assert s2.status == Status.SUCCESS


async def test_pipeline_id_resolves_single_run(tmp_path):
    """pipeline_id resolves correctly when there is exactly one active run."""
    rm = RunManager(tmp_path)
    p = _write_instant_yaml(tmp_path, "single_pipe")
    await rm.load(p)
    run_id = await rm.start_run("single_pipe")

    ctx = rm._runs[run_id]
    await ctx.main_task  # wait for completion

    # resolve by pipeline_id
    resolved = rm._resolve_run("single_pipe")
    assert resolved.run_id == run_id


async def test_pipeline_id_ambiguous_with_multiple_runs(tmp_path):
    """pipeline_id raises PipelineError when multiple runs exist for same pipeline."""
    rm = RunManager(tmp_path)
    p = _write_yaml(tmp_path, "dup_pipe", sleep=1.0)
    await rm.load(p)

    run1 = await rm.start_run("dup_pipe")
    run2 = await rm.start_run("dup_pipe")

    with pytest.raises(PipelineError, match="ambiguous"):
        rm._resolve_run("dup_pipe")

    # Cleanup
    rm._runs[run1].abort_event.set()
    rm._runs[run2].abort_event.set()
    await asyncio.gather(rm._runs[run1].main_task, rm._runs[run2].main_task)


async def test_run_id_always_resolves(tmp_path):
    """run_id always resolves directly without ambiguity."""
    rm = RunManager(tmp_path)
    p = _write_instant_yaml(tmp_path, "runid_pipe")
    await rm.load(p)
    run_id = await rm.start_run("runid_pipe")
    ctx = rm._runs[run_id]
    await ctx.main_task

    resolved = rm._resolve_run(run_id)
    assert resolved.run_id == run_id


async def test_load_registers_pipeline(tmp_path):
    rm = RunManager(tmp_path)
    p = _write_instant_yaml(tmp_path, "reg_pipe")
    pid = await rm.load(p)
    assert pid == "reg_pipe"
    assert "reg_pipe" in {pl["pipeline_id"] for pl in rm.list_pipelines()}


async def test_stop_aborts_active_run(tmp_path):
    """stop() sets abort_event; pipeline transitions to a non-running state."""
    rm = RunManager(tmp_path)
    p = _write_yaml(tmp_path, "stoppable_pipe", sleep=2.0)
    await rm.load(p)
    run_id = await rm.start_run("stoppable_pipe")

    await asyncio.sleep(0.05)  # let it start
    await rm.stop(run_id)
    ctx = rm._runs[run_id]
    await ctx.main_task  # drain

    assert ctx.abort_event.is_set()
