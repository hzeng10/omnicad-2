"""End-to-end tests for the CAD cost estimation pipeline example.

All sleeps run at 0.1× speed via PIPELINE_DEMO_FAST=1.
"""
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import pytest

# Force fast mode for CI — must be set before importing task modules
os.environ["PIPELINE_DEMO_FAST"] = "1"
os.environ.pop("PIPELINE_DEMO_FAIL", None)

from pipeline_engine.core.run_manager import RunManager
from pipeline_engine.models.runtime_state import Status

_PIPELINE_YAML = Path(__file__).parent.parent.parent / "examples" / "cad_pipeline" / "pipeline.yaml"


@pytest.fixture
async def rm(tmp_path):
    manager = RunManager(tmp_path)
    await manager.load(_PIPELINE_YAML)
    return manager


async def test_full_pipeline_completes_successfully(rm):
    run_id = await rm.start_run("cad_cost_estimation")
    ctx = rm._runs[run_id]
    await ctx.main_task

    state = await rm.get_run_state(run_id)
    assert state.status == Status.SUCCESS


async def test_all_steps_and_tasks_complete(rm):
    run_id = await rm.start_run("cad_cost_estimation")
    ctx = rm._runs[run_id]
    await ctx.main_task

    state = await rm.get_run_state(run_id)
    assert set(state.steps.keys()) == {"parse_dxf", "split_subgraph", "recognize", "aggregate"}

    for step_id, step_state in state.steps.items():
        for task_id, ts in step_state.tasks.items():
            assert ts.status in (Status.SUCCESS, Status.FIXED, Status.SKIPPED), \
                f"{step_id}/{task_id} ended in {ts.status}"
            assert ts.progress == 100
            assert ts.output_path is not None
            assert Path(ts.output_path).exists()


async def test_recognize_step_parallel_tasks_complete(rm):
    """All three recognize tasks must exist and succeed."""
    run_id = await rm.start_run("cad_cost_estimation")
    ctx = rm._runs[run_id]
    await ctx.main_task

    state = await rm.get_run_state(run_id)
    recognize = state.steps["recognize"]
    assert set(recognize.tasks.keys()) == {"rec_building", "rec_cable", "rec_schematic"}
    for ts in recognize.tasks.values():
        assert ts.status == Status.SUCCESS


async def test_aggregate_output_has_grand_total(rm, tmp_path):
    """Final merge/output.json must contain summary and grand_total."""
    from pipeline_engine.core import storage

    run_id = await rm.start_run("cad_cost_estimation")
    ctx = rm._runs[run_id]
    await ctx.main_task

    output = storage.load_task_output(tmp_path, "cad_cost_estimation", run_id, "aggregate", "merge")
    assert "grand_total" in output
    assert output["grand_total"] > 0
    assert "summary" in output
    assert len(output["summary"]) > 0


async def test_each_task_has_input_and_output_files(rm, tmp_path):
    """input.json and output.json must exist on disk for every task."""
    run_id = await rm.start_run("cad_cost_estimation")
    ctx = rm._runs[run_id]
    await ctx.main_task

    state = await rm.get_run_state(run_id)
    for step_id, step_state in state.steps.items():
        for task_id, ts in step_state.tasks.items():
            assert ts.input_path and Path(ts.input_path).exists(), \
                f"Missing input.json for {step_id}/{task_id}"
            assert ts.output_path and Path(ts.output_path).exists(), \
                f"Missing output.json for {step_id}/{task_id}"
