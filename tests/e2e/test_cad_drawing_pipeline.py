"""E2E tests for pipelines/cad_drawing_pipeline.

Covers:
- Happy path: all tasks succeed, refine_drawing step is skipped via manual_data
- Missing manual_data: skip step raises PipelineError
- PIPELINE_DEMO_FAIL=validate_dxf → fix --output → resume → SUCCESS
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
from pathlib import Path

import pytest

os.environ["PIPELINE_DEMO_FAST"] = "1"
os.environ.pop("PIPELINE_DEMO_FAIL", None)

from pipeline_engine.core.run_manager import RunManager
from pipeline_engine.core import storage
from pipeline_engine.core.errors import PipelineError
from pipeline_engine.models.runtime_state import Status

_PIPELINE_YAML = (
    Path(__file__).parent.parent.parent
    / "pipelines" / "cad_drawing_pipeline" / "pipeline.yaml"
)
_MOCK_DATA_DIR = (
    Path(__file__).parent.parent.parent
    / "pipelines" / "cad_drawing_pipeline" / "mock_data"
)


def _setup_manual_data(workspace: Path) -> None:
    """Copy pre-baked refine_drawing output.json into workspace/manual_data/."""
    src = _MOCK_DATA_DIR / "refine_drawing" / "output.json"
    dest = workspace / "manual_data" / "refine_drawing" / "output.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(src, dest)


@pytest.fixture
async def rm(tmp_path):
    _setup_manual_data(tmp_path)
    manager = RunManager(tmp_path)
    await manager.load(_PIPELINE_YAML)
    return manager


# ─── Happy path ───────────────────────────────────────────────────────────────

async def test_cad_drawing_happy_path(rm):
    """Full pipeline runs successfully; refine_drawing is SKIPPED via manual_data."""
    run_id = await rm.start_run("cad_drawing_pipeline")
    ctx = rm._runs[run_id]
    await ctx.main_task

    state = await rm.get_run_state(run_id)
    assert state.status == Status.SUCCESS

    refine_step = state.steps["refine_drawing"]
    assert refine_step.status == Status.SKIPPED


async def test_all_non_skipped_tasks_succeed(rm):
    run_id = await rm.start_run("cad_drawing_pipeline")
    ctx = rm._runs[run_id]
    await ctx.main_task

    state = await rm.get_run_state(run_id)
    for step_id, step_state in state.steps.items():
        if step_state.status == Status.SKIPPED:
            continue
        for task_id, ts in step_state.tasks.items():
            assert ts.status == Status.SUCCESS, \
                f"{step_id}/{task_id} ended in {ts.status}"


async def test_layout_tasks_run_in_parallel(rm):
    """gen_floor and gen_electrical are independent; both must complete."""
    run_id = await rm.start_run("cad_drawing_pipeline")
    ctx = rm._runs[run_id]
    await ctx.main_task

    state = await rm.get_run_state(run_id)
    gen_step = state.steps["generate_layout"]
    assert gen_step.tasks["gen_floor"].status == Status.SUCCESS
    assert gen_step.tasks["gen_electrical"].status == Status.SUCCESS


async def test_validate_output_written_to_disk(rm, tmp_path):
    """ValidateDXF output.json must exist on disk and contain is_valid=true."""
    run_id = await rm.start_run("cad_drawing_pipeline")
    ctx = rm._runs[run_id]
    await ctx.main_task

    output = storage.load_task_output(
        tmp_path, "cad_drawing_pipeline", run_id, "export_dxf", "validate"
    )
    assert output["is_valid"] is True
    assert "entity_count_positive" in output["checked_rules"]


# ─── Skip step: missing manual_data ──────────────────────────────────────────

async def test_missing_manual_data_raises(tmp_path):
    """If manual_data/refine_drawing/output.json is absent, pipeline raises PipelineError."""
    # Intentionally do NOT call _setup_manual_data
    manager = RunManager(tmp_path)
    await manager.load(_PIPELINE_YAML)

    run_id = await manager.start_run("cad_drawing_pipeline")
    ctx = manager._runs[run_id]
    with pytest.raises(PipelineError, match="manual_data"):
        await ctx.main_task


# ─── PIPELINE_DEMO_FAIL → fix → resume ───────────────────────────────────────

async def test_validate_fail_then_fix_then_resume(tmp_path, monkeypatch):
    """validate_dxf fails → fix --output injects result → resume → SUCCESS."""
    monkeypatch.setenv("PIPELINE_DEMO_FAIL", "validate_dxf")
    _setup_manual_data(tmp_path)

    manager = RunManager(tmp_path)
    await manager.load(_PIPELINE_YAML)

    run_id = await manager.start_run("cad_drawing_pipeline")
    ctx = manager._runs[run_id]
    await ctx.main_task

    state = await manager.get_run_state(run_id)
    assert state.steps["export_dxf"].tasks["validate"].status == Status.FAILED

    # Clear the env var so that after fix, resume will succeed
    monkeypatch.delenv("PIPELINE_DEMO_FAIL")

    # Provide a recovered output file
    recovered = tmp_path / "recovered_validation.json"
    recovered.write_text(json.dumps({
        "is_valid": True,
        "checked_rules": ["manually_verified"],
        "issues": [],
    }))

    await manager.fix(run_id, "export_dxf/validate", output_path=str(recovered))
    ts = await manager._runs[run_id].state_manager.get_task_state("export_dxf", "validate")
    assert ts.status == Status.FIXED

    await manager.resume(run_id)
    ctx2 = manager._runs[run_id]
    await ctx2.main_task

    state2 = await manager.get_run_state(run_id)
    assert state2.status == Status.SUCCESS, f"Expected SUCCESS, got {state2.status}"
