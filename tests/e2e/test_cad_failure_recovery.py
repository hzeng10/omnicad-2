"""E2E test: PIPELINE_DEMO_FAIL → fix --output → resume → SUCCESS."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

os.environ["PIPELINE_DEMO_FAST"] = "1"

from pipeline_engine.core.run_manager import RunManager
from pipeline_engine.core import storage
from pipeline_engine.models.runtime_state import Status

_PIPELINE_YAML = Path(__file__).parent.parent.parent / "pipelines" / "cad_identify_pipeline" / "pipeline.yaml"
_RECOVER_CABLE = Path(__file__).parent.parent.parent / "pipelines" / "cad_identify_pipeline" / "mock_data" / "recover_cable.json"


@pytest.fixture
def fail_cable(monkeypatch):
    monkeypatch.setenv("PIPELINE_DEMO_FAIL", "rec_cable")
    yield
    monkeypatch.delenv("PIPELINE_DEMO_FAIL", raising=False)


async def test_failure_then_fix_then_resume(tmp_path, fail_cable):
    """rec_cable fails → fix --output → resume → pipeline SUCCESS."""
    rm = RunManager(tmp_path)
    await rm.load(_PIPELINE_YAML)

    run_id = await rm.start_run("cad_identify_cost_estimation")
    ctx = rm._runs[run_id]
    await ctx.main_task  # pipeline finishes (with failure)

    state = await rm.get_run_state(run_id)
    assert state.steps["recognize"].tasks["rec_cable"].status == Status.FAILED

    # fix --output: supply recovered output
    await rm.fix(run_id, "recognize/rec_cable", output_path=str(_RECOVER_CABLE))

    ts = state.steps["recognize"].tasks["rec_cable"]
    ts_fresh = await rm._runs[run_id].state_manager.get_task_state("recognize", "rec_cable")
    assert ts_fresh.status == Status.FIXED
    assert ts_fresh.fixed_by is not None

    # resume
    await rm.resume(run_id)
    ctx2 = rm._runs[run_id]
    await ctx2.main_task

    state2 = await rm.get_run_state(run_id)
    assert state2.status == Status.SUCCESS, f"Expected SUCCESS, got {state2.status}"

    # aggregate must have run
    merge_ts = state2.steps["aggregate"].tasks["merge"]
    assert merge_ts.status == Status.SUCCESS
    output = storage.load_task_output(tmp_path, "cad_identify_cost_estimation", run_id, "aggregate", "merge")
    assert output["grand_total"] > 0
