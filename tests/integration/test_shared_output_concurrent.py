"""Integration test: concurrent parallel tasks writing to the same output file.

Simulates the `recognize` step of `cad_identify_cost_estimation` where 3 recognition
tasks run in parallel and each contributes results to a single shared JSON file.

Scenarios tested:
  1. Three parallel tasks with output_mode: accumulate → all results preserved
  2. Task finish-order doesn't affect completeness (different delays → different order)
  3. shared_json() context manager accumulates a list from parallel tasks
  4. YAML validation blocks same-path tasks without output_mode: accumulate
  5. Single-task overwrite (default) is unchanged by the new locking
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from pipeline_engine.core.base_task import BaseTask
from pipeline_engine.core.scheduler import AsyncScheduler
from pipeline_engine.core.state_manager import StateManager
from pipeline_engine.models.pipeline_spec import (
    PipelineMeta,
    PipelineSpec,
    StepSpec,
    TaskSpec,
)
from pipeline_engine.models.runtime_state import PipelineRunState, Status


# ─── helpers ──────────────────────────────────────────────────────────────────

def _make_scheduler(spec: PipelineSpec, workspace: Path) -> tuple[AsyncScheduler, StateManager]:
    run_dir = workspace / ".pipeline_runs" / spec.pipeline.id / "run1"
    run_dir.mkdir(parents=True)
    run_state = PipelineRunState(
        pipeline_id=spec.pipeline.id,
        run_id="run1",
        workspace=str(run_dir),
    )
    sm = StateManager(run_state)
    return AsyncScheduler(spec, sm, workspace, asyncio.Event(), asyncio.Semaphore(8)), sm


# ─── task implementations ─────────────────────────────────────────────────────

class RecognizerTask(BaseTask):
    """Simulates a CAD recognition task: waits `delay` seconds, returns entities."""

    async def execute(self, inputs, progress):
        delay = self.config.get("delay", 0.01)
        entity_type = self.config["entity_type"]
        count = self.config.get("count", 3)

        # Simulate recognition work
        await asyncio.sleep(delay)
        await progress(50)
        await asyncio.sleep(delay)
        await progress(100)

        return {
            "entity_type": entity_type,
            "count": count,
            "entities": [f"{entity_type}_{i}" for i in range(count)],
        }


class AccumulatorTask(BaseTask):
    """Uses shared_json() to append its result to a shared list file."""

    async def execute(self, inputs, progress):
        delay = self.config.get("delay", 0.01)
        item = self.config["item"]
        shared_path = self.config["shared_path"]

        await asyncio.sleep(delay)
        async with self.shared_json(shared_path) as data:
            data.setdefault("collected", []).append(item)
        await progress(100)
        return {"item": item}


# ─── test 1: three parallel recognizers → accumulate into one file ────────────

@pytest.mark.asyncio
async def test_three_parallel_tasks_accumulate_into_shared_file(tmp_path):
    """
    CAD recognize step: rec_building, rec_cable, rec_schematic all run in parallel.
    Each declares output: results/detections.json + output_mode: accumulate.

    Deliberate delays make them finish in order:
      rec_cable (0.01s × 2) → rec_schematic (0.02s × 2) → rec_building (0.03s × 2)

    Despite different finish order, final file must contain ALL three results.
    """
    shared_path = "results/detections.json"
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="cad_identify", name="CAD Identify", type="CAD图识别及算量",
                               max_parallelism=3),
        steps=[
            StepSpec(
                id="recognize",
                name="多类设备并行识别",
                max_parallelism=3,
                tasks=[
                    TaskSpec(
                        id="rec_building",
                        plugin="tests.integration.test_shared_output_concurrent.RecognizerTask",
                        output=shared_path,
                        output_mode="accumulate",
                        config={"entity_type": "building", "count": 2, "delay": 0.03},
                    ),
                    TaskSpec(
                        id="rec_cable",
                        plugin="tests.integration.test_shared_output_concurrent.RecognizerTask",
                        output=shared_path,
                        output_mode="accumulate",
                        config={"entity_type": "cable", "count": 5, "delay": 0.01},
                    ),
                    TaskSpec(
                        id="rec_schematic",
                        plugin="tests.integration.test_shared_output_concurrent.RecognizerTask",
                        output=shared_path,
                        output_mode="accumulate",
                        config={"entity_type": "schematic", "count": 4, "delay": 0.02},
                    ),
                ],
            )
        ],
    )

    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()

    # ── verify pipeline status ─────────────────────────────────────────────────
    run_state = await sm.get_run_state()
    assert run_state.status == Status.SUCCESS, f"pipeline status: {run_state.status}"
    for task_id in ("rec_building", "rec_cable", "rec_schematic"):
        task_st = run_state.steps["recognize"].tasks[task_id]
        assert task_st.status == Status.SUCCESS, f"{task_id} status: {task_st.status}"

    # ── verify shared output file ──────────────────────────────────────────────
    result_file = tmp_path / "results" / "detections.json"
    assert result_file.exists(), "shared output file was not created"

    result = json.loads(result_file.read_text())
    print(f"\n[accumulate result]\n{json.dumps(result, indent=2)}")

    # All three tasks must be present regardless of finish order
    assert set(result.keys()) == {"rec_building", "rec_cable", "rec_schematic"}, \
        f"expected 3 task keys, got: {list(result.keys())}"

    # Each task's output must be intact
    assert result["rec_building"]["entity_type"] == "building"
    assert result["rec_building"]["count"] == 2
    assert len(result["rec_building"]["entities"]) == 2

    assert result["rec_cable"]["entity_type"] == "cable"
    assert result["rec_cable"]["count"] == 5
    assert len(result["rec_cable"]["entities"]) == 5

    assert result["rec_schematic"]["entity_type"] == "schematic"
    assert result["rec_schematic"]["count"] == 4
    assert len(result["rec_schematic"]["entities"]) == 4


# ─── test 2: finish order doesn't matter (reverse delays) ─────────────────────

@pytest.mark.asyncio
async def test_accumulate_order_independent(tmp_path):
    """Reverse the delay order: building finishes first now. Result must still be complete."""
    shared_path = "results/detections2.json"
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="cad_identify2", name="CAD Identify 2", type="T"),
        steps=[
            StepSpec(
                id="recognize",
                max_parallelism=3,
                tasks=[
                    TaskSpec(
                        id="rec_building",
                        plugin="tests.integration.test_shared_output_concurrent.RecognizerTask",
                        output=shared_path,
                        output_mode="accumulate",
                        config={"entity_type": "building", "count": 1, "delay": 0.01},
                    ),
                    TaskSpec(
                        id="rec_cable",
                        plugin="tests.integration.test_shared_output_concurrent.RecognizerTask",
                        output=shared_path,
                        output_mode="accumulate",
                        config={"entity_type": "cable", "count": 1, "delay": 0.03},
                    ),
                    TaskSpec(
                        id="rec_schematic",
                        plugin="tests.integration.test_shared_output_concurrent.RecognizerTask",
                        output=shared_path,
                        output_mode="accumulate",
                        config={"entity_type": "schematic", "count": 1, "delay": 0.02},
                    ),
                ],
            )
        ],
    )

    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()

    result = json.loads((tmp_path / "results" / "detections2.json").read_text())
    assert set(result.keys()) == {"rec_building", "rec_cable", "rec_schematic"}


# ─── test 3: shared_json() context manager from task code ─────────────────────

@pytest.mark.asyncio
async def test_shared_json_accumulates_list_from_parallel_tasks(tmp_path):
    """
    Three tasks use self.shared_json() to append items to a shared list.
    Each task runs with a different delay — they finish in different orders.
    The final shared file must contain all 3 items.
    """
    shared_file = str(tmp_path / "collector.json")
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="list_pipe", name="List Pipe", type="T"),
        steps=[
            StepSpec(
                id="collect",
                max_parallelism=3,
                tasks=[
                    TaskSpec(
                        id="worker_a",
                        plugin="tests.integration.test_shared_output_concurrent.AccumulatorTask",
                        config={"item": "result_from_A", "shared_path": shared_file, "delay": 0.03},
                    ),
                    TaskSpec(
                        id="worker_b",
                        plugin="tests.integration.test_shared_output_concurrent.AccumulatorTask",
                        config={"item": "result_from_B", "shared_path": shared_file, "delay": 0.01},
                    ),
                    TaskSpec(
                        id="worker_c",
                        plugin="tests.integration.test_shared_output_concurrent.AccumulatorTask",
                        config={"item": "result_from_C", "shared_path": shared_file, "delay": 0.02},
                    ),
                ],
            )
        ],
    )

    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()

    run_state = await sm.get_run_state()
    assert run_state.status == Status.SUCCESS

    result = json.loads(Path(shared_file).read_text())
    print(f"\n[shared_json result]\n{json.dumps(result, indent=2)}")

    collected = result.get("collected", [])
    assert set(collected) == {"result_from_A", "result_from_B", "result_from_C"}, \
        f"expected 3 items, got: {collected}"


# ─── test 4: YAML validation blocks accidental shared path ───────────────────

def test_yaml_validation_blocks_shared_path_without_accumulate():
    """
    Two tasks pointing at the same output path without output_mode: accumulate
    must fail at YAML parse time — before any run is started.
    """
    with pytest.raises(ValidationError, match="output_mode: accumulate"):
        PipelineSpec(
            version="1.0",
            pipeline=PipelineMeta(id="bad_pipe", name="Bad", type="T"),
            steps=[
                StepSpec(
                    id="recognize",
                    tasks=[
                        TaskSpec(
                            id="rec_building",
                            plugin="some.module.Task",
                            output="results/shared.json",
                            # output_mode defaults to "overwrite" — not accumulate
                        ),
                        TaskSpec(
                            id="rec_cable",
                            plugin="some.module.Task",
                            output="results/shared.json",
                            # same path, same default mode → ValidationError
                        ),
                    ],
                )
            ],
        )


# ─── test 5: single-task overwrite is unaffected ─────────────────────────────

@pytest.mark.asyncio
async def test_single_task_overwrite_unchanged(tmp_path):
    """output_mode: overwrite (default) with a single task behaves exactly as before."""
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="solo_pipe", name="Solo", type="T"),
        steps=[
            StepSpec(
                id="s1",
                tasks=[
                    TaskSpec(
                        id="t1",
                        plugin="tests.integration.test_shared_output_concurrent.RecognizerTask",
                        output="results/solo.json",
                        # output_mode defaults to "overwrite"
                        config={"entity_type": "solo", "count": 2, "delay": 0.005},
                    )
                ],
            )
        ],
    )

    sched, sm = _make_scheduler(spec, tmp_path)
    await sched.run()

    result_file = tmp_path / "results" / "solo.json"
    assert result_file.exists()
    result = json.loads(result_file.read_text())
    # overwrite: file contains the task's output directly (not wrapped in {task_id: ...})
    assert result["entity_type"] == "solo"
    assert result["count"] == 2
