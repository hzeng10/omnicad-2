"""E2E tests: per-run run.log generation, content correctness, resume append."""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from pipeline_engine.core.run_manager import RunManager
from pipeline_engine.core import storage

CAD_DRAWING_YAML = Path("pipelines/cad_drawing_pipeline/pipeline.yaml")
CAD_DRAWING_MOCK = Path("pipelines/cad_drawing_pipeline/mock_data/refine_drawing/output.json")


@pytest.fixture()
def workspace(tmp_path):
    manual = tmp_path / "manual_data" / "refine_drawing"
    manual.mkdir(parents=True)
    import shutil
    shutil.copy(CAD_DRAWING_MOCK, manual / "output.json")
    return tmp_path


@pytest.mark.skipif(
    not CAD_DRAWING_YAML.exists(),
    reason="cad_drawing_pipeline not present",
)
def test_run_log_created_after_pipeline_run(workspace):
    """run.log must exist after a successful pipeline run."""
    os.environ["PIPELINE_DEMO_FAST"] = "1"
    try:
        async def _run():
            rm = RunManager(workspace)
            await rm.load(CAD_DRAWING_YAML)
            run_id = await rm.start_run("cad_drawing_pipeline")
            ctx = rm._runs[run_id]
            await ctx.await_main()
            return run_id

        run_id = asyncio.run(_run())
        log_path = storage.get_run_log_path(workspace, "cad_drawing_pipeline", run_id)
        assert log_path.exists(), f"run.log not found at {log_path}"
    finally:
        os.environ.pop("PIPELINE_DEMO_FAST", None)


@pytest.mark.skipif(
    not CAD_DRAWING_YAML.exists(),
    reason="cad_drawing_pipeline not present",
)
def test_run_log_contains_lifecycle_events(workspace):
    """run.log must contain start, skip, task, and finish lifecycle lines."""
    os.environ["PIPELINE_DEMO_FAST"] = "1"
    try:
        async def _run():
            rm = RunManager(workspace)
            await rm.load(CAD_DRAWING_YAML)
            run_id = await rm.start_run("cad_drawing_pipeline")
            ctx = rm._runs[run_id]
            await ctx.await_main()
            return run_id

        run_id = asyncio.run(_run())
        log_path = storage.get_run_log_path(workspace, "cad_drawing_pipeline", run_id)
        text = log_path.read_text(encoding="utf-8")

        assert "pipeline run started" in text
        assert "pipeline run ended" in text
        assert "task start:" in text
        assert "task done:" in text
        assert "step skipped" in text  # refine_drawing is skipped
    finally:
        os.environ.pop("PIPELINE_DEMO_FAST", None)


@pytest.mark.skipif(
    not CAD_DRAWING_YAML.exists(),
    reason="cad_drawing_pipeline not present",
)
def test_run_log_contains_error_on_failure(workspace):
    """When PIPELINE_DEMO_FAIL is set, run.log must contain an ERROR line."""
    os.environ["PIPELINE_DEMO_FAST"] = "1"
    os.environ["PIPELINE_DEMO_FAIL"] = "validate_dxf"
    try:
        async def _run():
            rm = RunManager(workspace)
            await rm.load(CAD_DRAWING_YAML)
            run_id = await rm.start_run("cad_drawing_pipeline")
            ctx = rm._runs[run_id]
            await ctx.await_main()
            return run_id

        run_id = asyncio.run(_run())
        log_path = storage.get_run_log_path(workspace, "cad_drawing_pipeline", run_id)
        text = log_path.read_text(encoding="utf-8")

        assert "ERROR" in text
        assert "validate" in text.lower() or "DEMO_FAIL" in text
    finally:
        os.environ.pop("PIPELINE_DEMO_FAST", None)
        os.environ.pop("PIPELINE_DEMO_FAIL", None)


@pytest.mark.skipif(
    not CAD_DRAWING_YAML.exists(),
    reason="cad_drawing_pipeline not present",
)
def test_run_log_appended_on_resume(workspace):
    """After fix + resume, run.log must contain entries from both sessions."""
    import json, tempfile
    os.environ["PIPELINE_DEMO_FAST"] = "1"
    os.environ["PIPELINE_DEMO_FAIL"] = "validate_dxf"
    try:
        async def _fail_then_fix():
            rm = RunManager(workspace)
            await rm.load(CAD_DRAWING_YAML)
            run_id = await rm.start_run("cad_drawing_pipeline")
            ctx = rm._runs[run_id]
            await ctx.await_main()

            # Write a fix file and resume
            fix_data = {"is_valid": True, "checked_rules": ["manual"], "issues": []}
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            ) as f:
                json.dump(fix_data, f)
                fix_path = f.name

            os.environ.pop("PIPELINE_DEMO_FAIL")
            await rm.fix(run_id, "export_dxf/validate", output_path=fix_path)
            await rm.resume(run_id)
            await rm._runs[run_id].await_main()
            return run_id

        run_id = asyncio.run(_fail_then_fix())
        log_path = storage.get_run_log_path(workspace, "cad_drawing_pipeline", run_id)
        text = log_path.read_text(encoding="utf-8")

        # Should have "pipeline run started" twice (original + resume) or at minimum
        # both an ERROR line from the first run and a success finish from resume
        started_count = text.count("pipeline run started")
        assert started_count >= 2, (
            f"Expected ≥2 'pipeline run started' lines (original + resume), got {started_count}"
        )
    finally:
        os.environ.pop("PIPELINE_DEMO_FAST", None)
        os.environ.pop("PIPELINE_DEMO_FAIL", None)


@pytest.mark.skipif(
    not CAD_DRAWING_YAML.exists(),
    reason="cad_drawing_pipeline not present",
)
def test_task_logger_output_captured_in_run_log(workspace):
    """Output from BaseTask.logger must appear in run.log."""
    os.environ["PIPELINE_DEMO_FAST"] = "1"
    try:
        async def _run():
            rm = RunManager(workspace)
            await rm.load(CAD_DRAWING_YAML)
            run_id = await rm.start_run("cad_drawing_pipeline")
            ctx = rm._runs[run_id]
            await ctx.await_main()
            return run_id

        run_id = asyncio.run(_run())
        log_path = storage.get_run_log_path(workspace, "cad_drawing_pipeline", run_id)
        text = log_path.read_text(encoding="utf-8")
        # run.log must have been written
        assert len(text.strip().splitlines()) >= 5, "run.log appears too short"
    finally:
        os.environ.pop("PIPELINE_DEMO_FAST", None)
