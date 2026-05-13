"""Unit tests for _new_run_id format and collision retry logic."""
from __future__ import annotations

import re
from datetime import datetime, timezone
from unittest.mock import patch

from pipeline_engine.core.run_manager import _new_run_id


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
