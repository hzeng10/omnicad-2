"""Tests for backward-compat migration of on-disk state.json (H2)."""
from __future__ import annotations

from pipeline_engine.models.runtime_state import Status, TaskState


def test_legacy_pending_status_migrated():
    """Old 'pending' value deserializes to Status.NEW."""
    ts = TaskState.model_validate({"id": "t1", "status": "pending", "progress": 0})
    assert ts.status == Status.NEW


def test_legacy_recovered_status_migrated():
    """Old 'recovered' value deserializes to Status.FIXED."""
    ts = TaskState.model_validate({"id": "t1", "status": "recovered", "progress": 100})
    assert ts.status == Status.FIXED


def test_legacy_recovered_by_renamed():
    """Old 'recovered_by' key is migrated to 'fixed_by'."""
    ts = TaskState.model_validate({
        "id": "t1",
        "status": "recovered",
        "progress": 100,
        "recovered_by": "fix-output@2024-01-01T00:00:00",
    })
    assert ts.fixed_by == "fix-output@2024-01-01T00:00:00"
    assert ts.status == Status.FIXED
