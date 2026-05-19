"""Tests for backward-compat migration of on-disk state.json (H2) and
terminal-status constant completeness (M2)."""
from __future__ import annotations

from pipeline_engine.models.runtime_state import (
    Status,
    TaskState,
    TERMINAL_PIPELINE_STATUSES,
)


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


# ── M2: TERMINAL_PIPELINE_STATUSES completeness ────────────────────────────────

def test_terminal_pipeline_statuses_covers_all_non_active():
    """M2: TERMINAL_PIPELINE_STATUSES must include every Status that is not NEW/RUNNING."""
    active = {Status.NEW, Status.RUNNING}
    expected = frozenset(s for s in Status if s not in active)
    assert TERMINAL_PIPELINE_STATUSES == expected, (
        f"missing: {expected - TERMINAL_PIPELINE_STATUSES}, "
        f"extra: {TERMINAL_PIPELINE_STATUSES - expected}"
    )


def test_terminal_pipeline_statuses_includes_fixed_and_skipped():
    """M2: FIXED and SKIPPED must be terminal (they were the original missing entries)."""
    assert Status.FIXED in TERMINAL_PIPELINE_STATUSES
    assert Status.SKIPPED in TERMINAL_PIPELINE_STATUSES


def test_events_terminal_set_derived_from_canonical():
    """M2: SSE _TERMINAL_STATUSES must equal the string values of TERMINAL_PIPELINE_STATUSES."""
    from pipeline_engine.api.routers.events import _TERMINAL_STATUSES
    expected = frozenset(s.value for s in TERMINAL_PIPELINE_STATUSES)
    assert _TERMINAL_STATUSES == expected
