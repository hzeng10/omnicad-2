"""Tests for DAG validation (task and step graphs)."""
from __future__ import annotations

import pytest

from pipeline_engine.core.dag_validator import (
    validate_pipeline,
    validate_step_dag,
    validate_task_dag,
)
from pipeline_engine.core.errors import PipelineError
from tests.conftest import make_pipeline, make_step, make_task


# ─── task-level tests ────────────────────────────────────────────────────────

def test_single_task_no_deps():
    step = make_step("s", [make_task("t1")])
    gens = validate_task_dag(step)
    assert gens == [["t1"]]


def test_linear_chain():
    t1 = make_task("t1")
    t2 = make_task("t2", depends_on=["t1"])
    t3 = make_task("t3", depends_on=["t2"])
    step = make_step("s", [t1, t2, t3])
    gens = validate_task_dag(step)
    assert gens == [["t1"], ["t2"], ["t3"]]


def test_parallel_tasks():
    t1 = make_task("t1")
    t2 = make_task("t2")
    t3 = make_task("t3")
    step = make_step("s", [t1, t2, t3])
    gens = validate_task_dag(step)
    # all three should be in the same generation
    assert len(gens) == 1
    assert set(gens[0]) == {"t1", "t2", "t3"}


def test_fan_in():
    t1 = make_task("t1")
    t2 = make_task("t2")
    t3 = make_task("t3", depends_on=["t1", "t2"])
    step = make_step("s", [t1, t2, t3])
    gens = validate_task_dag(step)
    assert set(gens[0]) == {"t1", "t2"}
    assert gens[1] == ["t3"]


def test_self_loop_detected():
    t1 = make_task("t1", depends_on=["t1"])
    step = make_step("s", [t1])
    with pytest.raises(PipelineError, match="Cycle"):
        validate_task_dag(step)


def test_long_cycle_detected():
    t1 = make_task("t1", depends_on=["t3"])
    t2 = make_task("t2", depends_on=["t1"])
    t3 = make_task("t3", depends_on=["t2"])
    step = make_step("s", [t1, t2, t3])
    with pytest.raises(PipelineError, match="Cycle"):
        validate_task_dag(step)


def test_depends_on_unknown_task():
    t1 = make_task("t1", depends_on=["ghost"])
    step = make_step("s", [t1])
    with pytest.raises(PipelineError, match="unknown task"):
        validate_task_dag(step)


# ─── step-level tests ─────────────────────────────────────────────────────────

def test_default_step_order():
    steps = [make_step("s1"), make_step("s2"), make_step("s3")]
    spec = make_pipeline(steps=steps)
    gens = validate_step_dag(spec)
    # default linear: each step in its own generation
    assert [gen[0] for gen in gens] == ["s1", "s2", "s3"]


def test_explicit_step_dependency():
    s1 = make_step("s1")
    s2 = make_step("s2", depends_on_steps=["s1"])
    s3 = make_step("s3", depends_on_steps=["s1"])  # s2 and s3 both depend on s1
    spec = make_pipeline(steps=[s1, s2, s3])
    gens = validate_step_dag(spec)
    assert gens[0] == ["s1"]
    assert set(gens[1]) == {"s2", "s3"}


def test_step_depends_on_unknown():
    s1 = make_step("s1", depends_on_steps=["ghost_step"])
    spec = make_pipeline(steps=[s1])
    with pytest.raises(PipelineError, match="unknown step"):
        validate_step_dag(spec)


def test_step_cycle_detected():
    s1 = make_step("s1", depends_on_steps=["s2"])
    s2 = make_step("s2", depends_on_steps=["s1"])
    spec = make_pipeline(steps=[s1, s2])
    with pytest.raises(PipelineError, match="Cycle"):
        validate_step_dag(spec)


def test_validate_pipeline_full():
    t1 = make_task("read")
    t2 = make_task("parse", depends_on=["read"])
    s1 = make_step("ingest", [t1, t2])
    s2 = make_step("process")
    spec = make_pipeline(steps=[s1, s2])
    validate_pipeline(spec)  # should not raise
