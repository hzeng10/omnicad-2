"""Shared fixtures for the test suite."""
from __future__ import annotations

import pytest

from pipeline_engine.models.pipeline_spec import (
    PipelineMeta,
    PipelineSpec,
    StepSpec,
    TaskSpec,
)


def make_task(task_id: str, depends_on: list[str] | None = None, **kw) -> TaskSpec:
    return TaskSpec(id=task_id, plugin="tests.fake.FakeTask", depends_on=depends_on or [], **kw)


def make_step(step_id: str, tasks: list[TaskSpec] | None = None, **kw) -> StepSpec:
    return StepSpec(id=step_id, tasks=tasks or [make_task("t1")], **kw)


def make_pipeline(
    pipeline_id: str = "test_pipeline",
    steps: list[StepSpec] | None = None,
) -> PipelineSpec:
    return PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id=pipeline_id, name="Test Pipeline", type="测试"),
        steps=steps or [make_step("step1")],
    )
