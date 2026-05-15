"""Tests for the optional `output` field on TaskSpec / StepSpec / PipelineMeta."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline_engine.models.pipeline_spec import (
    PipelineMeta,
    PipelineSpec,
    StepSpec,
    TaskSpec,
)


# ── defaults ──────────────────────────────────────────────────────────────────

def test_task_output_defaults_none():
    t = TaskSpec(id="t1", plugin="my.pkg.Task")
    assert t.output is None


def test_step_output_defaults_none():
    s = StepSpec(id="s1", tasks=[TaskSpec(id="t1", plugin="my.pkg.Task")])
    assert s.output is None


def test_pipeline_meta_output_defaults_none():
    m = PipelineMeta(id="p1", name="P", type="T")
    assert m.output is None


# ── accepts output at all three levels ────────────────────────────────────────

def test_task_spec_accepts_output():
    t = TaskSpec(id="t1", plugin="my.pkg.Task", output="results/t1.json")
    assert t.output == "results/t1.json"


def test_step_spec_accepts_output():
    s = StepSpec(
        id="s1",
        tasks=[TaskSpec(id="t1", plugin="my.pkg.Task")],
        output="results/s1.json",
    )
    assert s.output == "results/s1.json"


def test_pipeline_meta_accepts_output():
    m = PipelineMeta(id="p1", name="P", type="T", output="results/pipeline.json")
    assert m.output == "results/pipeline.json"


def test_pipeline_spec_accepts_output_at_three_levels():
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(
            id="pipe",
            name="Pipe",
            type="T",
            output="results/pipe.json",
        ),
        steps=[
            StepSpec(
                id="step_a",
                output="results/step_a.json",
                tasks=[
                    TaskSpec(
                        id="t1",
                        plugin="my.pkg.Task",
                        output="results/t1.json",
                    )
                ],
            )
        ],
    )
    assert spec.pipeline.output == "results/pipe.json"
    assert spec.steps[0].output == "results/step_a.json"
    assert spec.steps[0].tasks[0].output == "results/t1.json"


# ── JSON Schema mirror accepts output ─────────────────────────────────────────

def test_json_schema_accepts_output_field():
    """pipeline.schema.json の三箇所にいずれも output プロパティが定義されている。"""
    schema_path = Path(__file__).parents[2] / "config" / "pipeline.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    assert "output" in schema["$defs"]["PipelineMeta"]["properties"]
    assert "output" in schema["$defs"]["StepSpec"]["properties"]
    assert "output" in schema["$defs"]["TaskSpec"]["properties"]


def test_json_schema_output_is_optional():
    """output フィールドが required に含まれていないことを確認。"""
    schema_path = Path(__file__).parents[2] / "config" / "pipeline.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    assert "output" not in schema["$defs"]["PipelineMeta"].get("required", [])
    assert "output" not in schema["$defs"]["StepSpec"].get("required", [])
    assert "output" not in schema["$defs"]["TaskSpec"].get("required", [])


# ── absolute vs relative path: stored verbatim (resolution is storage-side) ──

def test_output_stores_string_verbatim():
    """output フィールドはパス文字列をそのまま保持する（解決はスケジューラ担当）。"""
    t = TaskSpec(id="t1", plugin="my.pkg.Task", output="/abs/path/out.json")
    assert t.output == "/abs/path/out.json"

    t2 = TaskSpec(id="t2", plugin="my.pkg.Task", output="relative/path.json")
    assert t2.output == "relative/path.json"
