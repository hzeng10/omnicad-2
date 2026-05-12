"""Tests for YAML parsing and schema validation."""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from pipeline_engine.core.errors import PipelineError
from pipeline_engine.core.yaml_parser import load_pipeline_spec


@pytest.fixture
def tmp_yaml(tmp_path: Path):
    """Factory that writes a YAML string to a temp file and returns its path."""

    def _write(content: str) -> Path:
        p = tmp_path / "pipeline.yaml"
        p.write_text(textwrap.dedent(content), encoding="utf-8")
        return p

    return _write


MINIMAL_YAML = """\
    version: "1.0"
    pipeline:
      id: my_pipeline
      name: "My Pipeline"
      type: "测试"
    steps:
      - id: step_a
        tasks:
          - id: task1
            plugin: mymodule.MyTask
"""


def test_load_valid_yaml(tmp_yaml):
    spec = load_pipeline_spec(tmp_yaml(MINIMAL_YAML))
    assert spec.pipeline.id == "my_pipeline"
    assert len(spec.steps) == 1
    assert spec.steps[0].tasks[0].plugin == "mymodule.MyTask"


def test_file_not_found(tmp_path):
    with pytest.raises(PipelineError, match="not found"):
        load_pipeline_spec(tmp_path / "missing.yaml")


def test_invalid_yaml_syntax(tmp_yaml):
    with pytest.raises(PipelineError, match="Failed to parse YAML"):
        load_pipeline_spec(tmp_yaml("key: [unclosed"))


def test_missing_required_field_name(tmp_yaml):
    yaml_str = """\
        version: "1.0"
        pipeline:
          id: no_name_pipeline
        steps:
          - id: s1
            tasks:
              - id: t1
                plugin: m.T
    """
    with pytest.raises(PipelineError):
        load_pipeline_spec(tmp_yaml(yaml_str))


def test_invalid_id_pattern(tmp_yaml):
    yaml_str = """\
        version: "1.0"
        pipeline:
          id: "Bad-ID"
          name: "x"
          type: "测试"
        steps:
          - id: s1
            tasks:
              - id: t1
                plugin: m.T
    """
    with pytest.raises(PipelineError):
        load_pipeline_spec(tmp_yaml(yaml_str))


def test_duplicate_step_ids(tmp_yaml):
    yaml_str = """\
        version: "1.0"
        pipeline:
          id: dup_steps
          name: "X"
          type: "测试"
        steps:
          - id: step_a
            tasks:
              - id: t1
                plugin: m.T
          - id: step_a
            tasks:
              - id: t2
                plugin: m.T
    """
    with pytest.raises(PipelineError):
        load_pipeline_spec(tmp_yaml(yaml_str))


def test_duplicate_task_ids_in_step(tmp_yaml):
    yaml_str = """\
        version: "1.0"
        pipeline:
          id: dup_tasks
          name: "X"
          type: "测试"
        steps:
          - id: step_a
            tasks:
              - id: t1
                plugin: m.T
              - id: t1
                plugin: m.T
    """
    with pytest.raises(PipelineError):
        load_pipeline_spec(tmp_yaml(yaml_str))


def test_config_and_inputs_preserved(tmp_yaml):
    yaml_str = """\
        version: "1.0"
        pipeline:
          id: cfg_pipeline
          name: "C"
          type: "测试"
        steps:
          - id: step_a
            tasks:
              - id: t1
                plugin: m.T
                config:
                  key: value
                inputs:
                  static_val: 42
    """
    spec = load_pipeline_spec(tmp_yaml(yaml_str))
    task = spec.steps[0].tasks[0]
    assert task.config == {"key": "value"}
    assert task.inputs == {"static_val": 42}


def test_depends_on_preserved(tmp_yaml):
    yaml_str = """\
        version: "1.0"
        pipeline:
          id: dep_pipeline
          name: "D"
          type: "测试"
        steps:
          - id: step_a
            tasks:
              - id: t1
                plugin: m.T
              - id: t2
                plugin: m.T
                depends_on: [t1]
    """
    spec = load_pipeline_spec(tmp_yaml(yaml_str))
    assert spec.steps[0].tasks[1].depends_on == ["t1"]


def test_empty_steps_rejected(tmp_yaml):
    yaml_str = """\
        version: "1.0"
        pipeline:
          id: empty_steps
          name: "E"
          type: "测试"
        steps: []
    """
    with pytest.raises(PipelineError):
        load_pipeline_spec(tmp_yaml(yaml_str))
