"""Tests for PluginLoader dynamic import."""
from __future__ import annotations

import pytest

from pipeline_engine.core.errors import PipelineError
from pipeline_engine.core.plugin_loader import instantiate_task, load_task_class


# ─── helpers ──────────────────────────────────────────────────────────────────

class _FakeTask:
    """Not a BaseTask subclass — used to test rejection."""
    pass


# ─── tests ────────────────────────────────────────────────────────────────────

def test_load_valid_task_class():
    # Use a real BaseTask subclass from the test_base_task module
    cls = load_task_class("tests.unit.test_base_task.AsyncTask")
    from tests.unit.test_base_task import AsyncTask
    assert cls is AsyncTask


def test_load_non_basetask_raises():
    # Inject _FakeTask into globals so we can reference it by path
    import sys
    module = sys.modules[__name__]
    with pytest.raises(PipelineError, match="not a subclass of BaseTask"):
        load_task_class(f"{__name__}._FakeTask")


def test_load_missing_module_raises():
    with pytest.raises(PipelineError, match="cannot import plugin module"):
        load_task_class("totally.nonexistent.module.MyTask")


def test_load_missing_class_raises():
    with pytest.raises(PipelineError, match="not found in module"):
        load_task_class("pipeline_engine.core.errors.GhostClass")


def test_no_dot_raises():
    with pytest.raises(PipelineError, match="must be 'module.ClassName'"):
        load_task_class("NoModulePath")


def test_instantiate_task():
    task = instantiate_task("tests.unit.test_base_task.AsyncTask", "my_task", {"k": "v"})
    assert task.task_id == "my_task"
    assert task.config == {"k": "v"}
