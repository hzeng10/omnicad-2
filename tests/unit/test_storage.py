"""Tests for Storage utilities."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipeline_engine.core import storage
from pipeline_engine.core.errors import PipelineError


def test_atomic_write_json(tmp_path):
    path = tmp_path / "out.json"
    storage.atomic_write_json(path, {"key": "value"})
    assert json.loads(path.read_text()) == {"key": "value"}
    assert not (tmp_path / "out.json.tmp").exists()


def test_atomic_write_creates_parents(tmp_path):
    path = tmp_path / "a" / "b" / "c.json"
    storage.atomic_write_json(path, {"x": 1})
    assert path.exists()


def test_atomic_write_no_leftover_on_success(tmp_path):
    path = tmp_path / "f.json"
    storage.atomic_write_json(path, {})
    tmp = Path(str(path) + ".tmp")
    assert not tmp.exists()


def test_load_manual_data_ok(tmp_path):
    step_dir = tmp_path / "manual_data" / "step_a"
    step_dir.mkdir(parents=True)
    (step_dir / "output.json").write_text('{"result": 42}')
    data = storage.load_manual_data(tmp_path, "step_a")
    assert data == {"result": 42}


def test_load_manual_data_missing(tmp_path):
    with pytest.raises(PipelineError, match="manual_data not found"):
        storage.load_manual_data(tmp_path, "ghost_step")


def test_load_manual_data_bad_json(tmp_path):
    step_dir = tmp_path / "manual_data" / "bad_step"
    step_dir.mkdir(parents=True)
    (step_dir / "output.json").write_text("not json {")
    with pytest.raises(PipelineError, match="not valid JSON"):
        storage.load_manual_data(tmp_path, "bad_step")


def test_load_manual_data_non_dict(tmp_path):
    step_dir = tmp_path / "manual_data" / "list_step"
    step_dir.mkdir(parents=True)
    (step_dir / "output.json").write_text("[1, 2, 3]")
    with pytest.raises(PipelineError, match="must be a JSON object"):
        storage.load_manual_data(tmp_path, "list_step")


def test_fix_output_copies_file(tmp_path):
    src = tmp_path / "recover.json"
    src.write_text('{"items": []}')
    dest = storage.fix_output(tmp_path, "pipe1", "run1", "step_a", "task_x", src)
    assert dest.exists()
    assert json.loads(dest.read_text()) == {"items": []}


def test_fix_output_missing_src(tmp_path):
    with pytest.raises(PipelineError, match="not found"):
        storage.fix_output(tmp_path, "p", "r", "s", "t", tmp_path / "ghost.json")


def test_fix_output_bad_json_src(tmp_path):
    src = tmp_path / "bad.json"
    src.write_text("{broken")
    with pytest.raises(PipelineError, match="not valid JSON"):
        storage.fix_output(tmp_path, "p", "r", "s", "t", src)


def test_task_output_exists(tmp_path):
    assert not storage.task_output_exists(tmp_path, "p", "r", "s", "t")
    td = storage.init_task_dir(tmp_path, "p", "r", "s", "t")
    (td / "output.json").write_text('{}')
    assert storage.task_output_exists(tmp_path, "p", "r", "s", "t")


def test_workspace_path_override(tmp_path):
    custom = tmp_path / "custom_ws"
    storage.atomic_write_json(custom / ".pipeline_runs" / "test.json", {"ok": True})
    assert (custom / ".pipeline_runs" / "test.json").exists()
