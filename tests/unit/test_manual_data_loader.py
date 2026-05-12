"""Tests for manual_data_loader."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline_engine.builtin.manual_data_loader import load_skip_output
from pipeline_engine.core.errors import PipelineError


def test_load_skip_output_success(tmp_path):
    step_id = "my_step"
    manual_dir = tmp_path / "manual_data" / step_id
    manual_dir.mkdir(parents=True)
    (manual_dir / "output.json").write_text(json.dumps({"result": 42}))
    data = load_skip_output(tmp_path, step_id)
    assert data == {"result": 42}


def test_load_skip_output_missing_raises(tmp_path):
    with pytest.raises(PipelineError):
        load_skip_output(tmp_path, "missing_step")
