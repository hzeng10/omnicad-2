"""Tests for PipelineMeta model validation."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from pipeline_engine.models.pipeline_spec import PipelineMeta


def test_pipeline_meta_requires_type():
    with pytest.raises(ValidationError, match="type"):
        PipelineMeta(id="p1", name="Test")


def test_pipeline_meta_valid_with_type():
    m = PipelineMeta(id="p1", name="Test", type="CAD图识别及算量")
    assert m.type == "CAD图识别及算量"
