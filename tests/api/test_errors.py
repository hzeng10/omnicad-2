"""Unit tests for pipeline_engine/api/errors.py."""
from __future__ import annotations

import pytest
from fastapi import Request
from fastapi.testclient import TestClient

from pipeline_engine.api.errors import _envelope_error, generic_error_handler, pipeline_error_handler
from pipeline_engine.core.errors import PipelineError


def test_envelope_error_pipeline_error():
    exc = PipelineError("broken", pipeline_id="p1", step_id="s1", task_id="t1")
    result = _envelope_error("start", exc)
    assert result["ok"] is False
    assert result["error"]["type"] == "PipelineError"
    assert result["error"]["pipeline_id"] == "p1"


def test_envelope_error_generic():
    exc = ValueError("something bad")
    result = _envelope_error("lint", exc)
    assert result["ok"] is False
    assert result["error"]["type"] == "ValueError"
    assert "something bad" in result["error"]["message"]


async def test_generic_error_handler_returns_500():
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/pipelines",
        "query_string": b"",
        "headers": [],
    }
    request = Request(scope)
    exc = RuntimeError("boom")
    response = await generic_error_handler(request, exc)
    assert response.status_code == 500
    import json
    body = json.loads(response.body)
    assert body["ok"] is False
    assert "boom" in body["error"]["message"]


async def test_pipeline_error_handler_returns_422():
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/runs",
        "query_string": b"",
        "headers": [],
    }
    request = Request(scope)
    exc = PipelineError("run not found", pipeline_id="p1")
    response = await pipeline_error_handler(request, exc)
    assert response.status_code == 422
    import json
    body = json.loads(response.body)
    assert body["ok"] is False
    assert body["error"]["type"] == "PipelineError"
