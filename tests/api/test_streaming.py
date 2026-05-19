"""Tests for the SSE streaming endpoint GET /runs/{run_id}/events."""
from __future__ import annotations

import asyncio

import pytest

from tests.api.conftest import make_pipeline_yaml


async def _collect_sse_lines(ac, run_id: str, timeout: float = 5.0) -> list[str]:
    """Read SSE stream until terminal event, collecting all non-empty lines."""
    lines: list[str] = []
    terminal_seen = False
    async with ac.stream("GET", f"/runs/{run_id}/events") as resp:
        assert resp.status_code == 200
        assert "text/event-stream" in resp.headers.get("content-type", "")
        deadline = asyncio.get_event_loop().time() + timeout
        async for line in resp.aiter_lines():
            if asyncio.get_event_loop().time() > deadline:
                break
            if line:
                lines.append(line)
            if terminal_seen and line.startswith("data:"):
                # Captured the data line following event: terminal — done
                break
            if line.startswith("event: terminal"):
                terminal_seen = True
    return lines


async def test_sse_terminal_event_after_run(client):
    ac, svc, tmp = client
    yaml_path = make_pipeline_yaml(tmp, "sse_pipe")
    await ac.post("/pipelines", json={"paths": [str(yaml_path)]})
    start_resp = await ac.post("/runs", json={"pipeline_ids": ["sse_pipe"]})
    run_id = start_resp.json()["runs"][0]["run_id"]

    lines = await _collect_sse_lines(ac, run_id)

    event_lines = [l for l in lines if l.startswith("event:")]
    assert any("terminal" in l for l in event_lines), (
        f"Expected a terminal event in SSE stream; got lines: {lines}"
    )


async def test_sse_already_terminal(client):
    """SSE for a completed run immediately sends terminal event."""
    ac, svc, tmp = client
    yaml_path = make_pipeline_yaml(tmp, "sse_done")
    await ac.post("/pipelines", json={"paths": [str(yaml_path)]})
    start_resp = await ac.post("/runs", json={"pipeline_ids": ["sse_done"]})
    run_id = start_resp.json()["runs"][0]["run_id"]

    # Wait for run to complete
    for _ in range(30):
        resp = await ac.get(f"/runs/{run_id}")
        if resp.json()["state"]["status"] in ("success", "failed"):
            break
        await asyncio.sleep(0.05)

    # Now SSE should immediately yield terminal
    lines = await _collect_sse_lines(ac, run_id, timeout=3.0)
    event_lines = [l for l in lines if l.startswith("event:")]
    assert any("terminal" in l for l in event_lines), (
        f"Expected immediate terminal for completed run; got: {lines}"
    )


async def test_sse_unknown_run(client):
    """SSE for nonexistent run yields an error event."""
    ac, svc, tmp = client
    lines: list[str] = []
    async with ac.stream("GET", "/runs/no_such_run/events") as resp:
        assert resp.status_code == 200
        async for line in resp.aiter_lines():
            if line:
                lines.append(line)
            if lines:
                break

    assert any("error" in l for l in lines), f"Expected error event; got: {lines}"


async def test_sse_broadcaster_integration(client):
    """Verifies that state_manager events flow to SSE subscriber."""
    ac, svc, tmp = client
    yaml_path = make_pipeline_yaml(tmp, "sse_int")
    await ac.post("/pipelines", json={"paths": [str(yaml_path)]})
    start_resp = await ac.post("/runs", json={"pipeline_ids": ["sse_int"]})
    run_id = start_resp.json()["runs"][0]["run_id"]

    lines = await _collect_sse_lines(ac, run_id)

    data_lines = [l for l in lines if l.startswith("data:")]
    assert data_lines, "Expected at least one data line in SSE stream"
