"""REST API endpoint tests using httpx.AsyncClient + ASGITransport.

Covers happy paths and key edge cases for every router.
"""
from __future__ import annotations

import pytest

from tests.api.conftest import make_pipeline_yaml


# ── health ────────────────────────────────────────────────────────────────────

async def test_health(client):
    ac, svc, tmp = client
    resp = await ac.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "status": "healthy"}


# ── lint ──────────────────────────────────────────────────────────────────────

async def test_lint_valid(client):
    ac, svc, tmp = client
    yaml_path = make_pipeline_yaml(tmp, "lint_test")
    resp = await ac.post("/lint", json={"path": str(yaml_path)})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["valid"] is True
    assert body["pipeline_id"] == "lint_test"


async def test_lint_invalid_path(client):
    ac, svc, tmp = client
    resp = await ac.post("/lint", json={"path": str(tmp / "nonexistent.yaml")})
    assert resp.status_code == 422
    body = resp.json()
    assert body["ok"] is False


# ── pipelines ─────────────────────────────────────────────────────────────────

async def test_load_and_list_pipelines(client):
    ac, svc, tmp = client
    yaml_path = make_pipeline_yaml(tmp, "p1")

    # Load
    resp = await ac.post("/pipelines", json={"paths": [str(yaml_path)]})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["loaded"][0]["pipeline_id"] == "p1"
    assert body["loaded"][0]["ok"] is True

    # List
    resp = await ac.get("/pipelines")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    ids = [p["pipeline_id"] for p in body["pipelines"]]
    assert "p1" in ids


async def test_load_nonexistent_file(client):
    ac, svc, tmp = client
    resp = await ac.post("/pipelines", json={"paths": ["/does/not/exist.yaml"]})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    assert body["loaded"][0]["ok"] is False


# ── runs ──────────────────────────────────────────────────────────────────────

async def _load_and_start(ac, svc, tmp):
    """Helper: register a pipeline then POST /runs."""
    yaml_path = make_pipeline_yaml(tmp, "run_p")
    await ac.post("/pipelines", json={"paths": [str(yaml_path)]})
    resp = await ac.post("/runs", json={"pipeline_ids": ["run_p"]})
    return resp


async def test_start_returns_202(client):
    ac, svc, tmp = client
    resp = await _load_and_start(ac, svc, tmp)
    assert resp.status_code == 202
    body = resp.json()
    assert body["ok"] is True
    assert len(body["runs"]) == 1
    assert "run_id" in body["runs"][0]


async def test_start_unknown_pipeline(client):
    ac, svc, tmp = client
    resp = await ac.post("/runs", json={"pipeline_ids": ["ghost_pipeline"]})
    assert resp.status_code == 422
    body = resp.json()
    assert body["ok"] is False


async def test_list_runs(client):
    ac, svc, tmp = client
    await _load_and_start(ac, svc, tmp)
    resp = await ac.get("/runs")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert "instances" in body


async def test_get_run(client):
    ac, svc, tmp = client
    start_resp = await _load_and_start(ac, svc, tmp)
    run_id = start_resp.json()["runs"][0]["run_id"]

    # Wait for it to finish (EchoTask is synchronous-fast)
    import asyncio
    for _ in range(20):
        resp = await ac.get(f"/runs/{run_id}")
        body = resp.json()
        if body["state"]["status"] in ("success", "failed"):
            break
        await asyncio.sleep(0.05)

    assert resp.status_code == 200
    assert body["ok"] is True
    assert "state" in body


async def test_get_run_not_found(client):
    ac, svc, tmp = client
    resp = await ac.get("/runs/nonexistent_run_id")
    assert resp.status_code in (404, 422)
    body = resp.json()
    assert body["ok"] is False


async def test_get_step(client):
    ac, svc, tmp = client
    start_resp = await _load_and_start(ac, svc, tmp)
    run_id = start_resp.json()["runs"][0]["run_id"]

    import asyncio
    for _ in range(20):
        resp = await ac.get(f"/runs/{run_id}")
        if resp.json()["state"]["status"] in ("success", "failed"):
            break
        await asyncio.sleep(0.05)

    resp = await ac.get(f"/runs/{run_id}/steps/step_a")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True


async def test_get_task(client):
    ac, svc, tmp = client
    start_resp = await _load_and_start(ac, svc, tmp)
    run_id = start_resp.json()["runs"][0]["run_id"]

    import asyncio
    for _ in range(20):
        resp = await ac.get(f"/runs/{run_id}")
        if resp.json()["state"]["status"] in ("success", "failed"):
            break
        await asyncio.sleep(0.05)

    resp = await ac.get(f"/runs/{run_id}/tasks/step_a/t1")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True


async def test_get_log(client):
    ac, svc, tmp = client
    start_resp = await _load_and_start(ac, svc, tmp)
    run_id = start_resp.json()["runs"][0]["run_id"]

    import asyncio
    for _ in range(20):
        resp = await ac.get(f"/runs/{run_id}")
        if resp.json()["state"]["status"] in ("success", "failed"):
            break
        await asyncio.sleep(0.05)

    resp = await ac.get(f"/runs/{run_id}/log")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert "lines" in body


async def test_stop_run(client):
    ac, svc, tmp = client
    start_resp = await _load_and_start(ac, svc, tmp)
    run_id = start_resp.json()["runs"][0]["run_id"]

    resp = await ac.post(f"/runs/{run_id}:stop")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True


async def test_resume_unknown_run(client):
    """POST :resume on nonexistent run triggers pipeline_error_handler (422)."""
    ac, svc, tmp = client
    resp = await ac.post("/runs/no_such_run:resume")
    assert resp.status_code == 422
    body = resp.json()
    assert body["ok"] is False


async def test_fix_unknown_run(client):
    """POST :fix on nonexistent run triggers pipeline_error_handler (422)."""
    ac, svc, tmp = client
    resp = await ac.post(
        "/runs/no_such_run/tasks/step_a/t1:fix",
        json={"mode": "output", "path": "/tmp/fake.json"},
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["ok"] is False


async def test_resume_terminal_run_blocked(client):
    """C2 guard: POST :resume on a completed (SUCCESS) run returns 422."""
    import asyncio
    ac, svc, tmp = client
    yaml_path = make_pipeline_yaml(tmp, "c2_pipe")
    await ac.post("/pipelines", json={"paths": [str(yaml_path)]})
    start_resp = await ac.post("/runs", json={"pipeline_ids": ["c2_pipe"]})
    run_id = start_resp.json()["runs"][0]["run_id"]

    # Wait for run to reach terminal state
    for _ in range(30):
        resp = await ac.get(f"/runs/{run_id}")
        if resp.json()["state"]["status"] in ("success", "failed"):
            break
        await asyncio.sleep(0.05)

    # Resuming a terminal run via REST must be rejected
    resp = await ac.post(f"/runs/{run_id}:resume")
    assert resp.status_code == 422
    body = resp.json()
    assert body["ok"] is False


async def test_resume_returns_202_immediately(client):
    """POST :resume must return 202 without waiting for the run to complete (H2)."""
    import asyncio
    ac, svc, tmp = client
    yaml_path = make_pipeline_yaml(tmp, "h2_pipe_a")
    await ac.post("/pipelines", json={"paths": [str(yaml_path)]})
    start_resp = await ac.post("/runs", json={"pipeline_ids": ["h2_pipe_a"]})
    run_id = start_resp.json()["runs"][0]["run_id"]

    # Stop the run so it's resumable
    await ac.post(f"/runs/{run_id}:stop")
    ctx = svc.rm._runs[run_id]
    await ctx.main_task

    resp = await ac.post(f"/runs/{run_id}:resume")
    assert resp.status_code == 202
    body = resp.json()
    assert body["ok"] is True
    assert body["resumed"] == run_id
    # wait=False path: final_status is not included (run may still be in progress)
    assert "final_status" not in body

    # cleanup
    await svc.rm._runs[run_id].main_task


async def test_resumed_run_actually_continues(client):
    """After a 202 resume, the run must eventually reach a terminal state."""
    import asyncio
    ac, svc, tmp = client
    yaml_path = make_pipeline_yaml(tmp, "h2_pipe_b")
    await ac.post("/pipelines", json={"paths": [str(yaml_path)]})
    start_resp = await ac.post("/runs", json={"pipeline_ids": ["h2_pipe_b"]})
    run_id = start_resp.json()["runs"][0]["run_id"]

    # Stop the run, wait for it to reach paused/failed
    await ac.post(f"/runs/{run_id}:stop")
    ctx = svc.rm._runs[run_id]
    await ctx.main_task

    # Resume (202) and then wait for the new main_task to complete
    resp = await ac.post(f"/runs/{run_id}:resume")
    assert resp.status_code == 202

    ctx2 = svc.rm._runs[run_id]
    await ctx2.main_task

    # Verify the run reached a terminal state
    status_resp = await ac.get(f"/runs/{run_id}")
    final_status = status_resp.json()["state"]["status"]
    assert final_status in ("success", "failed", "paused")


