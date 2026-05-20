"""Routers for run-level resources: /runs, /runs/{run_id}/..."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from pipeline_engine.api.schemas import FixRequest, ResumeRequest, StartRequest, envelope_err, envelope_ok
from pipeline_engine.core.errors import PipelineError
from pipeline_engine.models.runtime_state import Status, TERMINAL_PIPELINE_STATUSES
from pipeline_engine.service import PipelineService

# C2 guard: subset of terminal statuses from which resume is blocked.
# FAILED and PAUSED are intentionally excluded — those runs are resumable.
_RESUME_BLOCKED_STATUSES = frozenset(
    s for s in TERMINAL_PIPELINE_STATUSES
    if s not in (Status.FAILED, Status.PAUSED)
)

router = APIRouter()


def _svc(request: Request) -> PipelineService:
    return request.app.state.svc


# ── list instances ────────────────────────────────────────────────────────────

@router.get("/runs", summary="List run instances")
async def list_runs(svc: PipelineService = Depends(_svc)):
    result = await svc.cmd_list_instances()
    return envelope_ok("list", **result)


# ── start ─────────────────────────────────────────────────────────────────────

@router.post("/runs", responses={202: {"description": "Accepted"}}, summary="Start pipeline run(s)")
async def start(body: StartRequest, svc: PipelineService = Depends(_svc)):
    # REST always uses wait=False (async); client polls /runs/{id} or subscribes to SSE
    result = await svc.cmd_start(body.pipeline_ids, step=body.step, task=body.task, wait=False)
    any_error = any(not r["ok"] for r in result["runs"])
    if any_error:
        return JSONResponse(
            status_code=422,
            content=envelope_err("start", "一个或多个 pipeline 启动失败", "StartError", **result),
        )
    return JSONResponse(status_code=202, content=envelope_ok("start", **result))


# ── status / inspect ──────────────────────────────────────────────────────────

@router.get("/runs/{run_id}", summary="Get run status")
async def get_run(run_id: str, svc: PipelineService = Depends(_svc)):
    result = await svc.cmd_status(run_id)
    return envelope_ok("status", **result)


@router.get("/runs/{run_id}/steps/{step_id}", summary="Inspect a step")
async def get_step(run_id: str, step_id: str, svc: PipelineService = Depends(_svc)):
    result = await svc.cmd_inspect(run_id, step=step_id)
    return envelope_ok("inspect", **result)


@router.get(
    "/runs/{run_id}/tasks/{step_id}/{task_id}",
    summary="Inspect a task (detail with input/output/log)",
)
async def get_task(run_id: str, step_id: str, task_id: str, svc: PipelineService = Depends(_svc)):
    result = await svc.cmd_inspect(run_id, step=step_id, task=task_id)
    return envelope_ok("inspect", **result)


# ── stop ──────────────────────────────────────────────────────────────────────

@router.post("/runs/{run_id}:stop", summary="Stop a run")
async def stop(run_id: str, svc: PipelineService = Depends(_svc)):
    result = await svc.cmd_stop(run_id)
    return envelope_ok("stop", **result)


# ── resume ────────────────────────────────────────────────────────────────────

@router.post("/runs/{run_id}:resume", summary="Resume a failed/paused run")
async def resume(
    run_id: str,
    body: Optional[ResumeRequest] = None,
    svc: PipelineService = Depends(_svc),
):
    include_paused = body.include_paused if body else False
    # C2 guard (REST-only): reject resume on terminal runs to prevent duplicate execution
    # of side-effect tasks. The CLI allows resuming completed runs intentionally.
    state = await svc.rm.get_run_state(run_id)
    if state.status in _RESUME_BLOCKED_STATUSES:
        raise PipelineError(
            f"run '{run_id}' is in terminal state '{state.status.value}'; cannot resume",
            pipeline_id=state.pipeline_id,
        )
    # REST: fire-and-forget like POST /runs; client polls GET /runs/{id} or subscribes to SSE
    result = await svc.cmd_resume(run_id, include_paused=include_paused, wait=False)
    return JSONResponse(status_code=202, content=envelope_ok("resume", **result))


# ── fix ───────────────────────────────────────────────────────────────────────

@router.post(
    "/runs/{run_id}/tasks/{step_id}/{task_id}:fix",
    summary="Inject fix data into a failed task",
)
async def fix(
    run_id: str,
    step_id: str,
    task_id: str,
    body: FixRequest,
    svc: PipelineService = Depends(_svc),
):
    locator = f"{step_id}/{task_id}"
    output_path = Path(body.path) if body.mode == "output" else None
    input_path = Path(body.path) if body.mode == "input" else None
    result = await svc.cmd_fix(
        run_id, locator, output_path=output_path, input_path=input_path
    )
    return envelope_ok("fix", **result)


# ── log ───────────────────────────────────────────────────────────────────────

@router.get("/runs/{run_id}/log", summary="Read run log")
async def get_log(
    run_id: str,
    tail: int = 100,
    offset: int = 0,
    all: bool = False,
    errors_only: bool = False,
    svc: PipelineService = Depends(_svc),
):
    result = await svc.cmd_log(
        run_id, tail=tail, offset=offset, all_lines=all, errors_only=errors_only
    )
    return envelope_ok("log", **result)
