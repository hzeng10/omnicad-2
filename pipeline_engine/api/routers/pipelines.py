"""Routers for pipeline-level resources: /lint, /pipelines."""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, Request

from pipeline_engine.api.schemas import LintRequest, LoadRequest, envelope_err, envelope_ok
from pipeline_engine.service import PipelineService

router = APIRouter()


def _svc(request: Request) -> PipelineService:
    return request.app.state.svc


@router.post("/lint", summary="Validate a pipeline YAML (no execution)")
async def lint(body: LintRequest, svc: PipelineService = Depends(_svc)):
    result = await svc.cmd_lint(Path(body.path))
    return envelope_ok("lint", **result)


@router.post("/pipelines", summary="Register pipeline YAML files")
async def load(body: LoadRequest, svc: PipelineService = Depends(_svc)):
    result = await svc.cmd_load([Path(p) for p in body.paths])
    all_ok = all(item["ok"] for item in result["loaded"])
    if all_ok:
        return envelope_ok("load", **result)
    return envelope_err("load", "一个或多个文件加载失败", "LoadError", **result)


@router.get("/pipelines", summary="List registered pipelines")
async def list_pipelines(svc: PipelineService = Depends(_svc)):
    result = await svc.cmd_list_pipelines()
    return envelope_ok("list", **result)
