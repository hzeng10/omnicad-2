"""Exception handlers: PipelineError / generic Exception → envelope JSON response."""
from __future__ import annotations

from fastapi import Request
from fastapi.responses import JSONResponse

from pipeline_engine.core.errors import PipelineError


def _envelope_error(command: str, exc: BaseException) -> dict:
    if isinstance(exc, PipelineError):
        return {
            "ok": False,
            "command": command,
            "error": {
                "message": str(exc),
                "type": "PipelineError",
                "pipeline_id": exc.pipeline_id,
                "step_id": exc.step_id,
                "task_id": exc.task_id,
            },
        }
    return {
        "ok": False,
        "command": command,
        "error": {"message": str(exc), "type": type(exc).__name__},
    }


async def pipeline_error_handler(request: Request, exc: PipelineError) -> JSONResponse:
    command = request.url.path.lstrip("/").split("/")[0] or "api"
    return JSONResponse(status_code=422, content=_envelope_error(command, exc))


async def generic_error_handler(request: Request, exc: Exception) -> JSONResponse:
    command = request.url.path.lstrip("/").split("/")[0] or "api"
    return JSONResponse(status_code=500, content=_envelope_error(command, exc))
