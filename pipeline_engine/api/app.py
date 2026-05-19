"""FastAPI application factory.

``create_app(svc)`` wires up all routers and exception handlers, then
attaches the PipelineService to ``app.state.svc`` so route handlers can
access it via ``request.app.state.svc``.

The serve command passes an already-bootstrapped PipelineService so the
app has no awareness of workspace / autoload details.
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from pipeline_engine.api.errors import generic_error_handler, pipeline_error_handler
from pipeline_engine.api.routers import events, pipelines, runs
from pipeline_engine.core.errors import PipelineError
from pipeline_engine.service import PipelineService


def create_app(svc: PipelineService) -> FastAPI:
    """Build and return the FastAPI application."""
    app = FastAPI(
        title="Pipeline Engine API",
        description=(
            "RESTful HTTP interface for pipeline_engine. "
            "All responses follow the same JSON envelope as the CLI: "
            '``{"ok": true, "command": "...", ...payload}``.'
        ),
        version="1.0.0",
    )

    app.state.svc = svc

    app.add_exception_handler(PipelineError, pipeline_error_handler)
    app.add_exception_handler(Exception, generic_error_handler)

    app.include_router(pipelines.router)
    app.include_router(runs.router)
    app.include_router(events.router)

    @app.get("/health", include_in_schema=False)
    async def health() -> JSONResponse:
        return JSONResponse({"ok": True, "status": "healthy"})

    return app
