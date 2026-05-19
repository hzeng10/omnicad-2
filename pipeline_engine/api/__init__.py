"""FastAPI application factory for pipeline_engine HTTP API.

Import ``create_app`` to build the FastAPI app with a given PipelineService.
The ``pipeline_cli serve`` command uses this via ``uvicorn.run``.
"""
from pipeline_engine.api.app import create_app

__all__ = ["create_app"]
