"""Shared fixtures for API tests."""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from pipeline_engine.api import create_app
from pipeline_engine.core.base_task import BaseTask
from pipeline_engine.core.run_manager import RunManager
from pipeline_engine.service import PipelineService


class EchoTask(BaseTask):
    async def execute(self, inputs, progress):
        await progress(100)
        return {"echo": True}


def make_pipeline_yaml(tmp_path: Path, pid: str, plugin: str = "tests.api.conftest.EchoTask") -> Path:
    content = textwrap.dedent(f"""\
        version: "1.0"
        pipeline:
          id: {pid}
          name: "API Test {pid}"
          type: "test"
        steps:
          - id: step_a
            tasks:
              - id: t1
                plugin: {plugin}
    """)
    p = tmp_path / f"{pid}.yaml"
    p.write_text(content)
    return p


@pytest_asyncio.fixture
async def client(tmp_path):
    """Async HTTP client wired to a fresh PipelineService + FastAPI app."""
    rm = RunManager(tmp_path)
    svc = PipelineService(rm, no_autoload=True)
    app = create_app(svc)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac, svc, tmp_path
