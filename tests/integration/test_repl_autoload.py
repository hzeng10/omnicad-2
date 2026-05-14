"""Integration tests for REPL autoload behavior."""
from __future__ import annotations

import asyncio
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline_engine.core.run_manager import RunManager
from pipeline_engine.cli import _autoload_pipelines


def _make_pipeline_yaml(base_dir: Path, pid: str) -> Path:
    plugin = "tests.integration.test_repl_autoload.NopTask"
    pdir = base_dir / pid
    pdir.mkdir(parents=True, exist_ok=True)
    yaml_p = pdir / "pipeline.yaml"
    yaml_p.write_text(textwrap.dedent(f"""\
        version: "1.0"
        pipeline:
          id: {pid}
          name: "{pid}"
          type: "test"
        steps:
          - id: s
            tasks:
              - id: t
                plugin: {plugin}
    """))
    return yaml_p


from pipeline_engine.core.base_task import BaseTask


class NopTask(BaseTask):
    async def execute(self, inputs, progress):
        await progress(100)
        return {}


# ─── _autoload_pipelines helper ──────────────────────────────────────────────

def test_autoload_discovers_yamls(tmp_path):
    """_autoload_pipelines registers discovered pipelines into RunManager."""
    pl_dir = tmp_path / "pipelines"
    _make_pipeline_yaml(pl_dir, "disc_a")
    _make_pipeline_yaml(pl_dir, "disc_b")

    rm = RunManager(tmp_path)

    async def _run():
        return await _autoload_pipelines(rm, pl_dir)

    results = asyncio.run(_run())
    ok = [r for r in results if r["ok"]]
    assert len(ok) == 2
    assert {"disc_a", "disc_b"} == {r["pipeline_id"] for r in ok}
    # pipelines registered in RunManager
    pids = {p["pipeline_id"] for p in rm.list_pipelines()}
    assert "disc_a" in pids and "disc_b" in pids


def test_autoload_skips_bad_yaml(tmp_path):
    """A bad YAML is skipped without raising; good ones still loaded."""
    pl_dir = tmp_path / "pipelines"
    _make_pipeline_yaml(pl_dir, "good_one")

    bad_dir = pl_dir / "bad_one"
    bad_dir.mkdir()
    (bad_dir / "pipeline.yaml").write_text("{bad yaml content >>>")

    rm = RunManager(tmp_path)

    async def _run():
        return await _autoload_pipelines(rm, pl_dir)

    results = asyncio.run(_run())
    ok_ids = {r["pipeline_id"] for r in results if r["ok"]}
    fail_ids = {r["path"].split("/")[-2] for r in results if not r["ok"]}
    assert "good_one" in ok_ids
    assert "bad_one" in fail_ids


def test_autoload_missing_dir_returns_empty(tmp_path):
    """Non-existent base_dir returns empty results without error."""
    rm = RunManager(tmp_path)
    missing = tmp_path / "no_such_dir"

    async def _run():
        return await _autoload_pipelines(rm, missing)

    results = asyncio.run(_run())
    assert results == []
    assert rm.list_pipelines() == []


def test_autoload_idempotent(tmp_path):
    """Calling _autoload_pipelines twice doesn't duplicate registry entries."""
    pl_dir = tmp_path / "pipelines"
    _make_pipeline_yaml(pl_dir, "idem_pipe")

    rm = RunManager(tmp_path)

    async def _run():
        await _autoload_pipelines(rm, pl_dir)
        await _autoload_pipelines(rm, pl_dir)

    asyncio.run(_run())
    pids = [p["pipeline_id"] for p in rm.list_pipelines()]
    assert pids.count("idem_pipe") == 1
