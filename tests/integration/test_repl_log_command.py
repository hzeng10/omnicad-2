"""Integration tests for the REPL 'log' command."""
from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipeline_engine.core import storage
from pipeline_engine.core.run_manager import RunManager
from pipeline_engine.core.run_logger import RunLogger, _run_id_var
from pipeline_engine.repl import _dispatch


# ─── helpers ─────────────────────────────────────────────────────────────────

def _make_run_context(tmp_path: Path, run_id: str) -> "RunManager":
    """Create a RunManager with a pre-seeded run context (no real scheduler)."""
    rm = RunManager(tmp_path)

    # Minimal spec mock
    from pipeline_engine.models.pipeline_spec import PipelineMeta, PipelineSpec, StepSpec, TaskSpec
    spec = PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id="test_pipe", name="Test", type="test"),
        steps=[StepSpec(id="s", tasks=[TaskSpec(id="t", plugin="tests.fake.FakeTask")])],
    )
    rm._registry["test_pipe"] = spec

    ctx = MagicMock()
    ctx.pipeline_id = "test_pipe"
    ctx.run_id = run_id
    ctx.is_active.return_value = False
    rm._runs[run_id] = ctx
    return rm


def _write_sample_log(tmp_path: Path, pipeline_id: str, run_id: str) -> Path:
    """Write a sample run.log with INFO and ERROR entries."""
    log_path = storage.get_run_log_path(tmp_path, pipeline_id, run_id)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "2026-05-13T09:30:01.001Z  INFO   [pipeline                    ]  pipeline run started: x",
        "2026-05-13T09:30:01.100Z  INFO   [parse_req/parse             ]  task start: parse_req/parse",
        "2026-05-13T09:30:01.200Z  INFO   [parse_req/parse             ]  task done: parse_req/parse",
        "2026-05-13T09:30:02.000Z  ERROR  [export_dxf/validate         ]  DEMO_FAIL: validate_dxf 强制失败",
        "2026-05-13T09:30:02.001Z  INFO   [pipeline                    ]  pipeline finished — status=FAILED",
    ]
    log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return log_path


async def _run_cmd(rm: RunManager, cmd: str) -> None:
    await _dispatch(rm, cmd)


# ─── basic log display ────────────────────────────────────────────────────────

def test_log_missing_instance_id_prints_usage(tmp_path, capsys):
    rm = _make_run_context(tmp_path, "test_pipe_20260513-000000_0001")
    asyncio.run(_run_cmd(rm, "log"))
    captured = capsys.readouterr().out
    assert "用法" in captured


def test_log_nonexistent_log_file_graceful(tmp_path, capsys):
    run_id = "test_pipe_20260513-000000_0001"
    rm = _make_run_context(tmp_path, run_id)
    asyncio.run(_run_cmd(rm, f"log {run_id}"))
    captured = capsys.readouterr().out
    assert "尚未生成" in captured or "run.log" in captured.lower()


def test_log_displays_lines(tmp_path, capsys):
    run_id = "test_pipe_20260513-000000_0001"
    rm = _make_run_context(tmp_path, run_id)
    _write_sample_log(tmp_path, "test_pipe", run_id)

    asyncio.run(_run_cmd(rm, f"log {run_id}"))
    captured = capsys.readouterr().out
    assert "pipeline run started" in captured
    assert "DEMO_FAIL" in captured


# ─── --tail / --offset paging ─────────────────────────────────────────────────

def test_log_tail_limits_lines(tmp_path, capsys):
    run_id = "test_pipe_20260513-000000_0001"
    rm = _make_run_context(tmp_path, run_id)
    _write_sample_log(tmp_path, "test_pipe", run_id)

    asyncio.run(_run_cmd(rm, f"log {run_id} --tail 2"))
    captured = capsys.readouterr().out
    # Should show last 2 lines (error line + "pipeline finished")
    assert "DEMO_FAIL" in captured
    assert "pipeline finished" in captured
    # Should NOT show early lines
    assert "pipeline run started" not in captured


def test_log_offset_skips_recent_lines(tmp_path, capsys):
    run_id = "test_pipe_20260513-000000_0001"
    rm = _make_run_context(tmp_path, run_id)
    _write_sample_log(tmp_path, "test_pipe", run_id)

    # offset=2 skips the last 2 lines, --tail 2 shows lines before that
    asyncio.run(_run_cmd(rm, f"log {run_id} --offset 2 --tail 2"))
    captured = capsys.readouterr().out
    # Lines 2-3 (0-indexed 1-2): "task start" and "task done"
    assert "task start" in captured or "task done" in captured


def test_log_all_shows_every_line(tmp_path, capsys):
    run_id = "test_pipe_20260513-000000_0001"
    rm = _make_run_context(tmp_path, run_id)
    _write_sample_log(tmp_path, "test_pipe", run_id)

    asyncio.run(_run_cmd(rm, f"log {run_id} --all"))
    captured = capsys.readouterr().out
    assert "pipeline run started" in captured
    assert "DEMO_FAIL" in captured
    assert "pipeline finished" in captured


# ─── --errors-only filter ─────────────────────────────────────────────────────

def test_log_errors_only_filters_to_error_lines(tmp_path, capsys):
    run_id = "test_pipe_20260513-000000_0001"
    rm = _make_run_context(tmp_path, run_id)
    _write_sample_log(tmp_path, "test_pipe", run_id)

    asyncio.run(_run_cmd(rm, f"log {run_id} --errors-only"))
    captured = capsys.readouterr().out
    assert "DEMO_FAIL" in captured
    # Non-error lines should not appear
    assert "pipeline run started" not in captured
    assert "task start" not in captured


# ─── invalid arguments ────────────────────────────────────────────────────────

def test_log_invalid_tail_shows_error(tmp_path, capsys):
    run_id = "test_pipe_20260513-000000_0001"
    rm = _make_run_context(tmp_path, run_id)
    asyncio.run(_run_cmd(rm, f"log {run_id} --tail notanumber"))
    captured = capsys.readouterr().out
    assert "整数" in captured or "int" in captured.lower()


# ─── completion for log command ──────────────────────────────────────────────

def test_log_completion_suggests_instance_ids(tmp_path):
    from pipeline_engine.repl_completion import PipelineReplCompleter
    from prompt_toolkit.document import Document

    run_id = "test_pipe_20260513-000000_0001"
    rm = _make_run_context(tmp_path, run_id)
    c = PipelineReplCompleter(rm)

    doc = Document(f"log {run_id[:10]}")
    completions = [comp.text for comp in c.get_completions(doc, None)]
    assert any(run_id[10:] in t for t in completions)


def test_log_completion_suggests_flags(tmp_path):
    from pipeline_engine.repl_completion import PipelineReplCompleter
    from prompt_toolkit.document import Document

    run_id = "test_pipe_20260513-000000_0001"
    rm = _make_run_context(tmp_path, run_id)
    c = PipelineReplCompleter(rm)

    doc = Document(f"log {run_id} --")
    completions = [comp.text for comp in c.get_completions(doc, None)]
    assert any("errors-only" in t or "all" in t or "tail" in t for t in completions)
