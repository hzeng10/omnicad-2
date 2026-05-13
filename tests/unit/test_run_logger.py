"""Unit tests for RunLogger: attach/detach lifecycle, contextvar isolation,
task_context logging, and stdout capture."""
from __future__ import annotations

import asyncio
import io
import logging
import sys
from pathlib import Path

import pytest

from pipeline_engine.core.run_logger import (
    RunLogger,
    _active_loggers,
    _run_id_var,
    _task_ctx_var,
)


# ─── helpers ─────────────────────────────────────────────────────────────────

def _make_logger(tmp_path: Path, run_id: str = "test_pipe_20260513-093024_0001") -> RunLogger:
    log_path = tmp_path / run_id / "run.log"
    return RunLogger(run_id, log_path)


def _read_log(rl: RunLogger) -> list[str]:
    return rl._log_path.read_text(encoding="utf-8").splitlines()


# ─── attach / detach ─────────────────────────────────────────────────────────

def test_attach_creates_log_file(tmp_path: Path) -> None:
    rl = _make_logger(tmp_path)
    _run_id_var.set(rl._run_id)
    rl.attach()
    try:
        assert rl._log_path.exists()
    finally:
        rl.detach()


def test_attach_idempotent(tmp_path: Path) -> None:
    """Calling attach() twice must not duplicate the handler."""
    rl = _make_logger(tmp_path)
    _run_id_var.set(rl._run_id)
    rl.attach()
    pe_logger = logging.getLogger("pipeline_engine")
    count_before = sum(
        1 for h in pe_logger.handlers if getattr(h, "_run_id", None) == rl._run_id
    )
    rl.attach()  # second call
    count_after = sum(
        1 for h in pe_logger.handlers if getattr(h, "_run_id", None) == rl._run_id
    )
    assert count_before == count_after == 1
    rl.detach()


def test_detach_removes_handler(tmp_path: Path) -> None:
    rl = _make_logger(tmp_path)
    _run_id_var.set(rl._run_id)
    rl.attach()
    rl.detach()
    pe_logger = logging.getLogger("pipeline_engine")
    remaining = [h for h in pe_logger.handlers if getattr(h, "_run_id", None) == rl._run_id]
    assert remaining == []
    assert rl._run_id not in _active_loggers


def test_detach_idempotent(tmp_path: Path) -> None:
    rl = _make_logger(tmp_path)
    _run_id_var.set(rl._run_id)
    rl.attach()
    rl.detach()
    rl.detach()  # should not raise


def test_resume_appends_to_existing_log(tmp_path: Path) -> None:
    """Second attach (resume scenario) appends to existing run.log."""
    rl = _make_logger(tmp_path)
    _run_id_var.set(rl._run_id)

    rl.attach()
    rl.info("first session line")
    rl.detach()

    rl2 = _make_logger(tmp_path, rl._run_id)
    _run_id_var.set(rl2._run_id)
    rl2.attach()
    rl2.info("second session line")
    rl2.detach()

    lines = _read_log(rl2)
    text = "\n".join(lines)
    assert "first session line" in text
    assert "second session line" in text


# ─── log format ──────────────────────────────────────────────────────────────

def test_log_format_has_timestamp_level_context(tmp_path: Path) -> None:
    rl = _make_logger(tmp_path)
    _run_id_var.set(rl._run_id)
    rl.attach()
    rl.info("hello world")
    rl.detach()

    lines = _read_log(rl)
    # Find the "hello world" line (skip "pipeline run started/ended")
    target = next(ln for ln in lines if "hello world" in ln)
    # Format: 2026-..Z  INFO   [pipeline                     ]  hello world
    assert "INFO" in target
    assert "[pipeline" in target
    assert "hello world" in target


def test_log_format_error_level(tmp_path: Path) -> None:
    rl = _make_logger(tmp_path)
    _run_id_var.set(rl._run_id)
    rl.attach()
    rl.error("something broke")
    rl.detach()

    lines = _read_log(rl)
    target = next(ln for ln in lines if "something broke" in ln)
    assert "ERROR" in target


# ─── multi-run isolation ─────────────────────────────────────────────────────

def test_run_isolation_different_files(tmp_path: Path) -> None:
    """Two concurrent loggers must write to separate files without cross-contamination."""
    run_id_a = "pipe_20260513-000001_0001"
    run_id_b = "pipe_20260513-000001_0002"
    rl_a = RunLogger(run_id_a, tmp_path / run_id_a / "run.log")
    rl_b = RunLogger(run_id_b, tmp_path / run_id_b / "run.log")

    # Simulate run A's asyncio context
    ctx_a = contextvars.copy_context()
    ctx_a.run(_run_id_var.set, run_id_a)
    ctx_a.run(rl_a.attach)
    ctx_a.run(rl_a.info, "message from run A")
    ctx_a.run(rl_a.detach)

    # Simulate run B's asyncio context
    ctx_b = contextvars.copy_context()
    ctx_b.run(_run_id_var.set, run_id_b)
    ctx_b.run(rl_b.attach)
    ctx_b.run(rl_b.info, "message from run B")
    ctx_b.run(rl_b.detach)

    lines_a = (tmp_path / run_id_a / "run.log").read_text().splitlines()
    lines_b = (tmp_path / run_id_b / "run.log").read_text().splitlines()

    assert any("message from run A" in ln for ln in lines_a)
    assert not any("message from run B" in ln for ln in lines_a)
    assert any("message from run B" in ln for ln in lines_b)
    assert not any("message from run A" in ln for ln in lines_b)


# ─── task_context ─────────────────────────────────────────────────────────────

def test_task_context_injects_label(tmp_path: Path) -> None:
    rl = _make_logger(tmp_path)
    _run_id_var.set(rl._run_id)
    rl.attach()
    with rl.task_context("my_step", "my_task"):
        rl.info("inside task")
    rl.detach()

    lines = _read_log(rl)
    target = next(ln for ln in lines if "inside task" in ln)
    assert "my_step/my_task" in target


def test_task_context_resets_after_exit(tmp_path: Path) -> None:
    rl = _make_logger(tmp_path)
    _run_id_var.set(rl._run_id)
    rl.attach()
    with rl.task_context("s", "t"):
        pass
    # After context, _task_ctx_var should be reset to None
    assert _task_ctx_var.get(None) is None
    rl.detach()


def test_task_context_logs_start_and_done(tmp_path: Path) -> None:
    rl = _make_logger(tmp_path)
    _run_id_var.set(rl._run_id)
    rl.attach()
    with rl.task_context("step1", "task1"):
        pass
    rl.detach()

    text = rl._log_path.read_text()
    assert "task start: step1/task1" in text
    assert "task done: step1/task1" in text


def test_task_context_logs_error_on_exception(tmp_path: Path) -> None:
    rl = _make_logger(tmp_path)
    _run_id_var.set(rl._run_id)
    rl.attach()
    with pytest.raises(ValueError):
        with rl.task_context("step1", "fail_task"):
            raise ValueError("boom")
    rl.detach()

    text = rl._log_path.read_text()
    assert "task failed: step1/fail_task" in text
    assert "boom" in text


# ─── stdout capture (_RunAwareStream tested directly) ────────────────────────

def test_stream_adapter_routes_to_logger_when_run_active(tmp_path: Path) -> None:
    from pipeline_engine.core.run_logger import _RunAwareStream

    rl = _make_logger(tmp_path)
    _run_id_var.set(rl._run_id)
    rl.attach()

    original = io.StringIO()
    stream = _RunAwareStream(original, logging.INFO)
    stream.write("hello from stream\n")
    stream.flush()
    rl.detach()

    text = rl._log_path.read_text()
    assert "hello from stream" in text
    # Nothing went to the original StringIO since run context was active
    assert original.getvalue() == ""


def test_stream_adapter_falls_through_without_run_context(tmp_path: Path) -> None:
    from pipeline_engine.core.run_logger import _RunAwareStream

    _run_id_var.set(None)  # type: ignore[arg-type]
    original = io.StringIO()
    stream = _RunAwareStream(original, logging.INFO)
    stream.write("fallthrough text")
    assert "fallthrough text" in original.getvalue()


def test_stream_adapter_buffers_partial_lines(tmp_path: Path) -> None:
    from pipeline_engine.core.run_logger import _RunAwareStream

    rl = _make_logger(tmp_path)
    _run_id_var.set(rl._run_id)
    rl.attach()

    original = io.StringIO()
    stream = _RunAwareStream(original, logging.INFO)
    stream.write("part1 ")
    stream.write("part2\n")  # Only a newline flushes the buffer
    rl.detach()

    text = rl._log_path.read_text()
    assert "part1 part2" in text


# ─── import fix for contextvars ──────────────────────────────────────────────
import contextvars  # noqa: E402  (needed for test_run_isolation)
