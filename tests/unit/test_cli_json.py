"""Unit tests for pipeline_engine.cli_json utility functions."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import typer

from pipeline_engine.cli_json import (
    emit,
    emit_error,
    parse_log_line,
    read_json_file,
    read_log_tail,
)
from pipeline_engine.core.errors import PipelineError


# ── emit ──────────────────────────────────────────────────────────────────────

def test_emit_writes_json_envelope(capsys):
    emit("list", scope="pipeline", pipelines=[])
    out = capsys.readouterr().out.strip()
    obj = json.loads(out)
    assert obj["ok"] is True
    assert obj["command"] == "list"
    assert obj["scope"] == "pipeline"


# ── emit_error ────────────────────────────────────────────────────────────────

def test_emit_error_pipeline_error(capsys):
    exc = PipelineError("not found", pipeline_id="p1", step_id="s1", task_id="t1")
    result = emit_error("start", exc)
    out = capsys.readouterr().out.strip()
    obj = json.loads(out)
    assert obj["ok"] is False
    assert obj["command"] == "start"
    assert obj["error"]["type"] == "PipelineError"
    assert obj["error"]["pipeline_id"] == "p1"
    assert isinstance(result, typer.Exit)


def test_emit_error_generic_exception(capsys):
    exc = ValueError("something went wrong")
    result = emit_error("load", exc)
    out = capsys.readouterr().out.strip()
    obj = json.loads(out)
    assert obj["ok"] is False
    assert obj["error"]["type"] == "ValueError"
    assert "something went wrong" in obj["error"]["message"]
    assert isinstance(result, typer.Exit)


# ── parse_log_line ────────────────────────────────────────────────────────────

def test_parse_log_line_valid():
    raw = "2026-05-14T09:00:00Z  INFO  [step_a/task_b]  Starting task"
    result = parse_log_line(raw)
    assert result["timestamp"] == "2026-05-14T09:00:00Z"
    assert result["level"] == "INFO"
    assert result["ctx"] == "step_a/task_b"
    assert result["message"] == "Starting task"
    assert result["raw"] == raw


def test_parse_log_line_fallback_for_unparseable():
    raw = "this is not a valid log line"
    result = parse_log_line(raw)
    assert result["timestamp"] is None
    assert result["level"] is None
    assert result["ctx"] is None
    assert result["message"] == raw
    assert result["raw"] == raw


# ── read_json_file ────────────────────────────────────────────────────────────

def test_read_json_file_none_returns_none():
    assert read_json_file(None) is None


def test_read_json_file_empty_string_returns_none():
    assert read_json_file("") is None


def test_read_json_file_nonexistent_returns_none(tmp_path):
    assert read_json_file(str(tmp_path / "no_such.json")) is None


def test_read_json_file_valid(tmp_path):
    f = tmp_path / "data.json"
    f.write_text('{"key": 42}')
    assert read_json_file(str(f)) == {"key": 42}


def test_read_json_file_invalid_json_returns_none(tmp_path):
    f = tmp_path / "bad.json"
    f.write_text("{not valid json")
    assert read_json_file(str(f)) is None


# ── read_log_tail ─────────────────────────────────────────────────────────────

def test_read_log_tail_none_returns_empty():
    assert read_log_tail(None) == []


def test_read_log_tail_nonexistent_returns_empty(tmp_path):
    assert read_log_tail(str(tmp_path / "missing.log")) == []


def test_read_log_tail_returns_last_n_lines(tmp_path):
    f = tmp_path / "run.log"
    f.write_text("\n".join(f"line {i}" for i in range(10)))
    result = read_log_tail(str(f), tail=3)
    assert result == ["line 7", "line 8", "line 9"]


def test_read_log_tail_default_tail(tmp_path):
    f = tmp_path / "run.log"
    lines = [f"line {i}" for i in range(50)]
    f.write_text("\n".join(lines))
    result = read_log_tail(str(f))
    assert len(result) == 50
