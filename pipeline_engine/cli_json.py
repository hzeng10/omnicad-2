"""JSON 输出工具：为所有 CLI 子命令提供统一的信封格式。

信封格式
--------
成功::

    {"ok": true, "command": "<cmd>", ...payload}

失败::

    {"ok": false, "command": "<cmd>",
     "error": {"message": "...", "type": "...", "pipeline_id": null, ...}}

失败时 exit code = 1；JSON 仍写 stdout，保证 AI Agent 可以 json.loads(stdout)。
autoload 的 INFO/WARNING 行写 stderr，不污染 stdout JSON 流。
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

import typer

from pipeline_engine.core.errors import PipelineError

# 日志行解析正则：<timestamp>  <LEVEL>  [<ctx>]  <message>
_LOG_LINE_RE = re.compile(r"^(\S+)\s+(\w+)\s+\[([^\]]*)\]\s+(.*)")


def emit(command: str, **payload: Any) -> None:
    """打印成功 JSON envelope 到 stdout。"""
    obj: dict[str, Any] = {"ok": True, "command": command}
    obj.update(payload)
    typer.echo(json.dumps(obj, ensure_ascii=False, default=str))


def emit_error(command: str, exc: BaseException, exit_code: int = 1) -> "typer.Exit":
    """打印失败 JSON envelope 到 stdout，返回 typer.Exit；调用方 raise 它。"""
    if isinstance(exc, PipelineError):
        err: dict[str, Any] = {
            "message": str(exc),
            "type": "PipelineError",
            "pipeline_id": exc.pipeline_id,
            "step_id": exc.step_id,
            "task_id": exc.task_id,
        }
    else:
        err = {
            "message": str(exc),
            "type": type(exc).__name__,
        }
    obj: dict[str, Any] = {"ok": False, "command": command, "error": err}
    typer.echo(json.dumps(obj, ensure_ascii=False, default=str))
    return typer.Exit(exit_code)


def parse_log_line(raw: str) -> dict[str, Any]:
    """将单行 run.log 解析为结构化字典。

    格式：``<timestamp>  LEVEL  [ctx]  message``
    无法解析时回退到 ``{level: null, timestamp: null, ctx: null, message: raw, raw: raw}``。
    """
    m = _LOG_LINE_RE.match(raw)
    if m:
        return {
            "timestamp": m.group(1),
            "level": m.group(2).upper(),
            "ctx": m.group(3).strip(),
            "message": m.group(4),
            "raw": raw,
        }
    return {"timestamp": None, "level": None, "ctx": None, "message": raw, "raw": raw}


def read_json_file(path_str: str | None) -> Any:
    """安全读取 JSON 文件内容；文件不存在或解析失败返回 None。"""
    if not path_str:
        return None
    p = Path(path_str)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return None


def read_log_tail(path_str: str | None, tail: int = 100) -> list[str]:
    """安全读取 run.log 末尾 N 行；文件不存在返回空列表。"""
    if not path_str:
        return []
    p = Path(path_str)
    if not p.exists():
        return []
    lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
    return lines[-tail:] if tail > 0 else lines
