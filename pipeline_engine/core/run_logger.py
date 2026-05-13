"""per-run 日志基础设施：隔离、四源捕获、REPL 高亮友好格式。

设计说明
--------
每次 pipeline run 对应一个 RunLogger 实例，负责把四类输出汇聚到
<run_dir>/run.log，按列固定格式落盘：

    2026-05-13T09:30:24.123Z  INFO   [export_dxf/validate     ]  task start
    2026-05-13T09:30:24.456Z  ERROR  [export_dxf/validate     ]  DEMO_FAIL

四源：
1. 引擎生命周期 — scheduler 显式调用 RunLogger.info/error。
2. Python logging — pipeline_engine.* logger 均通过 propagate 流入
   挂在 "pipeline_engine" 上的 FileHandler（受 _RunFilter 隔离）。
3. Task 自定义 — BaseTask.logger 是 pipeline_engine.task.{task_id} 的
   子 logger，自动流入同一 FileHandler。
4. stdout/stderr — 全局替换为 _RunAwareStdout，按 ContextVar 路由到
   对应 run 的 logger（无 run 上下文时透传给原始 stdout）。

多 run 隔离通过两个 ContextVar 实现：
- _run_id_var  : 当前 asyncio Task 所属的 run_id（在 attach() 中设置）。
- _task_ctx_var: 当前正在执行的 (step_id, task_id) 元组。
asyncio Task 的 copy-on-write context 机制保证并发 run 之间互不干扰。
"""
from __future__ import annotations

import contextvars
import io
import logging
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

# ─── ContextVar：在 asyncio Task 内传播，不同 Task 互不影响 ───────────────────
_run_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "pipeline_run_id", default=None
)
_task_ctx_var: contextvars.ContextVar[tuple[str, str] | None] = contextvars.ContextVar(
    "pipeline_task_ctx", default=None
)

# ─── 全局活跃 logger 注册表（供 stdout 路由） ─────────────────────────────────
_active_loggers: dict[str, "RunLogger"] = {}
_stdout_installed: bool = False
_original_stdout: io.TextIOBase | None = None
_original_stderr: io.TextIOBase | None = None


# ─── 格式器 ──────────────────────────────────────────────────────────────────

class _RunFormatter(logging.Formatter):
    """固定列宽格式：时间(Z) + 级别 + [context] + 消息。"""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:  # noqa: N802
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(record.msecs):03d}Z"

    def format(self, record: logging.LogRecord) -> str:
        ctx = getattr(record, "ctx_label", "pipeline")
        ts = self.formatTime(record)
        level = record.levelname[:5].ljust(5)
        return f"{ts}  {level}  [{ctx:<28}]  {record.getMessage()}"


# ─── 过滤器 ──────────────────────────────────────────────────────────────────

class _RunFilter(logging.Filter):
    """只放行属于本 run 的 record，并注入 ctx_label extra 字段。"""

    def __init__(self, run_id: str) -> None:
        super().__init__()
        self._run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        if _run_id_var.get(None) != self._run_id:
            return False
        ctx = _task_ctx_var.get(None)
        record.ctx_label = f"{ctx[0]}/{ctx[1]}" if ctx else "pipeline"  # type: ignore[attr-defined]
        return True


# ─── stdout/stderr 路由包装器 ────────────────────────────────────────────────

class _RunAwareStream(io.TextIOBase):
    """将 write() 路由到当前 run 的 logger；无 run 上下文时透传原始流。"""

    def __init__(self, original: io.TextIOBase, level: int = logging.INFO) -> None:
        self._original = original
        self._level = level
        self._buffers: dict[str, str] = {}  # run_id → partial line buffer

    def write(self, text: str) -> int:  # type: ignore[override]
        run_id = _run_id_var.get(None)
        if run_id and run_id in _active_loggers:
            rl = _active_loggers[run_id]
            buf = self._buffers.get(run_id, "") + text
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                if line.strip():
                    rl._pe_logger.log(self._level, "%s", line)
            self._buffers[run_id] = buf
            return len(text)
        return self._original.write(text)  # type: ignore[return-value]

    def flush(self) -> None:
        for run_id, buf in list(self._buffers.items()):
            if buf.strip() and run_id in _active_loggers:
                _active_loggers[run_id]._pe_logger.log(self._level, "%s", buf)
                self._buffers[run_id] = ""
        self._original.flush()

    @property
    def encoding(self) -> str:
        return getattr(self._original, "encoding", "utf-8")

    def fileno(self) -> int:
        return self._original.fileno()

    def isatty(self) -> bool:
        run_id = _run_id_var.get(None)
        if run_id and run_id in _active_loggers:
            return False
        return self._original.isatty()  # type: ignore[return-value]


def _install_stdout_wrapper() -> None:
    """全局替换 sys.stdout/stderr（幂等）。"""
    global _stdout_installed, _original_stdout, _original_stderr
    if _stdout_installed:
        return
    _original_stdout = sys.stdout  # type: ignore[assignment]
    _original_stderr = sys.stderr  # type: ignore[assignment]
    sys.stdout = _RunAwareStream(sys.stdout, logging.INFO)   # type: ignore[assignment]
    sys.stderr = _RunAwareStream(sys.stderr, logging.WARNING)  # type: ignore[assignment]
    _stdout_installed = True


# ─── RunLogger（主类） ────────────────────────────────────────────────────────

class RunLogger:
    """单次 pipeline run 的日志管理器。"""

    def __init__(self, run_id: str, log_path: Path) -> None:
        self._run_id = run_id
        self._log_path = log_path
        self._handler: logging.FileHandler | None = None
        self._pe_logger = logging.getLogger("pipeline_engine")

    def attach(self) -> None:
        """安装 FileHandler，设置当前 asyncio Task 的 run_id contextvar（幂等）。"""
        # 设置 contextvar：此处调用在 asyncio.Task 内，只影响本 Task 及其子任务
        _run_id_var.set(self._run_id)

        # 幂等：若已挂载则跳过
        for h in self._pe_logger.handlers:
            if getattr(h, "_run_id", None) == self._run_id:
                return

        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(str(self._log_path), mode="a", encoding="utf-8")
        handler._run_id = self._run_id  # type: ignore[attr-defined]
        handler.addFilter(_RunFilter(self._run_id))
        handler.setFormatter(_RunFormatter())
        handler.setLevel(logging.DEBUG)
        self._pe_logger.addHandler(handler)
        if self._pe_logger.level == logging.NOTSET or self._pe_logger.level > logging.DEBUG:
            self._pe_logger.setLevel(logging.DEBUG)
        self._handler = handler
        _active_loggers[self._run_id] = self

        _install_stdout_wrapper()
        self._pe_logger.info("pipeline run started: %s", self._run_id)

    def detach(self) -> None:
        """卸载 FileHandler，关闭文件（幂等）。"""
        if self._handler is None:
            return
        self._pe_logger.info("pipeline run ended: %s", self._run_id)
        self._pe_logger.removeHandler(self._handler)
        try:
            self._handler.flush()
            self._handler.close()
        except Exception:
            pass
        self._handler = None
        _active_loggers.pop(self._run_id, None)

    @contextmanager
    def task_context(self, step_id: str, task_id: str) -> Generator[None, None, None]:
        """设置 task 上下文 contextvar，打 start/done/failed 日志行。"""
        token_ctx = _task_ctx_var.set((step_id, task_id))
        self._pe_logger.info("task start: %s/%s", step_id, task_id)
        try:
            yield
        except Exception as exc:
            self._pe_logger.error("task failed: %s/%s — %s: %s",
                                  step_id, task_id, type(exc).__name__, exc)
            raise
        else:
            self._pe_logger.info("task done: %s/%s", step_id, task_id)
        finally:
            _task_ctx_var.reset(token_ctx)

    def info(self, msg: str, *args: object) -> None:
        self._pe_logger.info(msg, *args)

    def warning(self, msg: str, *args: object) -> None:
        self._pe_logger.warning(msg, *args)

    def error(self, msg: str, *args: object) -> None:
        self._pe_logger.error(msg, *args)
