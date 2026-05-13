"""REPL 命令补全：为 prompt_toolkit PromptSession 提供动态候选。

架构说明
--------
- PipelineReplCompleter 持有 RunManager 引用，每次补全实时读取 _registry / _runs。
- get_completions 解析 text_before_cursor，按命令语法表分发到各类补全逻辑。
- 用 ThreadedCompleter 包装后注入 PromptSession，避免阻塞 asyncio 事件循环。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Iterable

from prompt_toolkit.completion import Completer, Completion, PathCompleter
from prompt_toolkit.document import Document

if TYPE_CHECKING:
    from pipeline_engine.core.run_manager import RunManager


@dataclass
class _Grammar:
    args: list[str]                                             # positional arg kinds, in order
    flags: frozenset[str] = field(default_factory=frozenset)   # boolean flags
    flag_values: dict[str, str] = field(default_factory=dict)  # flag → kind

    def __post_init__(self) -> None:
        if not isinstance(self.flags, frozenset):
            self.flags = frozenset(self.flags)


COMMANDS: dict[str, _Grammar] = {
    "help":    _Grammar(args=[]),
    "exit":    _Grammar(args=[]),
    "quit":    _Grammar(args=[]),
    "clear":   _Grammar(args=[]),
    "load":    _Grammar(args=["path"]),
    "list":    _Grammar(args=[], flags={"--pipeline", "--instance"}),
    "start":   _Grammar(
                   args=["pipeline_id"],
                   flags={"--step", "--task", "--wait"},
                   flag_values={"--step": "step_id", "--task": "task_ref"},
               ),
    "stop":    _Grammar(args=["ref"]),
    "resume":  _Grammar(args=["ref"], flags={"--include-paused"}),
    "status":  _Grammar(args=["ref"], flags={"--watch"}),
    "inspect": _Grammar(
                   args=["ref"],
                   flags={"--step", "--task"},
                   flag_values={"--step": "step_id", "--task": "task_ref"},
               ),
    "fix":     _Grammar(
                   args=["ref"],
                   flags={"--task", "--output", "--input"},
                   flag_values={"--task": "task_ref", "--output": "path", "--input": "path"},
               ),
    "log":     _Grammar(
                   args=["ref"],
                   flags={"--all", "--errors-only"},
                   flag_values={"--tail": "num", "--offset": "num"},
               ),
}

_PATH_COMPLETER = PathCompleter(only_directories=False, expanduser=True)


class PipelineReplCompleter(Completer):
    """动态 REPL 补全器：命令名 / pipeline_id / instance_id / step / task / 路径。"""

    def __init__(self, rm: "RunManager") -> None:
        self._rm = rm

    def get_completions(self, document: Document, complete_event) -> Iterable[Completion]:  # type: ignore[override]
        text = document.text_before_cursor
        parts = text.split()

        # Determine completed tokens vs the current (partial) token
        if not text or text.endswith(" "):
            completed = list(parts)
            current = ""
        else:
            completed = list(parts[:-1])
            current = parts[-1] if parts else ""

        # No tokens at all, or still typing first token → complete command names
        if not completed:
            yield from self._complete_commands(current)
            return

        cmd = completed[0]
        grammar = COMMANDS.get(cmd)
        if grammar is None:
            # Unknown command — no completions
            return

        rest = completed[1:]  # tokens after command name

        # If last completed token is a flag that takes a value → complete its value
        if rest and rest[-1] in grammar.flag_values:
            kind = grammar.flag_values[rest[-1]]
            yield from self._complete_kind(kind, cmd, completed, current)
            return

        # Current token starts with "--" → complete flag names
        if current.startswith("-"):
            used_flags = {t for t in rest if t.startswith("--")}
            remaining = grammar.flags - used_flags
            for flag in sorted(remaining):
                if flag.startswith(current):
                    yield Completion(flag[len(current):], display=flag)
            return

        # Offer remaining flags when input is empty and no positional is needed
        if not current and not self._positional_needed(grammar, rest):
            used_flags = {t for t in rest if t.startswith("--")}
            remaining = grammar.flags - used_flags
            for flag in sorted(remaining):
                yield Completion(flag, display=flag)
            return

        # Positional argument completion
        pos_idx = self._positional_index(grammar, rest)
        if pos_idx < len(grammar.args):
            kind = grammar.args[pos_idx]
            yield from self._complete_kind(kind, cmd, completed, current)

    # ─── kind dispatch ──────────────────────────────────────────────────────

    def _complete_kind(
        self, kind: str, cmd: str, completed: list[str], current: str
    ) -> Iterable[Completion]:
        if kind == "pipeline_id":
            yield from self._complete_pipeline_ids(current)
        elif kind == "ref":
            yield from self._complete_instance_ids(current)
        elif kind == "step_id":
            pid = self._extract_pipeline_context(cmd, completed)
            if pid:
                yield from self._complete_step_ids(pid, current)
        elif kind == "task_ref":
            pid = self._extract_pipeline_context(cmd, completed)
            if pid:
                yield from self._complete_task_refs(pid, current)
        elif kind == "num":
            return  # free numeric input — no completion candidates
        elif kind == "path":
            sub_doc = Document(current)
            yield from _PATH_COMPLETER.get_completions(sub_doc, None)

    # ─── concrete completions ───────────────────────────────────────────────

    def _complete_commands(self, current: str) -> Iterable[Completion]:
        for cmd_name in sorted(COMMANDS):
            if cmd_name.startswith(current.lower()):
                yield Completion(cmd_name[len(current):], display=cmd_name)

    def _complete_pipeline_ids(self, current: str) -> Iterable[Completion]:
        for pid, spec in sorted(self._rm._registry.items()):
            if pid.lower().startswith(current.lower()):
                meta = f"{spec.pipeline.type} | {spec.pipeline.name}"
                yield Completion(pid[len(current):], display=pid, display_meta=meta)

    def _complete_instance_ids(self, current: str) -> Iterable[Completion]:
        for run_id, ctx in sorted(self._rm._runs.items()):
            if run_id.lower().startswith(current.lower()):
                pid = ctx.pipeline_id
                status_val = self._get_status(ctx)
                meta = f"pipeline={pid} | status={status_val}"
                yield Completion(run_id[len(current):], display=run_id, display_meta=meta)

    def _complete_step_ids(self, pid: str, current: str) -> Iterable[Completion]:
        spec = self._rm._registry.get(pid)
        if not spec:
            return
        for idx, step in enumerate(spec.steps):
            if step.id.lower().startswith(current.lower()):
                yield Completion(
                    step.id[len(current):],
                    display=step.id,
                    display_meta=f"step #{idx + 1}",
                )

    def _complete_task_refs(self, pid: str, current: str) -> Iterable[Completion]:
        spec = self._rm._registry.get(pid)
        if not spec:
            return
        # If current contains '/' → user typed "step_id/", complete only task part
        if "/" in current:
            step_prefix, task_prefix = current.split("/", 1)
            for step in spec.steps:
                if step.id == step_prefix:
                    for task in step.tasks:
                        if task.id.lower().startswith(task_prefix.lower()):
                            full = f"{step.id}/{task.id}"
                            yield Completion(
                                full[len(current):],
                                display=full,
                                display_meta=f"task in {step.id}",
                            )
            return
        # No slash: offer all step/task combinations
        for step in spec.steps:
            for task in step.tasks:
                full = f"{step.id}/{task.id}"
                if full.lower().startswith(current.lower()):
                    yield Completion(
                        full[len(current):],
                        display=full,
                        display_meta=f"task in {step.id}",
                    )

    # ─── helpers ────────────────────────────────────────────────────────────

    def _positional_index(self, grammar: _Grammar, rest: list[str]) -> int:
        """Count completed positional (non-flag) arguments in rest."""
        count = 0
        i = 0
        while i < len(rest):
            tok = rest[i]
            if tok.startswith("--"):
                if tok in grammar.flag_values:
                    i += 2  # consume flag + its value token
                else:
                    i += 1  # boolean flag
            else:
                count += 1
                i += 1
        return count

    def _positional_needed(self, grammar: _Grammar, rest: list[str]) -> bool:
        return self._positional_index(grammar, rest) < len(grammar.args)

    def _extract_pipeline_context(self, cmd: str, completed: list[str]) -> str | None:
        """Derive pipeline_id from already-typed tokens for step/task completion."""
        grammar = COMMANDS.get(cmd)
        if not grammar:
            return None
        rest = completed[1:]
        positionals: list[str] = []
        i = 0
        while i < len(rest):
            tok = rest[i]
            if tok.startswith("--"):
                if tok in grammar.flag_values:
                    i += 2
                else:
                    i += 1
            else:
                positionals.append(tok)
                i += 1
        if not positionals:
            return None
        first_pos = positionals[0]
        if cmd == "start":
            return first_pos if first_pos in self._rm._registry else None
        # For instance_id args: look up the RunContext and read .pipeline_id directly
        # (avoids brittle string-parsing since pipeline_id itself may contain '_')
        if first_pos in self._rm._runs:
            return self._rm._runs[first_pos].pipeline_id
        if first_pos in self._rm._registry:
            return first_pos
        return None

    @staticmethod
    def _get_status(ctx) -> str:
        try:
            return ctx.state_manager._state.status.value
        except Exception:
            return "?"
