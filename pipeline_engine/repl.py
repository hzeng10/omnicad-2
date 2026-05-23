"""交互式 REPL：基于 prompt_toolkit + asyncio 的非阻塞命令行界面。

架构说明
--------
- REPL 作为 asyncio 协程运行，与后台调度器共享同一事件循环。
- 用户输入不会阻塞任务执行；任务状态更新不会干扰命令输入。
- 命令解析使用 shlex.split，支持带引号的路径参数。
- 终端渲染使用 Rich：进度表格、彩色状态、JSON 格式化。

``_get_flag`` 拒绝将相邻 ``--xxx`` 参数当作值，返回 None 并提示用法。
``start`` 命令支持多个 pipeline_id，行为与 CLI 一致。
"""
from __future__ import annotations

import asyncio
import json
import re
import shlex
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rich import box
from rich.console import Console
from rich.live import Live
from rich.table import Table

from pipeline_engine.core import storage
from pipeline_engine.core.errors import PipelineError
from pipeline_engine.core.run_manager import RunManager
from pipeline_engine.i18n import t
from pipeline_engine.models.runtime_state import PipelineRunState, Status

if TYPE_CHECKING:
    from pipeline_engine.service import PipelineService

# 日志行中第二列级别的正则（格式：<timestamp>  LEVEL  [ctx]  msg）
_LOG_LEVEL_RE = re.compile(r"^\S+\s+(\w+)\s+")

console = Console()

def _build_help() -> str:
    """Build the REPL help text in the active language."""
    return f"""\
[bold cyan]{t('repl.help.header')}[/bold cyan]
  load <path> [<path>...]                                          {t('repl.help.load')}
  list [--pipeline]                                                {t('repl.help.list_pipeline')}
  list --instance                                                  {t('repl.help.list_instance')}
  start <id> [<id>...] [--step S] [--task T]                      {t('repl.help.start')}
  stop <instance_id>                                               {t('repl.help.stop')}
  resume <instance_id> [--include-paused]                         {t('repl.help.resume')}
  status <instance_id> [--watch]                                   {t('repl.help.status')}
  status --all                                                     {t('repl.help.status_all')}
  inspect <instance_id> [--step S] [--task T]                     {t('repl.help.inspect')}
  fix <instance_id> --task T --output PATH                         {t('repl.help.fix_output')}
  fix <instance_id> --task T --input PATH                          {t('repl.help.fix_input')}
  log <instance_id> [--tail N] [--offset N] [--all] [--errors-only]  {t('repl.help.log')}
  clear                                                            {t('repl.help.clear')}
  help                                                             {t('repl.help.help')}
  exit / quit                                                      {t('repl.help.exit')}

[dim]{t('repl.hint.instance_id_fmt')}[/dim]
[dim]{t('repl.hint.tab_complete')}[/dim]
"""


# ─── 公共入口 ─────────────────────────────────────────────────────────────────

async def run_repl(
    workspace: Path,
    *,
    pipelines_dir: "Path | None" = None,
    no_autoload: bool = False,
) -> None:
    """启动交互式 REPL（优先使用 prompt_toolkit，不可用时退化为基础模式）。

    pipelines_dir: autoload 扫描目录（None → ./pipelines）。
    no_autoload:   True 时跳过 autoload。
    """
    try:
        from prompt_toolkit import PromptSession
        from prompt_toolkit.completion import ThreadedCompleter
        from prompt_toolkit.history import InMemoryHistory
        from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
        from pipeline_engine.repl_completion import PipelineReplCompleter
    except ImportError:
        console.print(f"[red]{t('repl.err.prompt_toolkit_missing')}[/red]")
        await _run_repl_basic(workspace, pipelines_dir=pipelines_dir, no_autoload=no_autoload)
        return

    from pipeline_engine.service import PipelineService
    rm = RunManager(workspace)
    base_dir = pipelines_dir if pipelines_dir is not None else Path.cwd() / "pipelines"
    svc = PipelineService(rm, pipelines_dir=base_dir, no_autoload=no_autoload)
    await _bootstrap_repl(svc)

    session: PromptSession = PromptSession(
        history=InMemoryHistory(),
        auto_suggest=AutoSuggestFromHistory(),
        completer=ThreadedCompleter(PipelineReplCompleter(rm)),
        complete_while_typing=True,
    )

    from pipeline_engine import branding as _branding
    _cfg = _branding.load_branding()
    _branding.print_banner(console, _cfg, workspace=workspace)

    while True:
        try:
            raw = await session.prompt_async(f"{_cfg.prompt}> ")
        except (EOFError, KeyboardInterrupt):
            console.print(f"\n[yellow]{t('repl.info.exit_hint')}[/yellow]")
            continue

        raw = raw.strip()
        if not raw:
            continue

        try:
            await _dispatch(svc, raw)
        except SystemExit:
            break
        except PipelineError as e:
            console.print(f"[red]{t('repl.label.err')}:[/red] {e}")
        except Exception as e:
            console.print(f"[red]{t('repl.label.unexpected_err')}:[/red] {e}")
            console.print_exception(max_frames=5)


async def _run_repl_basic(
    workspace: Path,
    *,
    pipelines_dir: "Path | None" = None,
    no_autoload: bool = False,
) -> None:
    """无 prompt_toolkit 时的简化 REPL（无历史/补全）。"""
    from pipeline_engine.service import PipelineService
    rm = RunManager(workspace)
    base_dir = pipelines_dir if pipelines_dir is not None else Path.cwd() / "pipelines"
    svc = PipelineService(rm, pipelines_dir=base_dir, no_autoload=no_autoload)
    await _bootstrap_repl(svc)
    from pipeline_engine import branding as _branding
    _cfg = _branding.load_branding()
    _branding.print_banner(console, _cfg, workspace=workspace)

    while True:
        try:
            raw = input(f"{_cfg.prompt}> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not raw:
            continue
        try:
            await _dispatch(svc, raw)
        except SystemExit:
            break
        except PipelineError as e:
            console.print(f"[red]{t('repl.label.err')}:[/red] {e}")


# ─── 命令分发 ─────────────────────────────────────────────────────────────────

async def _bootstrap_repl(svc: "PipelineService") -> None:
    """Bootstrap the REPL: reload registry from disk and autoload pipelines."""
    await svc.bootstrap(restore_runs=False, restore_writeback=False)


async def _dispatch(svc: "PipelineService", raw: str) -> None:
    """解析并分发单条命令。"""
    rm = svc.rm  # direct RunManager access for list/load rendering
    try:
        argv = shlex.split(raw)
    except ValueError as e:
        console.print(f"[red]{t('repl.err.parse_error')}:[/red] {e}")
        return

    if not argv:
        return

    cmd, *args = argv

    match cmd:
        case "help":
            console.print(_build_help())

        case "exit" | "quit":
            # 退出前提示仍有活跃 run
            active = [r for r in rm.list_runs() if r["active"]]
            if active:
                console.print(
                    f"[yellow]{t('repl.label.warn')}:[/yellow] "
                    + t("repl.warn.exit_active_runs").format(n=len(active))
                )
            raise SystemExit

        case "load":
            if not args:
                console.print(f"[yellow]{t('repl.label.usage')}:[/yellow] {t('repl.usage.load')}")
                return
            result = await svc.cmd_load([Path(p) for p in args])
            for item in result["loaded"]:
                if item["ok"]:
                    console.print(f"[green]{t('repl.success.load_ok')}:[/green] {item['pipeline_id']}")
                else:
                    console.print(f"[red]{t('repl.success.load_fail')}:[/red] {item['path']}: {item.get('error', '')}")

        case "list":
            flags = set(args)
            if "--instance" in flags or "--runs" in flags:
                await _print_instances(rm)
            else:
                _print_pipelines(rm)

        case "start":
            await _cmd_start(svc, args)

        case "stop":
            await _cmd_stop(svc, args)

        case "resume":
            await _cmd_resume(svc, args)

        case "status":
            await _cmd_status(svc, args)

        case "inspect":
            await _cmd_inspect(svc, args)

        case "fix":
            await _cmd_fix(svc, args)

        case "log":
            await _cmd_log(svc, args)

        case "clear":
            console.clear()

        case _:
            console.print(f"[red]{t('repl.err.unknown_cmd')}:[/red] {cmd!r}  ({t('repl.err.unknown_cmd_hint')})")


# ─── 各命令处理函数 ───────────────────────────────────────────────────────────

async def _cmd_start(svc: "PipelineService", args: list[str]) -> None:
    """start 命令处理器：支持多个 pipeline_id，行为与 CLI start 子命令一致。"""
    if not args:
        console.print(f"[yellow]{t('repl.label.usage')}:[/yellow] {t('repl.usage.start')}")
        return

    step_id = _get_flag(args, "--step")
    task_id = _get_flag(args, "--task")

    # 提取非 flag 参数作为 pipeline_id 列表
    pipeline_ids = [a for a in args if not a.startswith("--") and a != step_id and a != task_id]

    if not pipeline_ids:
        console.print(f"[yellow]{t('repl.label.usage')}:[/yellow] {t('repl.usage.start')}")
        return

    result = await svc.cmd_start(pipeline_ids, step=step_id, task=task_id, wait=False)
    for r in result["runs"]:
        if r["ok"]:
            console.print(f"[green]{t('repl.success.start_ok')}:[/green] {r['run_id']}  (pipeline: {r['pipeline_id']})")
        else:
            console.print(
                f"[red]{t('repl.label.err')}:[/red] "
                + t("repl.err.pipeline_start_failed").format(pipeline_id=r["pipeline_id"], error=r.get("error", ""))
            )


async def _cmd_stop(svc: "PipelineService", args: list[str]) -> None:
    if not args:
        console.print(f"[yellow]{t('repl.label.usage')}:[/yellow] {t('repl.usage.stop')}")
        return
    ref = args[0]
    await svc.cmd_stop(ref)
    console.print(f"[yellow]{t('repl.success.stop_ok')}:[/yellow] {ref}")


async def _cmd_resume(svc: "PipelineService", args: list[str]) -> None:
    if not args:
        console.print(f"[yellow]{t('repl.label.usage')}:[/yellow] {t('repl.usage.resume')}")
        return
    ref = args[0]
    include_paused = "--include-paused" in args
    # Non-blocking: fire the resume task and return immediately to the REPL prompt.
    run_id = await svc.rm.resume(ref, include_paused=include_paused)
    console.print(f"[green]{t('repl.success.resume_ok')}:[/green] {run_id}")


async def _cmd_status(svc: "PipelineService", args: list[str]) -> None:
    if "--all" in args:
        await _print_instances(svc.rm)
        return

    if not args:
        console.print(f"[yellow]{t('repl.label.usage')}:[/yellow] {t('repl.usage.status')}")
        return

    ref = args[0]
    watch = "--watch" in args

    if watch:
        await _watch_status(svc.rm, ref)
    else:
        state = await svc.rm.get_run_state(ref)
        _render_status(state)


async def _cmd_inspect(svc: "PipelineService", args: list[str]) -> None:
    if not args:
        console.print(f"[yellow]{t('repl.label.usage')}:[/yellow] {t('repl.usage.inspect')}")
        return
    ref = args[0]
    rest = args[1:]
    step_id = _get_flag(rest, "--step")
    task_id = _get_flag(rest, "--task")
    state = await svc.rm.get_run_state(ref)
    _render_inspect(state, step_id, task_id)


async def _cmd_fix(svc: "PipelineService", args: list[str]) -> None:
    if len(args) < 1:
        console.print(f"[yellow]{t('repl.label.usage')}:[/yellow] {t('repl.usage.fix')}")
        return
    ref = args[0]
    rest = args[1:]
    task_locator = _get_flag(rest, "--task")
    output_path = _get_flag(rest, "--output")
    input_path = _get_flag(rest, "--input")

    if not task_locator:
        console.print(f"[red]{t('repl.label.err')}:[/red] {t('repl.err.fix_task_missing')}")
        return
    if not output_path and not input_path:
        console.print(f"[red]{t('repl.label.err')}:[/red] {t('repl.err.fix_path_missing')}")
        return

    result = await svc.cmd_fix(
        ref,
        task_locator,
        output_path=Path(output_path) if output_path else None,
        input_path=Path(input_path) if input_path else None,
    )
    if result["mode"] == "output":
        console.print(f"[green]{t('repl.success.fix_output').format(task=task_locator)}[/green]")
    else:
        console.print(f"[green]{t('repl.success.fix_input').format(task=task_locator)}[/green]")


# ─── 渲染工具 ────────────────────────────────────────────────────────────────

_STATUS_COLOR = {
    Status.NEW:     "dim",
    Status.RUNNING: "bold cyan",
    Status.PAUSED:  "yellow",
    Status.SUCCESS: "green",
    Status.FAILED:  "bold red",
    Status.SKIPPED: "blue",
    Status.FIXED:   "magenta",
}


def _colorize(status: Status) -> str:
    """为状态值添加 Rich 颜色标签。"""
    color = _STATUS_COLOR.get(status, "white")
    return f"[{color}]{status.value}[/{color}]"


def _render_status(state: PipelineRunState) -> None:
    """渲染 run 整体状态表格。"""
    from pipeline_engine.view_model import build_pipeline_status_view
    view = build_pipeline_status_view(state)

    table = Table(
        title=f"Run: {view.run_id}  |  Pipeline: {view.pipeline_id}",
        box=box.ROUNDED,
        show_lines=True,
    )
    table.add_column(t("repl.col.step"), style="bold")
    table.add_column(t("repl.col.task"))
    table.add_column(t("repl.col.status"), justify="center")
    table.add_column(t("repl.col.progress"), justify="right")
    table.add_column(t("repl.col.error"), style="dim red", no_wrap=False, max_width=50)

    for step_id, step_view in view.steps.items():
        first = True
        for task_id, tv in step_view.tasks.items():
            step_label = step_id if first else ""
            first = False
            table.add_row(
                step_label,
                task_id,
                _colorize(tv.status),
                f"{tv.progress}%",
                tv.error or "",
            )
        if not step_view.tasks:
            table.add_row(step_id, "—", _colorize(step_view.status), "", "")

    console.print(table)
    console.print(f"{t('repl.label.pipeline_status')}{_colorize(view.status)}")


async def _watch_status(rm: RunManager, ref: str, refresh: float = 0.5) -> None:
    """持续刷新状态表格直到 run 结束（Live 模式）。"""
    ctx = await rm._get_ctx(ref)

    with Live(console=console, refresh_per_second=int(1 / refresh)) as live:
        while True:
            try:
                state = await rm.get_run_state(ref)
            except PipelineError:
                break
            live.update(_build_status_renderable(state))
            if not ctx.is_active():
                break
            await asyncio.sleep(refresh)
        # 最后刷新一次显示最终状态
        try:
            state = await rm.get_run_state(ref)
            live.update(_build_status_renderable(state))
        except PipelineError:
            pass


def _build_status_renderable(state: PipelineRunState) -> Table:
    """构建 Live 刷新用的状态表格（不带分隔线，轻量）。"""
    table = Table(
        title=f"Run: {state.run_id}  pipeline: {state.pipeline_id}  [{state.status.value}]",
        box=box.MINIMAL_DOUBLE_HEAD,
    )
    table.add_column(t("repl.col.step"))
    table.add_column(t("repl.col.task"))
    table.add_column(t("repl.col.status"), justify="center")
    table.add_column(t("repl.col.progress"), justify="right")

    for step_id, step_state in state.steps.items():
        first = True
        for task_id, ts in step_state.tasks.items():
            table.add_row(
                step_id if first else "",
                task_id,
                _colorize(ts.status),
                f"{ts.progress}%",
            )
            first = False
    return table


def _render_inspect(
    state: PipelineRunState,
    step_id: str | None,
    task_id: str | None,
) -> None:
    """渲染 task 详情：无 step_id 时展示整体状态；有时展示指定 task。"""
    if step_id is None:
        _render_status(state)
        return

    step_state = state.steps.get(step_id)
    if step_state is None:
        console.print(f"[red]{t('repl.err.step_not_found').format(step_id=step_id)}[/red]")
        return

    if task_id is None:
        for tid, ts in step_state.tasks.items():
            _render_task_detail(tid, ts)
        return

    ts = step_state.tasks.get(task_id)
    if ts is None:
        console.print(f"[red]{t('repl.err.task_not_found').format(task_id=task_id, step_id=step_id)}[/red]")
        return
    _render_task_detail(task_id, ts)


def _render_task_detail(task_id: str, ts) -> None:
    """渲染单个 task 的详细信息（状态/进度/错误/输入输出/日志）。"""
    from pipeline_engine.view_model import build_task_detail_view
    view = build_task_detail_view(ts, log_tail_size=200)

    console.rule(f"[bold]{task_id}[/bold]")
    console.print(f"{t('repl.label.status')}{_colorize(view.status)}")
    console.print(f"{t('repl.label.progress')}{view.progress}%")
    if view.error:
        console.print(f"[red]{t('repl.label.error_detail')}{view.error}[/red]")
    if view.stack_trace:
        console.print(f"[dim]{view.stack_trace}[/dim]")
    if view.fixed_by:
        console.print(f"[magenta]{t('repl.label.fix_method')}{view.fixed_by}[/magenta]")

    for label, path_attr, content in (
        (t("repl.label.input"), view.input_path, view.input),
        (t("repl.label.output"), view.output_path, view.output),
    ):
        if path_attr:
            p = Path(path_attr)
            console.print(f"\n[bold]{label}[/bold] ({p}):")
            if content is not None:
                console.print_json(json.dumps(content))
            elif p.exists():
                # read_json_file returned None (parse failure) — fall back to raw text
                lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
                console.print("\n".join(lines[:100]))
            else:
                console.print(f"[dim]{t('repl.label.file_not_exist')}[/dim]")

    if view.log_path:
        console.print(f"\n[bold]{t('repl.label.log')}[/bold] ({view.log_path}):")
        if view.log_tail:
            console.print("\n".join(view.log_tail))


async def _cmd_log(svc: "PipelineService", args: list[str]) -> None:
    """log 命令：分页显示指定 instance 的 run.log，ERROR 行高亮红色。"""
    if not args:
        console.print(
            f"[yellow]{t('repl.label.usage')}:[/yellow] {t('repl.usage.log')}"
        )
        return

    ref = args[0]
    rest = args[1:]

    try:
        tail_str = _get_flag(rest, "--tail")
        offset_str = _get_flag(rest, "--offset")
        tail = int(tail_str) if tail_str else 200
        offset = int(offset_str) if offset_str else 0
    except ValueError:
        console.print(f"[red]{t('repl.label.err')}:[/red] {t('repl.err.log_int_required')}")
        return

    show_all = "--all" in rest
    errors_only = "--errors-only" in rest

    result = await svc.cmd_log(
        ref, tail=tail, offset=offset, all_lines=show_all, errors_only=errors_only
    )
    if result["total"] == 0:
        console.print(f"[dim]{t('repl.log.not_found').format(log_path=result['log_path'])}[/dim]")
        return

    console.print(
        f"[dim]{t('repl.log.header').format(log_path=result['log_path'], total=result['total'], start=result['start'] + 1, end=result['end'])}[/dim]"
    )
    for line_dict in result["lines"]:
        _render_log_line(line_dict["raw"])


def _render_log_line(line: str) -> None:
    """按日志级别着色输出单行日志。"""
    m = _LOG_LEVEL_RE.match(line)
    level = m.group(1).upper() if m else ""
    if level == "ERROR" or level == "CRITICAL":
        console.print(line, style="bold red")
    elif level == "WARNING" or level == "WARN":
        console.print(line, style="yellow")
    else:
        console.print(line, style="dim")


def _print_pipelines(rm: RunManager) -> None:
    """以表格格式输出已注册的 pipeline 列表（pipeline_id / type / name）。"""
    pipelines = rm.list_pipelines()
    if not pipelines:
        console.print(f"[dim]{t('repl.info.empty_pipelines')}[/dim]")
        return
    table = Table(box=box.SIMPLE, title=t("repl.table.loaded_pipelines"))
    table.add_column(t("repl.col.pipeline_id"), style="bold")
    table.add_column(t("repl.col.type"))
    table.add_column(t("repl.col.name"))
    for p in pipelines:
        table.add_row(p["pipeline_id"], p.get("type", ""), p["name"])
    console.print(table)


async def _print_instances(rm: RunManager) -> None:
    """以表格格式输出运行实例列表（pipeline_id / instance_id / status）。"""
    instances = await rm.list_instances()
    if not instances:
        console.print(f"[dim]{t('repl.info.empty_instances')}[/dim]")
        return
    table = Table(box=box.SIMPLE, title=t("repl.table.instances"))
    table.add_column(t("repl.col.pipeline_id"))
    table.add_column(t("repl.col.instance_id"), style="bold")
    table.add_column(t("repl.col.status"), justify="center")
    for inst in instances:
        table.add_row(inst["pipeline_id"], inst["instance_id"], _colorize(Status(inst["status"])))
    console.print(table)


def _print_runs(rm: RunManager) -> None:
    """以表格格式输出所有已知 run 的列表。"""
    runs = rm.list_runs()
    if not runs:
        console.print(f"[dim]{t('repl.info.empty_runs')}[/dim]")
        return
    table = Table(box=box.SIMPLE, title=t("repl.table.runs"))
    table.add_column(t("repl.col.run_id"), style="bold")
    table.add_column(t("repl.col.pipeline_id"))
    table.add_column(t("repl.col.active"), justify="center")
    for r in runs:
        table.add_row(
            r["run_id"],
            r["pipeline_id"],
            t("repl.label.active_yes") if r["active"] else t("repl.label.active_no"),
        )
    console.print(table)


# ─── flag 解析工具 ───────────────────────────────────────────────────────────

def _get_flag(args: list[str], flag: str) -> str | None:
    """返回 --flag 后面的值，若不存在或值以 '--' 开头则返回 None。

    拒绝将相邻的另一个 --xxx 参数当作值，避免解析歧义。
    例如：``--step --task foo`` 中，--step 的值不应被解析为 ``--task``。
    """
    try:
        idx = args.index(flag)
        value = args[idx + 1]
        if value.startswith("--"):
            # 下一个 token 是另一个 flag，不是值
            console.print(
                f"[yellow]{t('repl.label.warn')}:[/yellow] "
                + t("repl.warn.flag_missing_value").format(flag=flag, value=value)
            )
            return None
        return value
    except (ValueError, IndexError):
        return None
