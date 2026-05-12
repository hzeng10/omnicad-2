"""交互式 REPL：基于 prompt_toolkit + asyncio 的非阻塞命令行界面。

架构说明
--------
- REPL 作为 asyncio 协程运行，与后台调度器共享同一事件循环。
- 用户输入不会阻塞任务执行；任务状态更新不会干扰命令输入。
- 命令解析使用 shlex.split，支持带引号的路径参数。
- 终端渲染使用 Rich：进度表格、彩色状态、JSON 格式化。

C9 修复：``_get_flag`` 拒绝将相邻 ``--xxx`` 参数当作值，返回 None 并提示用法。
C10 修复：``run`` 命令支持多个 pipeline_id，行为与 CLI 一致。
"""
from __future__ import annotations

import asyncio
import json
import shlex
from pathlib import Path
from typing import Any

from rich import box
from rich.console import Console
from rich.live import Live
from rich.table import Table

from pipeline_engine.core.errors import PipelineError
from pipeline_engine.core.run_manager import RunManager
from pipeline_engine.models.runtime_state import PipelineRunState, Status

console = Console()

_HELP = """\
[bold cyan]可用命令：[/bold cyan]
  load <path> [<path>...]                       加载 pipeline YAML 文件
  list [--runs]                                  列出 pipeline（--runs：显示 run 列表）
  run <id> [<id>...] [--step S] [--task T]       启动一个或多个 run（非阻塞）
  stop <ref> [--step S --task T]                 中止 run（或单个 task）
  resume <ref> [--include-paused]                恢复失败的 run
  status <ref> [--watch]                         查看状态（--watch：持续刷新）
  status --all                                   查看所有活跃 run
  inspect <ref> [--step S] [--task T]            查看 task 详情（输入/输出/日志）
  fix <ref> --task T --output PATH               注入恢复的 output.json
  fix <ref> --task T --input PATH                注入替换的 input.json
  help                                           显示此帮助
  exit / quit                                    退出 REPL
"""


# ─── 公共入口 ─────────────────────────────────────────────────────────────────

async def run_repl(workspace: Path) -> None:
    """启动交互式 REPL（优先使用 prompt_toolkit，不可用时退化为基础模式）。"""
    try:
        from prompt_toolkit import PromptSession
        from prompt_toolkit.history import InMemoryHistory
        from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
    except ImportError:
        console.print("[red]prompt_toolkit 未安装 — 退回基础输入模式。[/red]")
        await _run_repl_basic(workspace)
        return

    rm = RunManager(workspace)
    session: PromptSession = PromptSession(
        history=InMemoryHistory(),
        auto_suggest=AutoSuggestFromHistory(),
    )

    console.print("[bold green]Pipeline REPL[/bold green]  (输入 [cyan]help[/cyan] 查看命令)")
    console.print(f"工作目录: [dim]{workspace}[/dim]\n")

    while True:
        try:
            raw = await session.prompt_async("pipeline> ")
        except (EOFError, KeyboardInterrupt):
            console.print("\n[yellow]请输入 'exit' 退出。[/yellow]")
            continue

        raw = raw.strip()
        if not raw:
            continue

        try:
            await _dispatch(rm, raw)
        except SystemExit:
            break
        except PipelineError as e:
            console.print(f"[red]错误:[/red] {e}")
        except Exception as e:
            console.print(f"[red]意外错误:[/red] {e}")
            console.print_exception(max_frames=5)


async def _run_repl_basic(workspace: Path) -> None:
    """无 prompt_toolkit 时的简化 REPL（无历史/补全）。"""
    rm = RunManager(workspace)
    console.print("[bold green]Pipeline REPL（基础模式）[/bold green]")
    while True:
        try:
            raw = input("pipeline> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not raw:
            continue
        try:
            await _dispatch(rm, raw)
        except SystemExit:
            break
        except PipelineError as e:
            console.print(f"[red]错误:[/red] {e}")


# ─── 命令分发 ─────────────────────────────────────────────────────────────────

async def _dispatch(rm: RunManager, raw: str) -> None:
    """解析并分发单条命令。"""
    try:
        argv = shlex.split(raw)
    except ValueError as e:
        console.print(f"[red]解析错误:[/red] {e}")
        return

    if not argv:
        return

    cmd, *args = argv

    match cmd:
        case "help":
            console.print(_HELP)

        case "exit" | "quit":
            # C11：退出前提示仍有活跃 run
            active = [ctx for ctx in rm._runs.values() if ctx.is_active()]
            if active:
                console.print(
                    f"[yellow]警告:[/yellow] {len(active)} 个 run 仍在运行，"
                    "退出后将被放弃。"
                )
            raise SystemExit

        case "load":
            if not args:
                console.print("[yellow]用法:[/yellow] load <path> [<path>...]")
                return
            for p in args:
                pid = await rm.load(p)
                console.print(f"[green]已加载:[/green] {pid}")

        case "list":
            flags = set(args)
            if "--runs" in flags:
                _print_runs(rm)
            else:
                _print_pipelines(rm)

        case "run":
            await _cmd_run(rm, args)

        case "stop":
            await _cmd_stop(rm, args)

        case "resume":
            await _cmd_resume(rm, args)

        case "status":
            await _cmd_status(rm, args)

        case "inspect":
            await _cmd_inspect(rm, args)

        case "fix":
            await _cmd_fix(rm, args)

        case _:
            console.print(f"[red]未知命令:[/red] {cmd!r}  (输入 [cyan]help[/cyan] 查看命令列表)")


# ─── 各命令处理函数 ───────────────────────────────────────────────────────────

async def _cmd_run(rm: RunManager, args: list[str]) -> None:
    """run 命令处理器。

    C10 修复：支持多个 pipeline_id，行为与 CLI run 子命令一致。
    """
    if not args:
        console.print("[yellow]用法:[/yellow] run <pipeline_id> [<pipeline_id>...] [--step S] [--task T]")
        return

    step_id = _get_flag(args, "--step")
    task_id = _get_flag(args, "--task")

    # 提取非 flag 参数作为 pipeline_id 列表
    pipeline_ids = [a for a in args if not a.startswith("--") and a != step_id and a != task_id]

    if not pipeline_ids:
        console.print("[yellow]用法:[/yellow] run <pipeline_id> [<pipeline_id>...] [--step S] [--task T]")
        return

    for pipeline_id in pipeline_ids:
        try:
            run_id = await rm.start_run(pipeline_id, step_id=step_id, task_id=task_id)
            console.print(f"[green]已启动:[/green] {run_id}  (pipeline: {pipeline_id})")
        except PipelineError as e:
            console.print(f"[red]错误:[/red] pipeline '{pipeline_id}' 启动失败: {e}")


async def _cmd_stop(rm: RunManager, args: list[str]) -> None:
    if not args:
        console.print("[yellow]用法:[/yellow] stop <ref> [--step S --task T]")
        return
    ref = args[0]
    rest = args[1:]
    step_id = _get_flag(rest, "--step")
    task_id = _get_flag(rest, "--task")
    await rm.stop(ref, step_id=step_id, task_id=task_id)
    console.print(f"[yellow]已中止:[/yellow] {ref}")


async def _cmd_resume(rm: RunManager, args: list[str]) -> None:
    if not args:
        console.print("[yellow]用法:[/yellow] resume <ref> [--include-paused]")
        return
    ref = args[0]
    include_paused = "--include-paused" in args
    run_id = await rm.resume(ref, include_paused=include_paused)
    console.print(f"[green]已恢复:[/green] {run_id}")


async def _cmd_status(rm: RunManager, args: list[str]) -> None:
    if "--all" in args:
        _print_runs(rm)
        return

    if not args:
        console.print("[yellow]用法:[/yellow] status <ref> [--watch]")
        return

    ref = args[0]
    watch = "--watch" in args

    if watch:
        await _watch_status(rm, ref)
    else:
        state = await rm.get_run_state(ref)
        _render_status(state)


async def _cmd_inspect(rm: RunManager, args: list[str]) -> None:
    if not args:
        console.print("[yellow]用法:[/yellow] inspect <ref> [--step S] [--task T]")
        return
    ref = args[0]
    rest = args[1:]
    step_id = _get_flag(rest, "--step")
    task_id = _get_flag(rest, "--task")
    state = await rm.get_run_state(ref)
    _render_inspect(state, step_id, task_id)


async def _cmd_fix(rm: RunManager, args: list[str]) -> None:
    if len(args) < 1:
        console.print("[yellow]用法:[/yellow] fix <ref> --task T --output PATH  |  --input PATH")
        return
    ref = args[0]
    rest = args[1:]
    task_locator = _get_flag(rest, "--task")
    output_path = _get_flag(rest, "--output")
    input_path = _get_flag(rest, "--input")

    if not task_locator:
        console.print("[red]错误:[/red] fix 需要 --task 参数")
        return
    if not output_path and not input_path:
        console.print("[red]错误:[/red] fix 需要 --output 或 --input 参数")
        return

    await rm.fix(ref, task_locator, input_path=input_path, output_path=output_path)
    if output_path:
        console.print(f"[green]修复成功 (output):[/green] task '{task_locator}' → RECOVERED")
    else:
        console.print(f"[green]修复成功 (input):[/green] task '{task_locator}' 输入已更新 → PENDING")


# ─── 渲染工具 ────────────────────────────────────────────────────────────────

_STATUS_COLOR = {
    Status.PENDING:   "dim",
    Status.RUNNING:   "bold cyan",
    Status.PAUSED:    "yellow",
    Status.SUCCESS:   "green",
    Status.FAILED:    "bold red",
    Status.SKIPPED:   "blue",
    Status.RECOVERED: "magenta",
}


def _colorize(status: Status) -> str:
    """为状态值添加 Rich 颜色标签。"""
    color = _STATUS_COLOR.get(status, "white")
    return f"[{color}]{status.value}[/{color}]"


def _render_status(state: PipelineRunState) -> None:
    """渲染 run 整体状态表格。"""
    table = Table(
        title=f"Run: {state.run_id}  |  Pipeline: {state.pipeline_id}",
        box=box.ROUNDED,
        show_lines=True,
    )
    table.add_column("Step", style="bold")
    table.add_column("Task")
    table.add_column("Status", justify="center")
    table.add_column("Progress", justify="right")
    table.add_column("Error", style="dim red", no_wrap=False, max_width=50)

    for step_id, step_state in state.steps.items():
        first = True
        for task_id, ts in step_state.tasks.items():
            step_label = step_id if first else ""
            first = False
            table.add_row(
                step_label,
                task_id,
                _colorize(ts.status),
                f"{ts.progress}%",
                ts.error or "",
            )
        if not step_state.tasks:
            table.add_row(step_id, "—", _colorize(step_state.status), "", "")

    console.print(table)
    console.print(f"Pipeline 状态: {_colorize(state.status)}")


async def _watch_status(rm: RunManager, ref: str, refresh: float = 0.5) -> None:
    """持续刷新状态表格直到 run 结束（Live 模式）。"""
    ctx = rm._resolve_run(ref)

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
    table.add_column("Step")
    table.add_column("Task")
    table.add_column("Status", justify="center")
    table.add_column("Progress", justify="right")

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
        console.print(f"[red]Step '{step_id}' 在本 run 中不存在。[/red]")
        return

    if task_id is None:
        for tid, ts in step_state.tasks.items():
            _render_task_detail(tid, ts)
        return

    ts = step_state.tasks.get(task_id)
    if ts is None:
        console.print(f"[red]Task '{task_id}' 在 step '{step_id}' 中不存在。[/red]")
        return
    _render_task_detail(task_id, ts)


def _render_task_detail(task_id: str, ts) -> None:
    """渲染单个 task 的详细信息（状态/进度/错误/输入输出/日志）。"""
    console.rule(f"[bold]{task_id}[/bold]")
    console.print(f"状态    : {_colorize(ts.status)}")
    console.print(f"进度    : {ts.progress}%")
    if ts.error:
        console.print(f"[red]错误    : {ts.error}[/red]")
    if ts.stack_trace:
        console.print(f"[dim]{ts.stack_trace}[/dim]")
    if ts.recovered_by:
        console.print(f"[magenta]恢复方式: {ts.recovered_by}[/magenta]")

    for label, path_attr in (("输入", ts.input_path), ("输出", ts.output_path)):
        if path_attr:
            p = Path(path_attr)
            console.print(f"\n[bold]{label}[/bold] ({p}):")
            if p.exists():
                try:
                    data = json.loads(p.read_text(encoding="utf-8", errors="replace"))
                    console.print_json(json.dumps(data))
                except Exception:
                    # C12：按行截尾，避免截断 UTF-8 多字节字符
                    lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
                    console.print("\n".join(lines[:100]))
            else:
                console.print("[dim](文件不存在)[/dim]")

    if ts.log_path:
        log = Path(ts.log_path)
        if log.exists():
            console.print(f"\n[bold]日志[/bold] ({log}):")
            # C12：按行截尾（最后 200 行）
            lines = log.read_text(encoding="utf-8", errors="replace").splitlines()
            console.print("\n".join(lines[-200:]))


def _print_pipelines(rm: RunManager) -> None:
    """以表格格式输出已注册的 pipeline 列表。"""
    pipelines = rm.list_pipelines()
    if not pipelines:
        console.print("[dim]暂无已加载的 pipeline。[/dim]")
        return
    table = Table(box=box.SIMPLE, title="已加载的 Pipeline")
    table.add_column("pipeline_id", style="bold")
    table.add_column("name")
    for p in pipelines:
        table.add_row(p["pipeline_id"], p["name"])
    console.print(table)


def _print_runs(rm: RunManager) -> None:
    """以表格格式输出所有已知 run 的列表。"""
    runs = rm.list_runs()
    if not runs:
        console.print("[dim]暂无 run 记录。[/dim]")
        return
    table = Table(box=box.SIMPLE, title="Run 列表")
    table.add_column("run_id", style="bold")
    table.add_column("pipeline_id")
    table.add_column("活跃", justify="center")
    for r in runs:
        table.add_row(
            r["run_id"],
            r["pipeline_id"],
            "[green]是[/green]" if r["active"] else "[dim]否[/dim]",
        )
    console.print(table)


# ─── flag 解析工具 ───────────────────────────────────────────────────────────

def _get_flag(args: list[str], flag: str) -> str | None:
    """返回 --flag 后面的值，若不存在或值以 '--' 开头则返回 None。

    C9 修复：拒绝将相邻的另一个 --xxx 参数当作值，避免解析歧义。
    例如：``--step --task foo`` 中，--step 的值不应被解析为 ``--task``。
    """
    try:
        idx = args.index(flag)
        value = args[idx + 1]
        if value.startswith("--"):
            # 下一个 token 是另一个 flag，不是值
            console.print(
                f"[yellow]警告:[/yellow] {flag} 缺少值（'{value}' 看起来是另一个选项）"
            )
            return None
        return value
    except (ValueError, IndexError):
        return None
