"""Interactive REPL using prompt_toolkit + asyncio."""
from __future__ import annotations

import asyncio
import json
import shlex
import traceback
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich import box

from pipeline_engine.core.errors import PipelineError
from pipeline_engine.core.run_manager import RunManager
from pipeline_engine.models.runtime_state import PipelineRunState, Status

console = Console()

_HELP = """\
[bold cyan]Available commands:[/bold cyan]
  load <path> [<path>...]           Load pipeline YAML(s)
  list [--runs]                     List pipelines (--runs: list active runs)
  run <pipeline_id> [--step S] [--task T]   Start a run (non-blocking)
  stop <ref> [--step S --task T]    Abort a run (or single task)
  resume <ref> [--include-paused]   Resume failed run
  status <ref> [--watch]            Show run status (--watch: live refresh)
  status --all                      Show all active runs
  inspect <ref> --step S --task T   Show task detail (input/output/log)
  fix <ref> --task T --output PATH  Supply recovered output.json
  fix <ref> --task T --input PATH   Supply replacement input.json
  help                              This message
  exit / quit                       Exit REPL
"""


# ─── public entry point ───────────────────────────────────────────────────────

async def run_repl(workspace: Path) -> None:
    try:
        from prompt_toolkit import PromptSession
        from prompt_toolkit.history import InMemoryHistory
        from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
    except ImportError:
        console.print("[red]prompt_toolkit not installed — falling back to basic input.[/red]")
        await _run_repl_basic(workspace)
        return

    rm = RunManager(workspace)
    session: PromptSession = PromptSession(
        history=InMemoryHistory(),
        auto_suggest=AutoSuggestFromHistory(),
    )

    console.print("[bold green]Pipeline REPL[/bold green]  (type [cyan]help[/cyan] for commands)")
    console.print(f"Workspace: [dim]{workspace}[/dim]\n")

    while True:
        try:
            raw = await session.prompt_async("pipeline> ")
        except (EOFError, KeyboardInterrupt):
            console.print("\n[yellow]Use 'exit' to quit.[/yellow]")
            continue

        raw = raw.strip()
        if not raw:
            continue

        try:
            await _dispatch(rm, raw)
        except SystemExit:
            break
        except PipelineError as e:
            console.print(f"[red]Error:[/red] {e}")
        except Exception as e:
            console.print(f"[red]Unexpected error:[/red] {e}")
            console.print_exception(max_frames=5)


async def _run_repl_basic(workspace: Path) -> None:
    """Fallback REPL for environments without prompt_toolkit."""
    rm = RunManager(workspace)
    console.print("[bold green]Pipeline REPL (basic mode)[/bold green]")
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
            console.print(f"[red]Error:[/red] {e}")


# ─── command dispatcher ───────────────────────────────────────────────────────

async def _dispatch(rm: RunManager, raw: str) -> None:
    try:
        argv = shlex.split(raw)
    except ValueError as e:
        console.print(f"[red]Parse error:[/red] {e}")
        return

    if not argv:
        return

    cmd, *args = argv

    match cmd:
        case "help":
            console.print(_HELP)

        case "exit" | "quit":
            active = [ctx for ctx in rm._runs.values() if ctx.is_active()]
            if active:
                console.print(
                    f"[yellow]Warning:[/yellow] {len(active)} run(s) still active. "
                    "Stop them first or they will be abandoned."
                )
            raise SystemExit

        case "load":
            if not args:
                console.print("[yellow]Usage:[/yellow] load <path> [<path>...]")
                return
            for p in args:
                pid = await rm.load(p)
                console.print(f"[green]Loaded:[/green] {pid}")

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
            console.print(f"[red]Unknown command:[/red] {cmd!r}  (type [cyan]help[/cyan])")


# ─── individual command handlers ──────────────────────────────────────────────

async def _cmd_run(rm: RunManager, args: list[str]) -> None:
    if not args:
        console.print("[yellow]Usage:[/yellow] run <pipeline_id> [--step S] [--task T]")
        return
    pipeline_id = args[0]
    rest = args[1:]
    step_id = _get_flag(rest, "--step")
    task_id = _get_flag(rest, "--task")

    run_id = await rm.start_run(pipeline_id, step_id=step_id, task_id=task_id)
    console.print(f"[green]Started:[/green] {run_id}  (pipeline: {pipeline_id})")


async def _cmd_stop(rm: RunManager, args: list[str]) -> None:
    if not args:
        console.print("[yellow]Usage:[/yellow] stop <ref> [--step S --task T]")
        return
    ref = args[0]
    rest = args[1:]
    step_id = _get_flag(rest, "--step")
    task_id = _get_flag(rest, "--task")
    await rm.stop(ref, step_id=step_id, task_id=task_id)
    console.print(f"[yellow]Stopped:[/yellow] {ref}")


async def _cmd_resume(rm: RunManager, args: list[str]) -> None:
    if not args:
        console.print("[yellow]Usage:[/yellow] resume <ref> [--include-paused]")
        return
    ref = args[0]
    include_paused = "--include-paused" in args
    run_id = await rm.resume(ref, include_paused=include_paused)
    console.print(f"[green]Resumed:[/green] {run_id}")


async def _cmd_status(rm: RunManager, args: list[str]) -> None:
    if "--all" in args:
        _print_runs(rm)
        return

    if not args:
        console.print("[yellow]Usage:[/yellow] status <ref> [--watch]")
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
        console.print("[yellow]Usage:[/yellow] inspect <ref> [--step S] [--task T]")
        return
    ref = args[0]
    rest = args[1:]
    step_id = _get_flag(rest, "--step")
    task_id = _get_flag(rest, "--task")
    state = await rm.get_run_state(ref)
    _render_inspect(state, step_id, task_id)


async def _cmd_fix(rm: RunManager, args: list[str]) -> None:
    if len(args) < 1:
        console.print("[yellow]Usage:[/yellow] fix <ref> --task T --output PATH  |  --input PATH")
        return
    ref = args[0]
    rest = args[1:]
    task_locator = _get_flag(rest, "--task")
    output_path = _get_flag(rest, "--output")
    input_path = _get_flag(rest, "--input")

    if not task_locator:
        console.print("[red]Error:[/red] --task is required for fix")
        return
    if not output_path and not input_path:
        console.print("[red]Error:[/red] fix requires --output or --input")
        return

    await rm.fix(ref, task_locator, input_path=input_path, output_path=output_path)
    if output_path:
        console.print(f"[green]Fixed (output):[/green] task '{task_locator}' → RECOVERED")
    else:
        console.print(f"[green]Fixed (input):[/green] task '{task_locator}' input updated → PENDING")


# ─── rendering helpers ────────────────────────────────────────────────────────

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
    color = _STATUS_COLOR.get(status, "white")
    return f"[{color}]{status.value}[/{color}]"


def _render_status(state: PipelineRunState) -> None:
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

    pipeline_status = _colorize(state.status)
    console.print(table)
    console.print(f"Pipeline status: {pipeline_status}")


async def _watch_status(rm: RunManager, ref: str, refresh: float = 0.5) -> None:
    """Live-refresh the status table until the run finishes."""
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
        # Final update
        try:
            state = await rm.get_run_state(ref)
            live.update(_build_status_renderable(state))
        except PipelineError:
            pass


def _build_status_renderable(state: PipelineRunState) -> Table:
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
    if step_id is None:
        # Show all steps/tasks summary
        _render_status(state)
        return

    step_state = state.steps.get(step_id)
    if step_state is None:
        console.print(f"[red]Step '{step_id}' not found in run.[/red]")
        return

    if task_id is None:
        # Show all tasks in this step
        for tid, ts in step_state.tasks.items():
            _render_task_detail(tid, ts)
        return

    ts = step_state.tasks.get(task_id)
    if ts is None:
        console.print(f"[red]Task '{task_id}' not found in step '{step_id}'.[/red]")
        return
    _render_task_detail(task_id, ts)


def _render_task_detail(task_id: str, ts) -> None:
    console.rule(f"[bold]{task_id}[/bold]")
    console.print(f"Status   : {_colorize(ts.status)}")
    console.print(f"Progress : {ts.progress}%")
    if ts.error:
        console.print(f"[red]Error    : {ts.error}[/red]")
    if ts.stack_trace:
        console.print(f"[dim]{ts.stack_trace}[/dim]")
    if ts.recovered_by:
        console.print(f"[magenta]Recovered: {ts.recovered_by}[/magenta]")

    for label, path_attr in (("Input", ts.input_path), ("Output", ts.output_path)):
        if path_attr:
            p = Path(path_attr)
            console.print(f"\n[bold]{label}[/bold] ({p}):")
            if p.exists():
                try:
                    data = json.loads(p.read_text())
                    console.print_json(json.dumps(data))
                except Exception:
                    console.print(p.read_text()[:2000])
            else:
                console.print("[dim](file not found)[/dim]")

    if ts.log_path:
        log = Path(ts.log_path)
        if log.exists():
            console.print(f"\n[bold]Log[/bold] ({log}):")
            console.print(log.read_text()[-4000:])


def _print_pipelines(rm: RunManager) -> None:
    pipelines = rm.list_pipelines()
    if not pipelines:
        console.print("[dim]No pipelines loaded.[/dim]")
        return
    table = Table(box=box.SIMPLE, title="Loaded Pipelines")
    table.add_column("pipeline_id", style="bold")
    table.add_column("name")
    for p in pipelines:
        table.add_row(p["pipeline_id"], p["name"])
    console.print(table)


def _print_runs(rm: RunManager) -> None:
    runs = rm.list_runs()
    if not runs:
        console.print("[dim]No runs.[/dim]")
        return
    table = Table(box=box.SIMPLE, title="Runs")
    table.add_column("run_id", style="bold")
    table.add_column("pipeline_id")
    table.add_column("active", justify="center")
    for r in runs:
        table.add_row(
            r["run_id"],
            r["pipeline_id"],
            "[green]yes[/green]" if r["active"] else "[dim]no[/dim]",
        )
    console.print(table)


# ─── flag parser ──────────────────────────────────────────────────────────────

def _get_flag(args: list[str], flag: str) -> str | None:
    """Return the value after --flag, or None if absent."""
    try:
        idx = args.index(flag)
        return args[idx + 1]
    except (ValueError, IndexError):
        return None
