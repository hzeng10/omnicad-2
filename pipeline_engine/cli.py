"""CLI entry point — both one-shot subcommands and REPL launcher."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Optional

import typer

app = typer.Typer(
    name="pipeline_cli",
    help="Pipeline DAG Engine — run, monitor and fix YAML-defined workflows.",
    add_completion=False,
    no_args_is_help=False,
)

_workspace_option = typer.Option(
    None,
    "--workspace",
    "-w",
    help="Root workspace directory (default: current directory).",
    show_default=False,
)


def _get_workspace(workspace: Optional[Path]) -> Path:
    return Path(workspace) if workspace else Path.cwd()


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    workspace: Optional[Path] = _workspace_option,
) -> None:
    """Enter interactive REPL when called without a subcommand."""
    if ctx.invoked_subcommand is None:
        from pipeline_engine.repl import run_repl
        asyncio.run(run_repl(_get_workspace(workspace)))


@app.command()
def load(
    paths: list[Path] = typer.Argument(..., help="YAML pipeline file(s) to load."),
    workspace: Optional[Path] = _workspace_option,
) -> None:
    """Parse, validate and register one or more pipeline YAML files."""
    from pipeline_engine.core.run_manager import RunManager

    async def _load() -> None:
        rm = RunManager(_get_workspace(workspace))
        for p in paths:
            pid = await rm.load(p)
            typer.echo(f"Loaded: {pid}")

    asyncio.run(_load())


@app.command()
def lint(
    path: Path = typer.Argument(..., help="Pipeline YAML file to validate."),
) -> None:
    """Validate a pipeline YAML without running it."""
    from pipeline_engine.core.yaml_parser import load_pipeline_spec
    from pipeline_engine.core.dag_validator import validate_pipeline
    from pipeline_engine.core.errors import PipelineError

    try:
        spec = load_pipeline_spec(path)
        validate_pipeline(spec)
        typer.echo(f"OK — pipeline '{spec.pipeline.id}' is valid.")
    except PipelineError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command("run")
def run_cmd(
    pipeline_ids: list[str] = typer.Argument(..., help="Pipeline ID(s) to run."),
    workspace: Optional[Path] = _workspace_option,
    step: Optional[str] = typer.Option(None, "--step", "-s", help="Run a single step."),
    task: Optional[str] = typer.Option(None, "--task", "-t", help="Run a single task within --step."),
    wait: bool = typer.Option(False, "--wait", help="Block until all runs complete."),
) -> None:
    """Start one or more pipeline runs (concurrent). Prints run_id(s)."""
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError

    async def _run() -> None:
        rm = RunManager(_get_workspace(workspace))
        # Load all registered pipelines from registry on disk
        from pipeline_engine.core import storage
        reg = storage.load_registry(rm.workspace)
        for pid, meta in reg.items():
            spec_path = meta.get("yaml_path")
            if spec_path:
                try:
                    await rm.load(spec_path)
                except Exception:
                    pass

        run_ids = []
        for pid in pipeline_ids:
            try:
                run_id = await rm.start_run(pid, step_id=step, task_id=task)
                typer.echo(f"Started: {run_id}  (pipeline: {pid})")
                run_ids.append(run_id)
            except PipelineError as e:
                typer.echo(f"Error: {e}", err=True)
                raise typer.Exit(1)

        if wait:
            import asyncio as _asyncio
            tasks = [rm._runs[rid].main_task for rid in run_ids if rid in rm._runs]
            if tasks:
                await _asyncio.gather(*tasks, return_exceptions=True)
            for rid in run_ids:
                state = await rm.get_run_state(rid)
                typer.echo(f"  {rid}: {state.status.value}")

    asyncio.run(_run())


@app.command()
def status(
    ref: str = typer.Argument(..., help="run_id or pipeline_id."),
    workspace: Optional[Path] = _workspace_option,
) -> None:
    """Show the status of a run."""
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError
    from pipeline_engine.repl import _render_status

    async def _status() -> None:
        rm = RunManager(_get_workspace(workspace))
        _reload_registry(rm)
        try:
            state = await rm.get_run_state(ref)
            _render_status(state)
        except PipelineError as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)

    asyncio.run(_status())


@app.command()
def inspect(
    ref: str = typer.Argument(..., help="run_id or pipeline_id."),
    workspace: Optional[Path] = _workspace_option,
    step: Optional[str] = typer.Option(None, "--step", "-s"),
    task: Optional[str] = typer.Option(None, "--task", "-t"),
) -> None:
    """Show input/output/log detail for a task."""
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError
    from pipeline_engine.repl import _render_inspect

    async def _inspect() -> None:
        rm = RunManager(_get_workspace(workspace))
        _reload_registry(rm)
        try:
            state = await rm.get_run_state(ref)
            _render_inspect(state, step, task)
        except PipelineError as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)

    asyncio.run(_inspect())


@app.command("list")
def list_cmd(
    workspace: Optional[Path] = _workspace_option,
) -> None:
    """List all registered pipelines."""
    from pipeline_engine.core.run_manager import RunManager

    async def _list() -> None:
        rm = RunManager(_get_workspace(workspace))
        _reload_registry(rm)
        pipelines = rm.list_pipelines()
        if not pipelines:
            typer.echo("No pipelines loaded.")
            return
        for p in pipelines:
            typer.echo(f"  {p['pipeline_id']:30s}  {p['name']}")

    asyncio.run(_list())


def _reload_registry(rm) -> None:
    """Reload pipelines from on-disk registry into rm._registry (sync helper)."""
    from pipeline_engine.core import storage
    from pipeline_engine.core.yaml_parser import load_pipeline_spec
    from pipeline_engine.core.dag_validator import validate_pipeline

    reg = storage.load_registry(rm.workspace)
    for pid, meta in reg.items():
        spec_path = meta.get("yaml_path")
        if spec_path and pid not in rm._registry:
            try:
                spec = load_pipeline_spec(spec_path)
                validate_pipeline(spec)
                rm._registry[pid] = spec
            except Exception:
                pass
