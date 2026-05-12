"""命令行入口：提供一次性子命令和交互式 REPL 两种使用模式。

子命令列表
----------
- ``load``：解析并注册 pipeline YAML 文件。
- ``lint``：校验 YAML 语法与 DAG 合法性，不执行。
- ``run``：启动一个或多个 pipeline run（支持 --step / --task 细粒度启动）。
- ``stop``：中止指定 run（或其中某个 task）。
- ``resume``：恢复 FAILED/PAUSED 的 run。
- ``fix``：向失败任务注入 input 或 output 数据。
- ``status``：查看 run 的整体进度。
- ``inspect``：查看 task 的详细信息（输入/输出/日志）。
- ``list``：列出所有已注册的 pipeline。

修复说明
--------
- **A4**：`resume` 子命令改用 ``ctx.await_main()`` 等待 main_task，
  避免 main_task 为 None 时裸 ``await`` 抛 TypeError。
- **B8**：``run`` 子命令在多 pipeline 场景下容错：单个 pipeline 启动失败不
  立即退出，而是继续尝试其余 pipeline，最后汇总报告并以非零码退出。
- **B10**：全局 ``--workspace`` 选项通过 ``ctx.obj`` 透传给所有子命令，
  使得 ``pipeline_cli --workspace /x run p1`` 可以正确生效。
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Optional

import typer

app = typer.Typer(
    name="pipeline_cli",
    help="Pipeline DAG Engine — 通过 YAML 定义、运行和监控 DAG 工作流。",
    add_completion=False,
    no_args_is_help=False,
)

# 子命令本地 --workspace 选项（None 表示回退到全局配置或 cwd）
_workspace_option = typer.Option(
    None,
    "--workspace",
    "-w",
    help="工作目录（默认：当前目录）。",
    show_default=False,
)


def _get_workspace(local_workspace: Optional[Path], ctx: typer.Context) -> Path:
    """解析最终使用的 workspace 路径。

    优先级：子命令 --workspace > 全局 --workspace（通过 ctx.obj） > cwd。
    B10 修复：通过 ctx.obj 读取全局选项，而不是全局变量。
    """
    if local_workspace is not None:
        return Path(local_workspace)
    global_ws = (ctx.obj or {}).get("workspace")
    if global_ws is not None:
        return Path(global_ws)
    return Path.cwd()


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    workspace: Optional[Path] = _workspace_option,
) -> None:
    """无子命令时进入交互式 REPL；有子命令时透传全局 --workspace。

    B10 修复：将全局 workspace 存入 ctx.obj，供所有子命令通过 _get_workspace 读取。
    """
    # 初始化 ctx.obj 并存入全局 workspace
    if ctx.obj is None:
        ctx.obj = {}
    if workspace is not None:
        ctx.obj["workspace"] = workspace

    if ctx.invoked_subcommand is None:
        # 无子命令 → 进入 REPL
        from pipeline_engine.repl import run_repl
        ws = workspace if workspace is not None else Path.cwd()
        asyncio.run(run_repl(ws))


@app.command()
def load(
    ctx: typer.Context,
    paths: list[Path] = typer.Argument(..., help="要加载的 YAML pipeline 文件路径。"),
    workspace: Optional[Path] = _workspace_option,
) -> None:
    """解析、校验并注册一个或多个 pipeline YAML 文件。"""
    from pipeline_engine.core.run_manager import RunManager

    async def _load() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
        for p in paths:
            pid = await rm.load(p)
            typer.echo(f"Loaded: {pid}")

    asyncio.run(_load())


@app.command()
def lint(
    path: Path = typer.Argument(..., help="要校验的 pipeline YAML 文件路径。"),
) -> None:
    """校验 pipeline YAML 语法与 DAG 合法性（不执行）。"""
    from pipeline_engine.core.yaml_parser import load_pipeline_spec
    from pipeline_engine.core.dag_validator import validate_pipeline
    from pipeline_engine.core.errors import PipelineError

    try:
        spec = load_pipeline_spec(path)
        validate_pipeline(spec)
        typer.echo(f"OK — pipeline '{spec.pipeline.id}' 校验通过。")
    except PipelineError as e:
        typer.echo(f"错误: {e}", err=True)
        raise typer.Exit(1)


@app.command("run")
def run_cmd(
    ctx: typer.Context,
    pipeline_ids: list[str] = typer.Argument(..., help="要运行的 pipeline ID 列表。"),
    workspace: Optional[Path] = _workspace_option,
    step: Optional[str] = typer.Option(None, "--step", "-s", help="仅运行指定 step。"),
    task: Optional[str] = typer.Option(None, "--task", "-t", help="仅运行指定 task（需配合 --step）。"),
    wait: bool = typer.Option(False, "--wait", help="阻塞直到所有 run 完成。"),
) -> None:
    """启动一个或多个 pipeline run（后台并发执行）。

    B8 修复：多 pipeline 场景下，单个 pipeline 启动失败不立即退出，
    而是继续尝试其余 pipeline，最后统一汇报并以非零码退出。
    """
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError

    async def _run() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
        # 从 registry 加载已注册的 pipeline 配置
        from pipeline_engine.core import storage
        reg = storage.load_registry(rm.workspace)
        for pid, meta in reg.items():
            spec_path = meta.get("yaml_path")
            if spec_path:
                try:
                    await rm.load(spec_path)
                except Exception:
                    pass

        run_ids: list[str] = []
        errors: list[str] = []

        # B8：逐个启动，收集错误，不提前退出
        for pid in pipeline_ids:
            try:
                run_id = await rm.start_run(pid, step_id=step, task_id=task)
                typer.echo(f"Started: {run_id}  (pipeline: {pid})")
                run_ids.append(run_id)
            except PipelineError as e:
                msg = f"pipeline '{pid}' failed to start: {e}"
                typer.echo(f"Error: {msg}", err=True)
                errors.append(msg)

        if wait and run_ids:
            # 使用 await_main() 安全等待（A4 修复）
            tasks_to_wait = [
                rm._runs[rid].await_main()
                for rid in run_ids
                if rid in rm._runs
            ]
            if tasks_to_wait:
                await asyncio.gather(*tasks_to_wait, return_exceptions=True)
            for rid in run_ids:
                state = await rm.get_run_state(rid)
                typer.echo(f"  {rid}: {state.status.value}")

        # B8：所有 pipeline 尝试完后统一报告失败
        if errors:
            raise typer.Exit(1)

    asyncio.run(_run())


@app.command()
def status(
    ctx: typer.Context,
    ref: str = typer.Argument(..., help="run_id 或 pipeline_id。"),
    workspace: Optional[Path] = _workspace_option,
) -> None:
    """查看指定 run 的整体状态与进度。"""
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError
    from pipeline_engine.repl import _render_status

    async def _status() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
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
    ctx: typer.Context,
    ref: str = typer.Argument(..., help="run_id 或 pipeline_id。"),
    workspace: Optional[Path] = _workspace_option,
    step: Optional[str] = typer.Option(None, "--step", "-s"),
    task: Optional[str] = typer.Option(None, "--task", "-t"),
) -> None:
    """查看 task 的详细信息（输入/输出/日志/堆栈）。"""
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError
    from pipeline_engine.repl import _render_inspect

    async def _inspect() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
        _reload_registry(rm)
        try:
            state = await rm.get_run_state(ref)
            _render_inspect(state, step, task)
        except PipelineError as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)

    asyncio.run(_inspect())


@app.command()
def stop(
    ctx: typer.Context,
    ref: str = typer.Argument(..., help="run_id 或 pipeline_id。"),
    workspace: Optional[Path] = _workspace_option,
    step: Optional[str] = typer.Option(None, "--step", "-s"),
    task: Optional[str] = typer.Option(None, "--task", "-t"),
) -> None:
    """中止指定 run（或其中某个 task）。"""
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError

    async def _stop() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
        _reload_registry(rm, restore_runs=True)
        try:
            await rm.stop(ref, step_id=step, task_id=task)
            typer.echo(f"Stopped: {ref}")
        except PipelineError as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)

    asyncio.run(_stop())


@app.command()
def resume(
    ctx: typer.Context,
    ref: str = typer.Argument(..., help="run_id 或 pipeline_id。"),
    workspace: Optional[Path] = _workspace_option,
    include_paused: bool = typer.Option(False, "--include-paused", help="同时恢复 PAUSED 状态的 task。"),
) -> None:
    """恢复 FAILED（或 PAUSED）的 run，并等待其完成。

    A4 修复：使用 ``ctx.await_main()`` 安全等待，避免 main_task 为 None 时抛 TypeError。
    """
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError

    async def _resume() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
        _reload_registry(rm, restore_runs=True)
        try:
            run_id = await rm.resume(ref, include_paused=include_paused)
            typer.echo(f"Resumed: {run_id}")
            # A4 修复：通过 await_main() 安全等待，main_task 为 None 时直接返回
            run_ctx = rm._runs[run_id]
            await run_ctx.await_main()
            state = await rm.get_run_state(run_id)
            typer.echo(f"  {run_id}: {state.status.value}")
        except PipelineError as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)

    asyncio.run(_resume())


@app.command()
def fix(
    ctx: typer.Context,
    ref: str = typer.Argument(..., help="run_id 或 pipeline_id。"),
    task_locator: str = typer.Option(..., "--task", "-t", help="step_id/task_id 或 task_id。"),
    workspace: Optional[Path] = _workspace_option,
    output_path: Optional[Path] = typer.Option(None, "--output", help="提供的 output.json 文件路径。"),
    input_path: Optional[Path] = typer.Option(None, "--input", help="替换的 input.json 文件路径。"),
) -> None:
    """向失败任务注入 output（RECOVERED）或替换 input（PENDING）以驱动 resume。"""
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError

    async def _fix() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
        _reload_registry(rm, restore_runs=True)
        try:
            await rm.fix(
                ref, task_locator,
                output_path=str(output_path) if output_path else None,
                input_path=str(input_path) if input_path else None,
            )
            if output_path:
                typer.echo(f"Fixed (output): task '{task_locator}' → RECOVERED")
            else:
                typer.echo(f"Fixed (input): task '{task_locator}' input updated → PENDING")
        except PipelineError as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)

    asyncio.run(_fix())


@app.command("list")
def list_cmd(
    ctx: typer.Context,
    workspace: Optional[Path] = _workspace_option,
) -> None:
    """列出所有已注册的 pipeline。"""
    from pipeline_engine.core.run_manager import RunManager

    async def _list() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
        _reload_registry(rm)
        pipelines = rm.list_pipelines()
        if not pipelines:
            typer.echo("No pipelines loaded.")
            return
        for p in pipelines:
            typer.echo(f"  {p['pipeline_id']:30s}  {p['name']}")

    asyncio.run(_list())


# ─── 内部工具 ─────────────────────────────────────────────────────────────────

def _reload_registry(rm, restore_runs: bool = False) -> None:
    """从磁盘重新加载 pipeline 注册表（及可选的 run 状态）。

    供 CLI 一次性子命令在新进程中重建 RunManager 状态。
    """
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
    if restore_runs:
        rm.restore_runs_from_disk()
