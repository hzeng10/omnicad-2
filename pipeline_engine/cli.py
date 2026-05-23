"""命令行入口：提供一次性子命令（JSON 输出）和交互式 REPL 两种使用模式。

子命令列表
----------
- ``load``：解析并注册 pipeline YAML 文件。
- ``lint``：校验 YAML 语法与 DAG 合法性，不执行。
- ``list``：列出已注册 pipeline（--pipeline）或运行实例（--instance）。
- ``start``：启动一个或多个 pipeline run（支持 --step / --task 细粒度启动）。
- ``stop``：中止指定 pipeline 实例（instance_id）。
- ``resume``：恢复 FAILED/PAUSED 的 pipeline 实例。
- ``fix``：向失败任务注入 input 或 output 数据。
- ``status``：查看 pipeline 实例的整体进度。
- ``inspect``：查看 task 的详细信息（输入/输出/日志）。
- ``log``：分页查看 pipeline 实例的 run.log。

输出模式
--------
所有子命令默认输出单个 JSON 对象（扁平 + ok 字段信封）到 stdout，方便 AI Agent
直接 json.loads。REPL 交互模式保持 Rich 文本渲染，行为完全不变。

失败时：exit code = 1，JSON 的 ok=false 仍写到 stdout，保证 Agent 可以一次
json.loads(stdout) 拿到完整错误信息。

Autoload
--------
CLI 启动时（REPL 与子命令均适用）自动扫描 ``./pipelines/*/pipeline.yaml``，
等价于逐一调用 ``load`` 命令。可通过 ``--pipelines-dir`` 改变目录，
``--no-autoload`` 禁用。单个 YAML 解析失败时跳过，写 WARNING 到 stderr。

全局选项
--------
- ``--workspace / -w``：工作目录（默认 cwd）。
- ``--pipelines-dir``：autoload 发现目录（默认 ./pipelines，env: PIPELINE_AUTOLOAD_DIR）。
- ``--no-autoload``：禁用 autoload（env: PIPELINE_NO_AUTOLOAD）。
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Optional

import typer

app = typer.Typer(
    name="omnicad",
    help="OmniCAD — DAG-based CAD workflow orchestration engine.",
    add_completion=False,
    no_args_is_help=False,
)

# 子命令本地 --workspace 选项
_workspace_option = typer.Option(
    None,
    "--workspace",
    "-w",
    help="工作目录（默认：当前目录）。",
    show_default=False,
)


def _get_workspace(local_workspace: Optional[Path], ctx: typer.Context) -> Path:
    """解析最终使用的 workspace 路径。优先级：子命令 > 全局 > cwd。"""
    if local_workspace is not None:
        return Path(local_workspace)
    global_ws = (ctx.obj or {}).get("workspace")
    if global_ws is not None:
        return Path(global_ws)
    return Path.cwd()


def _get_pipelines_dir(ctx: typer.Context) -> Optional[Path]:
    """从 ctx.obj 读取 pipelines_dir（可为 None）。"""
    v = (ctx.obj or {}).get("pipelines_dir")
    return Path(v) if v else None


def _is_no_autoload(ctx: typer.Context) -> bool:
    """从 ctx.obj 读取 no_autoload 标志。"""
    return bool((ctx.obj or {}).get("no_autoload", False))


def _make_service(workspace: Path, ctx: typer.Context):
    """便捷工厂：创建与当前 CLI 上下文匹配的 PipelineService 实例。"""
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.service import PipelineService

    rm = RunManager(workspace)
    return PipelineService(
        rm,
        pipelines_dir=_get_pipelines_dir(ctx),
        no_autoload=_is_no_autoload(ctx),
    )


# ─── app.callback ─────────────────────────────────────────────────────────────

@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    workspace: Optional[Path] = _workspace_option,
    pipelines_dir: Optional[Path] = typer.Option(
        None,
        "--pipelines-dir",
        help="Pipeline YAML 发现目录（默认 ./pipelines）。",
        envvar="PIPELINE_AUTOLOAD_DIR",
        show_default=False,
    ),
    no_autoload: bool = typer.Option(
        False,
        "--no-autoload",
        help="禁用启动时自动加载 pipeline。",
        envvar="PIPELINE_NO_AUTOLOAD",
    ),
) -> None:
    """无子命令时进入交互式 REPL；有子命令时透传全局选项。"""
    if ctx.obj is None:
        ctx.obj = {}
    if workspace is not None:
        ctx.obj["workspace"] = workspace
    ctx.obj["pipelines_dir"] = pipelines_dir
    ctx.obj["no_autoload"] = no_autoload

    if ctx.invoked_subcommand is None:
        from pipeline_engine.repl import run_repl
        ws = workspace if workspace is not None else Path.cwd()
        asyncio.run(run_repl(ws, pipelines_dir=pipelines_dir, no_autoload=no_autoload))


# ─── load ─────────────────────────────────────────────────────────────────────

@app.command()
def load(
    ctx: typer.Context,
    paths: list[Path] = typer.Argument(..., help="要加载的 YAML pipeline 文件路径。"),
    workspace: Optional[Path] = _workspace_option,
) -> None:
    """解析、校验并注册一个或多个 pipeline YAML 文件。"""
    from pipeline_engine.cli_json import emit

    async def _load() -> None:
        svc = _make_service(_get_workspace(workspace, ctx), ctx)
        result = await svc.cmd_load(paths)
        all_ok = all(item["ok"] for item in result["loaded"])
        if all_ok:
            emit("load", **result)
        else:
            obj = {
                "ok": False,
                "command": "load",
                **result,
                "error": {"message": "一个或多个文件加载失败", "type": "LoadError"},
            }
            typer.echo(json.dumps(obj, ensure_ascii=False, indent=2, default=str))
            raise typer.Exit(1)

    asyncio.run(_load())


# ─── lint ─────────────────────────────────────────────────────────────────────

@app.command()
def lint(
    ctx: typer.Context,
    path: Path = typer.Argument(..., help="要校验的 pipeline YAML 文件路径。"),
) -> None:
    """校验 pipeline YAML 语法与 DAG 合法性（不执行）。"""
    from pipeline_engine.cli_json import emit, emit_error
    from pipeline_engine.core.errors import PipelineError

    try:
        svc = _make_service(Path.cwd(), ctx)
        result = asyncio.run(svc.cmd_lint(path))
        emit("lint", **result)
    except PipelineError as e:
        raise emit_error("lint", e)
    except Exception as e:
        raise emit_error("lint", e)


# ─── list ─────────────────────────────────────────────────────────────────────

@app.command("list")
def list_cmd(
    ctx: typer.Context,
    workspace: Optional[Path] = _workspace_option,
    pipeline_flag: bool = typer.Option(False, "--pipeline", help="列出已注册的 pipeline（默认行为）。"),
    instance_flag: bool = typer.Option(False, "--instance", help="列出运行实例（pipeline_id / instance_id / status）。"),
) -> None:
    """列出已注册的 pipeline（默认）或运行实例（--instance）。"""
    from pipeline_engine.cli_json import emit, emit_error
    from pipeline_engine.core.errors import PipelineError

    async def _list() -> None:
        svc = _make_service(_get_workspace(workspace, ctx), ctx)
        try:
            await svc.bootstrap(restore_runs=True)
            if instance_flag:
                result = await svc.cmd_list_instances()
            else:
                result = await svc.cmd_list_pipelines()
            emit("list", **result)
        except PipelineError as e:
            raise emit_error("list", e)
        except Exception as e:
            raise emit_error("list", e)

    asyncio.run(_list())


# ─── start ────────────────────────────────────────────────────────────────────

@app.command("start")
def start_cmd(
    ctx: typer.Context,
    pipeline_ids: list[str] = typer.Argument(..., help="要运行的 pipeline ID 列表。"),
    workspace: Optional[Path] = _workspace_option,
    step: Optional[str] = typer.Option(None, "--step", "-s", help="仅运行指定 step。"),
    task: Optional[str] = typer.Option(None, "--task", "-t", help="仅运行指定 task（需配合 --step）。"),
) -> None:
    """启动一个或多个 pipeline run，阻塞直到完成。"""
    from pipeline_engine.cli_json import emit

    async def _run() -> None:
        svc = _make_service(_get_workspace(workspace, ctx), ctx)
        try:
            await svc.bootstrap(restore_runs=False)
        except Exception:
            pass  # autoload 失败不阻断

        result = await svc.cmd_start(pipeline_ids, step=step, task=task, wait=True)
        any_error = any(not r["ok"] for r in result["runs"])

        if any_error:
            obj = {
                "ok": False,
                "command": "start",
                **result,
                "error": {"message": "一个或多个 pipeline 启动失败", "type": "StartError"},
            }
            typer.echo(json.dumps(obj, ensure_ascii=False, indent=2, default=str))
            raise typer.Exit(1)
        else:
            emit("start", **result)

    asyncio.run(_run())


# ─── stop ─────────────────────────────────────────────────────────────────────

@app.command()
def stop(
    ctx: typer.Context,
    ref: str = typer.Argument(..., metavar="INSTANCE_ID", help="要中止的 pipeline 实例 ID。"),
    workspace: Optional[Path] = _workspace_option,
) -> None:
    """中止指定 pipeline 实例（整个 run）。"""
    from pipeline_engine.cli_json import emit, emit_error
    from pipeline_engine.core.errors import PipelineError

    async def _stop() -> None:
        svc = _make_service(_get_workspace(workspace, ctx), ctx)
        try:
            await svc.bootstrap(restore_runs=True)
            result = await svc.cmd_stop(ref)
            emit("stop", **result)
        except PipelineError as e:
            raise emit_error("stop", e)
        except Exception as e:
            raise emit_error("stop", e)

    asyncio.run(_stop())


# ─── resume ───────────────────────────────────────────────────────────────────

@app.command()
def resume(
    ctx: typer.Context,
    ref: str = typer.Argument(..., metavar="INSTANCE_ID", help="要恢复的 pipeline 实例 ID。"),
    workspace: Optional[Path] = _workspace_option,
    include_paused: bool = typer.Option(False, "--include-paused", help="同时恢复 PAUSED 状态的任务。"),
) -> None:
    """恢复 FAILED（或 PAUSED）的 pipeline 实例，并等待其完成。"""
    from pipeline_engine.cli_json import emit, emit_error
    from pipeline_engine.core.errors import PipelineError

    async def _resume() -> None:
        svc = _make_service(_get_workspace(workspace, ctx), ctx)
        try:
            await svc.bootstrap(restore_runs=True, restore_writeback=True)
            result = await svc.cmd_resume(ref, include_paused=include_paused)
            emit("resume", **result)
        except PipelineError as e:
            raise emit_error("resume", e)
        except Exception as e:
            raise emit_error("resume", e)

    asyncio.run(_resume())


# ─── fix ──────────────────────────────────────────────────────────────────────

@app.command()
def fix(
    ctx: typer.Context,
    ref: str = typer.Argument(..., metavar="INSTANCE_ID", help="要修复的 pipeline 实例 ID。"),
    task_locator: str = typer.Option(..., "--task", "-t", help="目标 task（格式：step_id/task_id 或 task_id）。"),
    workspace: Optional[Path] = _workspace_option,
    output_path: Optional[Path] = typer.Option(None, "--output", help="注入 output.json 路径（任务状态转为 FIXED）。"),
    input_path: Optional[Path] = typer.Option(None, "--input", help="注入 input.json 路径（任务状态重置为 NEW）。"),
) -> None:
    """向 pipeline 实例中的失败任务注入 output（FIXED）或替换 input（NEW）。"""
    from pipeline_engine.cli_json import emit, emit_error
    from pipeline_engine.core.errors import PipelineError

    async def _fix() -> None:
        svc = _make_service(_get_workspace(workspace, ctx), ctx)
        try:
            await svc.bootstrap(restore_runs=True, restore_writeback=True)
            result = await svc.cmd_fix(ref, task_locator, output_path=output_path, input_path=input_path)
            emit("fix", **result)
        except PipelineError as e:
            raise emit_error("fix", e)
        except Exception as e:
            raise emit_error("fix", e)

    asyncio.run(_fix())


# ─── status ───────────────────────────────────────────────────────────────────

@app.command()
def status(
    ctx: typer.Context,
    ref: str = typer.Argument(..., metavar="INSTANCE_ID", help="要查看状态的 pipeline 实例 ID。"),
    workspace: Optional[Path] = _workspace_option,
) -> None:
    """查看指定 pipeline 实例的整体状态与进度（JSON 输出）。"""
    from pipeline_engine.cli_json import emit, emit_error
    from pipeline_engine.core.errors import PipelineError

    async def _status() -> None:
        svc = _make_service(_get_workspace(workspace, ctx), ctx)
        try:
            await svc.bootstrap(restore_runs=True)
            result = await svc.cmd_status(ref)
            emit("status", **result)
        except PipelineError as e:
            raise emit_error("status", e)
        except Exception as e:
            raise emit_error("status", e)

    asyncio.run(_status())


# ─── inspect ──────────────────────────────────────────────────────────────────

@app.command()
def inspect(
    ctx: typer.Context,
    ref: str = typer.Argument(..., metavar="INSTANCE_ID", help="要查看的 pipeline 实例 ID。"),
    workspace: Optional[Path] = _workspace_option,
    step: Optional[str] = typer.Option(None, "--step", "-s", help="指定要查看的 step ID。"),
    task: Optional[str] = typer.Option(None, "--task", "-t", help="指定要查看的 task ID（需配合 --step）。"),
) -> None:
    """查看 pipeline 实例中 task 的详细信息（输入/输出/日志/堆栈）。"""
    from pipeline_engine.cli_json import emit, emit_error
    from pipeline_engine.core.errors import PipelineError

    async def _inspect() -> None:
        svc = _make_service(_get_workspace(workspace, ctx), ctx)
        try:
            await svc.bootstrap(restore_runs=True)
            result = await svc.cmd_inspect(ref, step=step, task=task)
            emit("inspect", **result)
        except PipelineError as e:
            raise emit_error("inspect", e)
        except Exception as e:
            raise emit_error("inspect", e)

    asyncio.run(_inspect())


# ─── log ──────────────────────────────────────────────────────────────────────

@app.command("log")
def log_cmd(
    ctx: typer.Context,
    ref: str = typer.Argument(..., metavar="INSTANCE_ID"),
    workspace: Optional[Path] = _workspace_option,
    tail: int = typer.Option(100, "--tail", help="显示最后 N 行（默认 100）。"),
    offset: int = typer.Option(0, "--offset", help="从末尾倒数第 N 行起开始显示。"),
    all_: bool = typer.Option(False, "--all", help="显示全部行。"),
    errors_only: bool = typer.Option(False, "--errors-only", help="仅显示 ERROR 行。"),
) -> None:
    """查看指定 pipeline 实例的 run.log（JSON 格式，含结构化行记录）。"""
    from pipeline_engine.cli_json import emit, emit_error
    from pipeline_engine.core.errors import PipelineError

    async def _log() -> None:
        svc = _make_service(_get_workspace(workspace, ctx), ctx)
        try:
            await svc.bootstrap(restore_runs=True)
            result = await svc.cmd_log(
                ref, tail=tail, offset=offset, all_lines=all_, errors_only=errors_only
            )
            emit("log", **result)
        except PipelineError as e:
            raise emit_error("log", e)
        except Exception as e:
            raise emit_error("log", e)

    asyncio.run(_log())


# ─── serve ────────────────────────────────────────────────────────────────────

@app.command()
def serve(
    ctx: typer.Context,
    workspace: Optional[Path] = _workspace_option,
    host: str = typer.Option("127.0.0.1", "--host", help="绑定地址（默认 127.0.0.1）。"),
    port: int = typer.Option(8765, "--port", "-p", help="监听端口（默认 8765）。"),
) -> None:
    """以 HTTP REST API 方式启动 pipeline engine 服务（127.0.0.1 本机绑定，无鉴权）。

    服务退出时所有进行中的 run 将被取消。安装 HTTP API 功能：pip install pipeline_engine[api]
    """
    try:
        import uvicorn
        from pipeline_engine.api import create_app
    except ImportError:
        typer.echo(
            "错误：HTTP API 依赖未安装。请运行：pip install pipeline_engine[api]",
            err=True,
        )
        raise typer.Exit(1)

    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.service import PipelineService

    ws = _get_workspace(workspace, ctx)

    # L4: prevent two serve processes from sharing the same workspace.
    # fcntl.flock is atomic and auto-released when the fd is GC'd on exit.
    import fcntl
    import os
    lock_dir = ws / ".pipeline_runs"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / ".serve.lock"
    _lock_fd = lock_path.open("w")
    try:
        fcntl.flock(_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        typer.echo(
            f"错误：workspace '{ws}' 已被另一个 serve 进程占用（{lock_path}）。",
            err=True,
        )
        raise typer.Exit(1)
    _lock_fd.write(str(os.getpid()))
    _lock_fd.flush()

    rm = RunManager(ws)
    svc = PipelineService(
        rm,
        pipelines_dir=_get_pipelines_dir(ctx),
        no_autoload=_is_no_autoload(ctx),
    )

    async def _serve() -> None:
        await svc.bootstrap(restore_runs=True)
        fastapi_app = create_app(svc)
        config = uvicorn.Config(fastapi_app, host=host, port=port, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()

    asyncio.run(_serve())
