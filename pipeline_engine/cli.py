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


# ─── Autoload helpers ────────────────────────────────────────────────────────

async def _autoload_pipelines(rm, base_dir: Path) -> list[dict]:
    """扫描 <base_dir>/*/pipeline.yaml 并逐一注册。

    Returns a list of per-file results: {path, pipeline_id, ok, error?}.
    单个文件失败不影响其余文件。
    """
    from pipeline_engine.core.run_manager import RunManager  # 仅用于类型注解

    results: list[dict] = []
    if not base_dir.exists() or not base_dir.is_dir():
        return results
    for yaml_path in sorted(base_dir.glob("*/pipeline.yaml")):
        try:
            pid = await rm.load(yaml_path)
            results.append({"path": str(yaml_path), "pipeline_id": pid, "ok": True})
        except Exception as e:
            results.append({
                "path": str(yaml_path),
                "pipeline_id": None,
                "ok": False,
                "error": str(e),
            })
            print(
                f"[autoload WARNING] 加载失败 {yaml_path}: {e}",
                file=sys.stderr,
            )
    return results


async def _bootstrap(rm, ctx: typer.Context, restore_runs: bool = False) -> None:
    """重建 RunManager 状态：先从 registry 恢复，再 autoload。"""
    _reload_registry(rm, restore_runs=restore_runs)
    if not _is_no_autoload(ctx):
        pd = _get_pipelines_dir(ctx)
        base_dir = pd if pd is not None else (Path.cwd() / "pipelines")
        await _autoload_pipelines(rm, base_dir)


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
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.cli_json import emit, emit_error

    async def _load() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
        loaded = []
        all_ok = True
        for p in paths:
            try:
                pid = await rm.load(p)
                loaded.append({"path": str(p), "pipeline_id": pid, "ok": True})
            except Exception as e:
                loaded.append({"path": str(p), "pipeline_id": None, "ok": False, "error": str(e)})
                all_ok = False
        if all_ok:
            emit("load", loaded=loaded)
        else:
            from pipeline_engine.cli_json import emit_error as _ee
            obj = {"ok": False, "command": "load", "loaded": loaded,
                   "error": {"message": "一个或多个文件加载失败", "type": "LoadError"}}
            typer.echo(json.dumps(obj, ensure_ascii=False, default=str))
            raise typer.Exit(1)

    asyncio.run(_load())


# ─── lint ─────────────────────────────────────────────────────────────────────

@app.command()
def lint(
    ctx: typer.Context,
    path: Path = typer.Argument(..., help="要校验的 pipeline YAML 文件路径。"),
) -> None:
    """校验 pipeline YAML 语法与 DAG 合法性（不执行）。"""
    from pipeline_engine.core.yaml_parser import load_pipeline_spec
    from pipeline_engine.core.dag_validator import validate_pipeline
    from pipeline_engine.core.errors import PipelineError
    from pipeline_engine.cli_json import emit, emit_error

    try:
        spec = load_pipeline_spec(path)
        validate_pipeline(spec)
        emit("lint", path=str(path), pipeline_id=spec.pipeline.id, valid=True)
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
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError
    from pipeline_engine.cli_json import emit, emit_error

    async def _list() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
        try:
            await _bootstrap(rm, ctx, restore_runs=True)
            if instance_flag:
                instances = await rm.list_instances()
                emit("list", scope="instance", instances=instances)
            else:
                emit("list", scope="pipeline", pipelines=rm.list_pipelines())
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
    wait: bool = typer.Option(False, "--wait", help="阻塞直到所有 run 完成。"),
) -> None:
    """启动一个或多个 pipeline run（后台并发执行）。"""
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError
    from pipeline_engine.cli_json import emit, emit_error

    async def _run() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
        try:
            await _bootstrap(rm, ctx, restore_runs=False)
        except Exception:
            pass  # autoload 失败不阻断

        runs: list[dict] = []
        any_error = False

        for pid in pipeline_ids:
            try:
                run_id = await rm.start_run(pid, step_id=step, task_id=task)
                entry: dict = {"pipeline_id": pid, "run_id": run_id, "ok": True}
                if wait:
                    if run_id in rm._runs:
                        await rm._runs[run_id].await_main()
                    state = await rm.get_run_state(run_id)
                    entry["final_status"] = state.status.value
                runs.append(entry)
            except PipelineError as e:
                runs.append({"pipeline_id": pid, "run_id": None, "ok": False, "error": str(e)})
                any_error = True

        if any_error:
            obj = {"ok": False, "command": "start", "runs": runs,
                   "error": {"message": "一个或多个 pipeline 启动失败", "type": "StartError"}}
            typer.echo(json.dumps(obj, ensure_ascii=False, default=str))
            raise typer.Exit(1)
        else:
            emit("start", runs=runs)

    asyncio.run(_run())


# ─── stop ─────────────────────────────────────────────────────────────────────

@app.command()
def stop(
    ctx: typer.Context,
    ref: str = typer.Argument(..., metavar="INSTANCE_ID"),
    workspace: Optional[Path] = _workspace_option,
) -> None:
    """中止指定 pipeline 实例（整个 run）。"""
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError
    from pipeline_engine.cli_json import emit, emit_error

    async def _stop() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
        try:
            await _bootstrap(rm, ctx, restore_runs=True)
            await rm.stop(ref)
            emit("stop", stopped=ref)
        except PipelineError as e:
            raise emit_error("stop", e)
        except Exception as e:
            raise emit_error("stop", e)

    asyncio.run(_stop())


# ─── resume ───────────────────────────────────────────────────────────────────

@app.command()
def resume(
    ctx: typer.Context,
    ref: str = typer.Argument(..., metavar="INSTANCE_ID"),
    workspace: Optional[Path] = _workspace_option,
    include_paused: bool = typer.Option(False, "--include-paused"),
) -> None:
    """恢复 FAILED（或 PAUSED）的 pipeline 实例，并等待其完成。"""
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError
    from pipeline_engine.cli_json import emit, emit_error

    async def _resume() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
        try:
            await _bootstrap(rm, ctx, restore_runs=True)
            run_id = await rm.resume(ref, include_paused=include_paused)
            run_ctx = rm._runs[run_id]
            await run_ctx.await_main()
            state = await rm.get_run_state(run_id)
            emit("resume", resumed=run_id, final_status=state.status.value)
        except PipelineError as e:
            raise emit_error("resume", e)
        except Exception as e:
            raise emit_error("resume", e)

    asyncio.run(_resume())


# ─── fix ──────────────────────────────────────────────────────────────────────

@app.command()
def fix(
    ctx: typer.Context,
    ref: str = typer.Argument(..., metavar="INSTANCE_ID"),
    task_locator: str = typer.Option(..., "--task", "-t"),
    workspace: Optional[Path] = _workspace_option,
    output_path: Optional[Path] = typer.Option(None, "--output"),
    input_path: Optional[Path] = typer.Option(None, "--input"),
) -> None:
    """向 pipeline 实例中的失败任务注入 output（FIXED）或替换 input（NEW）。"""
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError
    from pipeline_engine.cli_json import emit, emit_error

    async def _fix() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
        try:
            await _bootstrap(rm, ctx, restore_runs=True)
            await rm.fix(
                ref, task_locator,
                output_path=str(output_path) if output_path else None,
                input_path=str(input_path) if input_path else None,
            )
            if output_path:
                emit("fix", instance_id=ref, task=task_locator, mode="output", new_status="fixed")
            else:
                emit("fix", instance_id=ref, task=task_locator, mode="input", new_status="new")
        except PipelineError as e:
            raise emit_error("fix", e)
        except Exception as e:
            raise emit_error("fix", e)

    asyncio.run(_fix())


# ─── status ───────────────────────────────────────────────────────────────────

@app.command()
def status(
    ctx: typer.Context,
    ref: str = typer.Argument(..., metavar="INSTANCE_ID"),
    workspace: Optional[Path] = _workspace_option,
) -> None:
    """查看指定 pipeline 实例的整体状态与进度（JSON 输出）。"""
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError
    from pipeline_engine.cli_json import emit, emit_error

    async def _status() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
        try:
            await _bootstrap(rm, ctx, restore_runs=True)
            state = await rm.get_run_state(ref)
            emit("status", state=state.model_dump(mode="json"))
        except PipelineError as e:
            raise emit_error("status", e)
        except Exception as e:
            raise emit_error("status", e)

    asyncio.run(_status())


# ─── inspect ──────────────────────────────────────────────────────────────────

@app.command()
def inspect(
    ctx: typer.Context,
    ref: str = typer.Argument(..., metavar="INSTANCE_ID"),
    workspace: Optional[Path] = _workspace_option,
    step: Optional[str] = typer.Option(None, "--step", "-s"),
    task: Optional[str] = typer.Option(None, "--task", "-t"),
) -> None:
    """查看 pipeline 实例中 task 的详细信息（输入/输出/日志/堆栈）。"""
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError
    from pipeline_engine.cli_json import emit, emit_error, read_json_file, read_log_tail

    async def _inspect() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
        try:
            await _bootstrap(rm, ctx, restore_runs=True)
            state = await rm.get_run_state(ref)

            if step is None:
                # 无 step：返回整体 state（同 status）
                emit("inspect", state=state.model_dump(mode="json"))
                return

            step_state = state.steps.get(step)
            if step_state is None:
                raise PipelineError(
                    f"Step '{step}' 在 run '{ref}' 中不存在",
                    pipeline_id=state.pipeline_id,
                    step_id=step,
                )

            if task is None:
                # 只指定 step：返回该 step 的所有 task 详情
                tasks_detail = []
                for tid, ts in step_state.tasks.items():
                    tasks_detail.append(_build_task_json(tid, ts))
                emit("inspect", step_id=step, step_status=step_state.status.value,
                     tasks=tasks_detail)
                return

            # 指定 step + task
            ts = step_state.tasks.get(task)
            if ts is None:
                raise PipelineError(
                    f"Task '{task}' 在 step '{step}' 中不存在",
                    pipeline_id=state.pipeline_id,
                    step_id=step,
                    task_id=task,
                )
            emit("inspect", task=_build_task_json(task, ts))

        except PipelineError as e:
            raise emit_error("inspect", e)
        except Exception as e:
            raise emit_error("inspect", e)

    asyncio.run(_inspect())


def _build_task_json(task_id: str, ts) -> dict:
    """将 TaskState 序列化为 inspect 用的 JSON dict（含内联 input/output/log_tail）。"""
    from pipeline_engine.cli_json import read_json_file, read_log_tail
    return {
        "id": task_id,
        "status": ts.status.value,
        "progress": ts.progress,
        "started_at": ts.started_at.isoformat() if ts.started_at else None,
        "finished_at": ts.finished_at.isoformat() if ts.finished_at else None,
        "error": ts.error,
        "stack_trace": ts.stack_trace,
        "fixed_by": ts.fixed_by,
        "input_path": ts.input_path,
        "input": read_json_file(ts.input_path),
        "output_path": ts.output_path,
        "output": read_json_file(ts.output_path),
        "log_path": ts.log_path,
        "log_tail": read_log_tail(ts.log_path),
    }


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
    from pipeline_engine.core.run_manager import RunManager
    from pipeline_engine.core.errors import PipelineError
    from pipeline_engine.core import storage
    from pipeline_engine.cli_json import emit, emit_error, parse_log_line

    async def _log() -> None:
        rm = RunManager(_get_workspace(workspace, ctx))
        try:
            await _bootstrap(rm, ctx, restore_runs=True)
            ctx_ = rm._resolve_run(ref)
            log_path = storage.get_run_log_path(rm.workspace, ctx_.pipeline_id, ctx_.run_id)
            if not log_path.exists():
                emit("log", run_id=ref, log_path=str(log_path),
                     total=0, start=0, end=0, lines=[])
                return

            raw_lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()

            if errors_only:
                raw_lines = [ln for ln in raw_lines if " ERROR " in ln]

            total = len(raw_lines)
            if all_:
                start_idx, end_idx = 0, total
                view = raw_lines
            else:
                end_idx = max(0, total - offset)
                start_idx = max(0, end_idx - tail)
                view = raw_lines[start_idx:end_idx]

            lines = [parse_log_line(ln) for ln in view]
            emit("log", run_id=ref, log_path=str(log_path),
                 total=total, start=start_idx, end=end_idx, lines=lines)

        except PipelineError as e:
            raise emit_error("log", e)
        except Exception as e:
            raise emit_error("log", e)

    asyncio.run(_log())


# ─── 内部工具 ─────────────────────────────────────────────────────────────────

def _reload_registry(rm, restore_runs: bool = False) -> None:
    """从磁盘重新加载 pipeline 注册表（及可选的 run 状态）。"""
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
