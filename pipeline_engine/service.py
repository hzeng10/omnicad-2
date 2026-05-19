"""PipelineService：命令业务逻辑的唯一来源。

CLI 子命令和 REST API 路由都通过本模块操作引擎，不直接依赖 typer.Context 或
HTTP 框架，保持 transport 层与业务层解耦。

用法
----
CLI（每次命令独立创建）::

    rm  = RunManager(workspace)
    svc = PipelineService(rm, pipelines_dir=pd, no_autoload=no_autoload)
    await svc.bootstrap(restore_runs=True)
    result = await svc.cmd_status(ref)
    cli_json.emit("status", **result)

REST server（进程级单例，启动时 bootstrap 一次）::

    rm  = RunManager(workspace)
    svc = PipelineService(rm, pipelines_dir=pd)
    await svc.bootstrap(restore_runs=True, restore_writeback=False)
    app.state.svc = svc

每个 ``cmd_*`` 方法在成功时返回 payload dict（直接用于 cli_json.emit / JSON 响应体），
失败时抛出 PipelineError 或其他 Exception，由调用层处理。

``load`` / ``start`` 的返回 dict 中包含各项 ``ok`` 字段，调用层需自行判断
是否存在部分失败并决定 HTTP 状态码 / exit code。
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from pipeline_engine.core.errors import PipelineError
from pipeline_engine.core.run_manager import RunManager


# ─── 注册表 / Autoload 工具（供 CLI、REPL、REST server 共享） ─────────────────

def reload_registry(
    rm: RunManager,
    restore_runs: bool = False,
    restore_writeback: bool = False,
) -> None:
    """从磁盘重新加载 pipeline 注册表（及可选的 run 状态）。

    直接操作 ``rm._registry`` 是合理的包内访问——service 是 RunManager 的协作者，
    不是外部客户端。
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
        rm.restore_runs_from_disk(write_back=restore_writeback)


async def autoload_pipelines(rm: RunManager, base_dir: Path) -> list[dict[str, Any]]:
    """扫描 <base_dir>/*/pipeline.yaml 并逐一注册。

    Returns a list of per-file results: {path, pipeline_id, ok, error?}.
    单个文件失败不影响其余文件；失败信息写 stderr 以不污染 JSON stdout。
    """
    results: list[dict[str, Any]] = []
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


# ─── PipelineService ─────────────────────────────────────────────────────────

class PipelineService:
    """所有 pipeline 命令的业务逻辑层；框架无关。"""

    def __init__(
        self,
        rm: RunManager,
        *,
        pipelines_dir: Path | None = None,
        no_autoload: bool = False,
    ) -> None:
        self._rm = rm
        self._pipelines_dir = pipelines_dir
        self._no_autoload = no_autoload

    @property
    def rm(self) -> RunManager:
        return self._rm

    async def bootstrap(
        self,
        restore_runs: bool = False,
        restore_writeback: bool = False,
    ) -> None:
        """从磁盘加载注册表（及可选 run 状态），然后执行 autoload。

        CLI 每个命令调用一次；REST server 在 lifespan 启动时调用一次。
        """
        reload_registry(self._rm, restore_runs=restore_runs, restore_writeback=restore_writeback)
        if not self._no_autoload:
            base_dir = self._pipelines_dir or (Path.cwd() / "pipelines")
            await autoload_pipelines(self._rm, base_dir)

    # ── lint ──────────────────────────────────────────────────────────────────

    async def cmd_lint(self, path: Path) -> dict[str, Any]:
        """校验 pipeline YAML，不执行。成功返回 {path, pipeline_id, valid:True}。"""
        from pipeline_engine.core.yaml_parser import load_pipeline_spec
        from pipeline_engine.core.dag_validator import validate_pipeline

        spec = load_pipeline_spec(path)
        validate_pipeline(spec)
        return {"path": str(path), "pipeline_id": spec.pipeline.id, "valid": True}

    # ── load ──────────────────────────────────────────────────────────────────

    async def cmd_load(self, paths: list[Path]) -> dict[str, Any]:
        """注册一批 pipeline YAML。

        返回 ``{"loaded": [{path, pipeline_id, ok, error?}, ...]}``。
        调用层通过 ``all(item["ok"] for item in result["loaded"])`` 判断是否全部成功。
        """
        loaded: list[dict[str, Any]] = []
        for p in paths:
            try:
                pid = await self._rm.load(p)
                loaded.append({"path": str(p), "pipeline_id": pid, "ok": True})
            except Exception as e:
                loaded.append({"path": str(p), "pipeline_id": None, "ok": False, "error": str(e)})
        return {"loaded": loaded}

    # ── list ──────────────────────────────────────────────────────────────────

    async def cmd_list_pipelines(self) -> dict[str, Any]:
        """返回 ``{"scope":"pipeline", "pipelines":[...]}``。"""
        return {"scope": "pipeline", "pipelines": self._rm.list_pipelines()}

    async def cmd_list_instances(self) -> dict[str, Any]:
        """返回 ``{"scope":"instance", "instances":[...]}``。"""
        return {"scope": "instance", "instances": await self._rm.list_instances()}

    # ── start ─────────────────────────────────────────────────────────────────

    async def cmd_start(
        self,
        pipeline_ids: list[str],
        *,
        step: str | None = None,
        task: str | None = None,
        wait: bool = True,
    ) -> dict[str, Any]:
        """启动一批 pipeline run。

        返回 ``{"runs": [{pipeline_id, run_id, ok, final_status?, error?}, ...]}``。
        ``wait=True`` 时阻塞到每个 run 完成并附上 final_status。
        调用层通过 ``all(r["ok"] for r in result["runs"])`` 判断是否全部成功。
        """
        runs: list[dict[str, Any]] = []
        for pid in pipeline_ids:
            try:
                run_id = await self._rm.start_run(pid, step_id=step, task_id=task)
                entry: dict[str, Any] = {"pipeline_id": pid, "run_id": run_id, "ok": True}
                if wait:
                    if run_id in self._rm._runs:
                        await self._rm._runs[run_id].await_main()
                    state = await self._rm.get_run_state(run_id)
                    entry["final_status"] = state.status.value
                runs.append(entry)
            except PipelineError as e:
                runs.append({"pipeline_id": pid, "run_id": None, "ok": False, "error": str(e)})
        return {"runs": runs}

    # ── stop ──────────────────────────────────────────────────────────────────

    async def cmd_stop(self, ref: str) -> dict[str, Any]:
        """触发 abort_event。返回 ``{"stopped": ref}``。"""
        await self._rm.stop(ref)
        return {"stopped": ref}

    # ── resume ────────────────────────────────────────────────────────────────

    async def cmd_resume(
        self,
        ref: str,
        *,
        include_paused: bool = False,
    ) -> dict[str, Any]:
        """恢复 run 并阻塞到完成。返回 ``{"resumed", "final_status"}``。"""
        run_id = await self._rm.resume(ref, include_paused=include_paused)
        run_ctx = self._rm._runs[run_id]
        await run_ctx.await_main()
        state = await self._rm.get_run_state(run_id)
        return {"resumed": run_id, "final_status": state.status.value}

    # ── fix ───────────────────────────────────────────────────────────────────

    async def cmd_fix(
        self,
        ref: str,
        task_locator: str,
        *,
        output_path: Path | None = None,
        input_path: Path | None = None,
    ) -> dict[str, Any]:
        """注入 output/input 修复数据。返回 ``{instance_id, task, mode, new_status}``。"""
        await self._rm.fix(
            ref,
            task_locator,
            output_path=str(output_path) if output_path else None,
            input_path=str(input_path) if input_path else None,
        )
        if output_path:
            return {"instance_id": ref, "task": task_locator, "mode": "output", "new_status": "fixed"}
        return {"instance_id": ref, "task": task_locator, "mode": "input", "new_status": "new"}

    # ── status ────────────────────────────────────────────────────────────────

    async def cmd_status(self, ref: str) -> dict[str, Any]:
        """返回 ``{"state": <PipelineStatusView dict>}``。"""
        from pipeline_engine.view_model import build_pipeline_status_view

        state = await self._rm.get_run_state(ref)
        return {"state": build_pipeline_status_view(state).model_dump(mode="json")}

    # ── inspect ───────────────────────────────────────────────────────────────

    async def cmd_inspect(
        self,
        ref: str,
        *,
        step: str | None = None,
        task: str | None = None,
    ) -> dict[str, Any]:
        """查看 pipeline / step / task 详情，按 step/task 指定粒度返回不同形状。

        - 无 step    → 同 cmd_status 的 state dict
        - 仅 step    → {step_id, step_status, tasks:[TaskStatusView...]}
        - step+task  → {task: TaskDetailView}
        """
        from pipeline_engine.view_model import (
            build_pipeline_status_view,
            build_task_detail_view,
            build_task_status_view,
        )

        state = await self._rm.get_run_state(ref)

        if step is None:
            return {"state": build_pipeline_status_view(state).model_dump(mode="json")}

        step_state = state.steps.get(step)
        if step_state is None:
            raise PipelineError(
                f"Step '{step}' 在 run '{ref}' 中不存在",
                pipeline_id=state.pipeline_id,
                step_id=step,
            )

        if task is None:
            tasks_detail = [
                build_task_status_view(ts).model_dump(mode="json")
                for ts in step_state.tasks.values()
            ]
            return {
                "step_id": step,
                "step_status": step_state.status.value,
                "tasks": tasks_detail,
            }

        ts = step_state.tasks.get(task)
        if ts is None:
            raise PipelineError(
                f"Task '{task}' 在 step '{step}' 中不存在",
                pipeline_id=state.pipeline_id,
                step_id=step,
                task_id=task,
            )
        return {"task": build_task_detail_view(ts, log_tail_size=100).model_dump(mode="json")}

    # ── log ───────────────────────────────────────────────────────────────────

    async def cmd_log(
        self,
        ref: str,
        *,
        tail: int = 100,
        offset: int = 0,
        all_lines: bool = False,
        errors_only: bool = False,
    ) -> dict[str, Any]:
        """读取 run.log 并返回结构化行列表。

        返回 ``{run_id, log_path, total, start, end, lines:[{timestamp,level,ctx,message,raw}...]}``.
        """
        from pipeline_engine.core import storage
        from pipeline_engine.cli_json import parse_log_line

        ctx = self._rm._resolve_run(ref)
        log_path = storage.get_run_log_path(
            self._rm.workspace, ctx.pipeline_id, ctx.run_id
        )

        if not log_path.exists():
            return {
                "run_id": ref,
                "log_path": str(log_path),
                "total": 0,
                "start": 0,
                "end": 0,
                "lines": [],
            }

        raw_lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()

        if errors_only:
            raw_lines = [ln for ln in raw_lines if " ERROR " in ln]

        total = len(raw_lines)
        if all_lines:
            start_idx, end_idx = 0, total
            view = raw_lines
        else:
            end_idx = max(0, total - offset)
            start_idx = max(0, end_idx - tail)
            view = raw_lines[start_idx:end_idx]

        lines = [parse_log_line(ln) for ln in view]
        return {
            "run_id": ref,
            "log_path": str(log_path),
            "total": total,
            "start": start_idx,
            "end": end_idx,
            "lines": lines,
        }
