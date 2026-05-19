"""运行管理器：进程级单例，协调所有已加载 pipeline 及其活跃 run。

职责
----
- **加载**：解析并校验 YAML 配置，写入 registry.json。
- **启动**：为每个 run 创建独立 RunContext（scheduler + state_manager + abort_event）。
- **停止**：设置 abort_event 触发有序关闭；可选强制取消 main_task。
- **恢复**：复位 FAILED/PAUSED 任务 → PENDING，重建调度器 Task。
- **修复**：向失败任务注入 input/output 数据，驱动后续 resume。
- **查询**：列出已注册 pipeline 和运行记录。
- **跨进程恢复**：`restore_runs_from_disk` 从 state.json 重建 RunContext，
  用于 CLI 一次性子命令（stop/resume/fix）访问上一进程启动的 run。

线程安全：所有 registry / runs 字典的读写通过 ``asyncio.Lock`` 序列化。
重入保护（A3）：start_run / resume / fix 会检查 ``ctx.is_active()``，
拒绝在已有活跃调度器的情况下重复启动，防止两个调度器并发写同一 StateManager。
"""
from __future__ import annotations

import asyncio
import multiprocessing
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline_engine.core import storage
from pipeline_engine.core.dag_validator import validate_pipeline
from pipeline_engine.core.errors import PipelineError
from pipeline_engine.core.run_context import RunContext
from pipeline_engine.core.run_logger import RunLogger
from pipeline_engine.core.scheduler import AsyncScheduler
from pipeline_engine.core.state_manager import StateManager
from pipeline_engine.core.yaml_parser import load_pipeline_spec
from pipeline_engine.models.pipeline_spec import PipelineSpec
from pipeline_engine.models.runtime_state import PipelineRunState, Status


def _new_run_id(pipeline_id: str) -> str:
    """生成唯一 run_id：<pipeline_id>_yyyyMMdd-HHmmss_<4位随机数>。"""
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d-%H%M%S")
    suffix = f"{secrets.randbelow(10000):04d}"
    return f"{pipeline_id}_{ts}_{suffix}"


# H1: maximum number of terminal runs kept in memory; oldest are evicted when exceeded.
_MAX_RUNS = 200


class RunManager:
    """进程级单例：管理所有已加载 pipeline 及活跃 run。

    外部代码通过本类的 async 方法操作 run，不应直接访问 ``_registry`` / ``_runs``
    等私有字典。
    """

    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace)
        self._registry: dict[str, PipelineSpec] = {}   # pipeline_id → spec
        self._runs: dict[str, RunContext] = {}          # run_id → ctx
        self._lock = asyncio.Lock()
        # 进程级并发限制：最多同时跑 cpu_count 个 task 线程
        cpu = multiprocessing.cpu_count()
        self._global_sem = asyncio.Semaphore(cpu)

    # ─── 加载 ─────────────────────────────────────────────────────────────────

    async def load(self, yaml_path: str | Path) -> str:
        """解析并注册 pipeline YAML，写入 registry.json。返回 pipeline_id。"""
        spec = load_pipeline_spec(yaml_path)
        validate_pipeline(spec)
        async with self._lock:
            self._registry[spec.pipeline.id] = spec
            reg = storage.load_registry(self.workspace)
            reg[spec.pipeline.id] = {
                "yaml_path": str(Path(yaml_path).resolve()),
                "loaded_at": datetime.now(tz=timezone.utc).isoformat(),
                "name": spec.pipeline.name,
            }
            storage.save_registry(self.workspace, reg)
        return spec.pipeline.id

    # ─── 启动 ─────────────────────────────────────────────────────────────────

    async def start_run(
        self,
        pipeline_id: str,
        *,
        step_id: str | None = None,
        task_id: str | None = None,
    ) -> str:
        """在后台启动新 run，返回 run_id。

        A3 修复：若该 pipeline 已有活跃 run（is_active() == True），则拒绝，
        防止两个调度器并发写同一 StateManager 引发状态混乱。
        """
        async with self._lock:
            spec = self._get_spec(pipeline_id)
            # A3：防止同一 pipeline 重叠运行
            for ctx in self._runs.values():
                if ctx.pipeline_id == pipeline_id and ctx.is_active():
                    raise PipelineError(
                        f"pipeline '{pipeline_id}' 已有活跃的 run '{ctx.run_id}'，"
                        "请先 stop 后再 run",
                        pipeline_id=pipeline_id,
                    )
            run_id = _new_run_id(pipeline_id)
            for _ in range(3):
                if run_id not in self._runs:
                    break
                run_id = _new_run_id(pipeline_id)
            else:
                raise PipelineError(
                    "instance_id collision after 3 retries",
                    pipeline_id=pipeline_id,
                )
            run_dir = storage.init_run_dir(self.workspace, pipeline_id, run_id)
            run_state = PipelineRunState(
                pipeline_id=pipeline_id,
                run_id=run_id,
                workspace=str(run_dir),
            )
            sm = StateManager(run_state)
            abort_event = asyncio.Event()
            log_path = storage.get_run_log_path(self.workspace, pipeline_id, run_id)
            run_logger = RunLogger(run_id, log_path)
            sched = AsyncScheduler(
                spec, sm, self.workspace, abort_event, self._global_sem, run_logger
            )
            ctx = RunContext(
                pipeline_spec=spec,
                run_id=run_id,
                workspace=run_dir,
                scheduler=sched,
                state_manager=sm,
                abort_event=abort_event,
                run_logger=run_logger,
            )
            self._runs[run_id] = ctx
            self._prune_terminal_runs()  # H1: keep _runs bounded
            # H9: assign main_task inside the lock so is_active() is immediately
            # consistent with the run being in _runs.
            if step_id and task_id:
                coro = ctx.scheduler.run_task(step_id, task_id)
            elif step_id:
                coro = ctx.scheduler.run_step(step_id)
            else:
                coro = ctx.scheduler.run()
            ctx.main_task = asyncio.create_task(coro, name=f"run-{run_id}")
        return run_id

    # ─── 停止 ─────────────────────────────────────────────────────────────────

    async def stop(self, ref: str) -> None:
        """触发有序中止：设置 abort_event，不再分发新 task。

        H4 修复：在 _lock 内读取并 set abort_event，与 resume() 的替换操作互斥，
        防止 stop() 读到旧 abort_event 而 resume() 已换新的竞态。
        """
        async with self._lock:
            ctx = self._resolve_run(ref)
            ctx.abort_event.set()

    # ─── 恢复 ─────────────────────────────────────────────────────────────────

    async def resume(self, ref: str, *, include_paused: bool = False) -> str:
        """恢复 FAILED/PAUSED 的 run。

        A3 修复：检查是否已有活跃调度器，拒绝重入。
        C1 修复：通过 ``sm.reset_pipeline_status()`` 公共 API 重置 pipeline 状态，
        不再直接访问 ``sm._lock`` / ``sm._state``。

        流程：
        1. 将 FAILED（及可选 PAUSED）task 复位为 PENDING。
        2. 重置 pipeline 状态为 PENDING。
        3. 刷新 abort_event（允许再次被 stop）。
        4. 创建新的 asyncio.Task 驱动调度器。
        """
        ctx = self._resolve_run(ref)

        # A3：防止重叠运行
        if ctx.is_active():
            raise PipelineError(
                f"run '{ctx.run_id}' 已处于活跃状态，请先 stop 后再 resume",
                pipeline_id=ctx.pipeline_id,
            )

        sm = ctx.state_manager
        run_state = await sm.get_run_state()

        for step_state in run_state.steps.values():
            for tid in step_state.tasks:
                await sm.reset_for_resume(step_state.id, tid, include_paused=include_paused)

        # C1：使用公共 API 重置 pipeline 状态，避免直接访问内部属性
        await sm.reset_pipeline_status(Status.NEW)

        # H4/H9: assign abort_event and main_task atomically inside lock.
        # stop() reads abort_event under the same lock, so the new event and
        # the new task are always visible together — no intermediate state where
        # abort_event is new but main_task is still the completed previous task.
        new_event = asyncio.Event()
        async with self._lock:
            ctx.abort_event = new_event
            ctx.scheduler._abort_event = new_event
            ctx.main_task = asyncio.create_task(
                ctx.scheduler.run(), name=f"run-{ctx.run_id}-resume"
            )
        return ctx.run_id

    # ─── 修复 ─────────────────────────────────────────────────────────────────

    async def fix(
        self,
        ref: str,
        task_locator: str,
        *,
        input_path: str | Path | None = None,
        output_path: str | Path | None = None,
    ) -> None:
        """手动注入 input 或 output 以修复失败任务。

        task_locator 格式：``step_id/task_id``（或仅 task_id，将在所有 step 搜索）。

        A3 修复：若 run 正在活跃运行，拒绝 fix，防止并发写入。
        B4 修复：fix --output 在写盘前先对 JSON 做 OutputModel 校验，
        确保注入的数据符合任务契约。
        C1 修复：fix --input 改用 ``sm.replace_task_input()`` 公共 API，
        不再直接访问 ``sm._lock`` / ``sm._state``。
        """
        ctx = self._resolve_run(ref)

        # A3：活跃运行时禁止 fix，防止并发改写状态
        if ctx.is_active():
            raise PipelineError(
                f"run '{ctx.run_id}' 正在运行，请先 stop 后再 fix",
                pipeline_id=ctx.pipeline_id,
            )

        step_id, task_id = self._parse_task_locator(ctx, task_locator)
        pipeline_id = ctx.pipeline_id
        run_id = ctx.run_id

        if output_path:
            # B4：先做 OutputModel 校验，验证通过再写盘
            src = Path(output_path)
            if not src.exists():
                raise PipelineError(f"input file not found: {src}")
            try:
                raw_data = storage.read_json(src)
            except Exception as exc:
                raise PipelineError(f"fix source is not valid JSON: {exc}") from exc

            # 找到 task spec，若有 OutputModel 则校验
            task_spec = self._find_task_spec(ctx, step_id, task_id)
            if task_spec is not None:
                from pipeline_engine.core.plugin_loader import instantiate_task as _inst
                try:
                    inst = _inst(task_spec.plugin, task_id, task_spec.config)
                    raw_data = inst.validate_output(raw_data)
                except PipelineError as exc:
                    raise PipelineError(
                        f"fix --output data does not satisfy OutputModel: {exc}",
                        pipeline_id=pipeline_id,
                        step_id=step_id,
                        task_id=task_id,
                    ) from exc

            dest = storage.fix_output(
                self.workspace, pipeline_id, run_id, step_id, task_id, src
            )
            fixed_by = f"fix-output@{datetime.now(tz=timezone.utc).isoformat()}"
            await ctx.state_manager.recover_task(
                step_id, task_id,
                output_path=str(dest),
                fixed_by=fixed_by,
            )
        elif input_path:
            src = Path(input_path)
            if not src.exists():
                raise PipelineError(f"input file not found: {src}")
            task_dir = storage.init_task_dir(
                self.workspace, pipeline_id, run_id, step_id, task_id
            )
            storage.atomic_write_json(task_dir / "input.json", storage.read_json(src))
            # C1：使用公共 API 复位任务状态，不再直接访问 _lock/_state
            await ctx.state_manager.replace_task_input(step_id, task_id)
        else:
            raise PipelineError("fix requires --input or --output")

    # ─── 查询 ─────────────────────────────────────────────────────────────────

    def list_pipelines(self) -> list[dict[str, Any]]:
        """列出所有已注册的 pipeline（含 type 字段）。"""
        return [
            {"pipeline_id": pid, "type": spec.pipeline.type, "name": spec.pipeline.name}
            for pid, spec in self._registry.items()
        ]

    async def list_instances(self) -> list[dict[str, Any]]:
        """列出所有运行实例的摘要信息（pipeline_id / instance_id / status）。"""
        result = []
        for run_id, ctx in self._runs.items():
            state = await ctx.state_manager.get_run_state()
            result.append({
                "pipeline_id": ctx.pipeline_id,
                "instance_id": run_id,
                "status": state.status.value,
            })
        return result

    def list_runs(self) -> list[dict[str, Any]]:
        """列出所有已知 run 的摘要信息。"""
        result = []
        for run_id, ctx in self._runs.items():
            result.append({
                "run_id": run_id,
                "pipeline_id": ctx.pipeline_id,
                "active": ctx.is_active(),
            })
        return result

    async def get_run_state(self, ref: str) -> PipelineRunState:
        """返回指定 run 的完整状态快照。"""
        ctx = self._resolve_run(ref)
        return await ctx.state_manager.get_run_state()

    # ─── 跨进程恢复 ───────────────────────────────────────────────────────────

    def restore_runs_from_disk(self, write_back: bool = True) -> None:
        """从磁盘重建所有持久化 run 的 RunContext。

        供 CLI 一次性子命令（stop / resume / fix）调用，使其能操作上一进程
        启动的 run。重建后 main_task 为 None（原进程已消失），调度器和状态
        管理器均完整重建，fix / resume 可正常工作。

        ``write_back=True``（默认）：调用 ``demote_orphans_sync()`` 将上一进程崩溃
        遗留的 RUNNING 状态复位为 FAILED 并写回磁盘，供 resume 重调度。
        ``write_back=False``：只读模式，跳过降级，不修改磁盘上任何文件。
        用于 status / inspect / log 等只需读取状态的命令，避免污染仍在
        另一进程（如 REPL）中运行的 run。
        """
        runs_root = storage.get_runs_root(self.workspace)
        if not runs_root.exists():
            return
        for pid_dir in runs_root.iterdir():
            if not pid_dir.is_dir() or pid_dir.name == "registry.json":
                continue
            pipeline_id = pid_dir.name
            if pipeline_id not in self._registry:
                continue
            spec = self._registry[pipeline_id]
            for run_dir in pid_dir.iterdir():
                if not run_dir.is_dir():
                    continue
                run_id = run_dir.name
                if run_id in self._runs:
                    continue
                try:
                    run_state = storage.load_state(self.workspace, pipeline_id, run_id)
                except PipelineError:
                    continue
                sm = StateManager(run_state)
                if write_back:
                    # 将崩溃遗留的 RUNNING 状态复位为 FAILED，写回磁盘供 resume 重调度
                    sm.demote_orphans_sync()
                abort_event = asyncio.Event()
                log_path = storage.get_run_log_path(self.workspace, pipeline_id, run_id)
                run_logger = RunLogger(run_id, log_path)
                sched = AsyncScheduler(
                    spec, sm, self.workspace, abort_event, self._global_sem, run_logger
                )
                ctx = RunContext(
                    pipeline_spec=spec,
                    run_id=run_id,
                    workspace=run_dir,
                    scheduler=sched,
                    state_manager=sm,
                    abort_event=abort_event,
                    run_logger=run_logger,
                )
                self._runs[run_id] = ctx

    # ─── 内部工具方法 ─────────────────────────────────────────────────────────

    def _prune_terminal_runs(self) -> None:
        """驱逐最旧的终态 run，将 _runs 总量限制在 _MAX_RUNS 以内。

        H1 修复：防止 serve 模式长期运行时 _runs 无限增长导致 OOM。
        仅驱逐 is_active()==False 的 run（RUNNING/PAUSED 不动）。
        必须在持有 self._lock 时调用。
        """
        if len(self._runs) <= _MAX_RUNS:
            return
        terminal = [run_id for run_id, ctx in self._runs.items() if not ctx.is_active()]
        excess = len(self._runs) - _MAX_RUNS
        for run_id in terminal[:excess]:
            del self._runs[run_id]

    def _get_spec(self, pipeline_id: str) -> PipelineSpec:
        """按 pipeline_id 查找已注册 spec，不存在则抛出 PipelineError。"""
        if pipeline_id not in self._registry:
            raise PipelineError(
                f"pipeline '{pipeline_id}' 未加载 — 请先执行 'load <path>'",
                pipeline_id=pipeline_id,
            )
        return self._registry[pipeline_id]

    def _resolve_run(self, ref: str) -> RunContext:
        """将 ref 解析为 RunContext：优先作为 run_id，其次作为 pipeline_id。"""
        if ref in self._runs:
            return self._runs[ref]
        # 按 pipeline_id 查找：必须唯一
        matching = [ctx for ctx in self._runs.values() if ctx.pipeline_id == ref]
        if len(matching) == 1:
            return matching[0]
        if len(matching) > 1:
            run_ids = [ctx.run_id for ctx in matching]
            raise PipelineError(
                f"'{ref}' 匹配多个活跃 run {run_ids}，请使用 run_id 而非 pipeline_id",
                pipeline_id=ref,
            )
        raise PipelineError(f"未找到 run '{ref}'")

    def _parse_task_locator(self, ctx: RunContext, locator: str) -> tuple[str, str]:
        """解析 'step_id/task_id' 格式，或在所有 step 中搜索 task_id。"""
        if "/" in locator:
            parts = locator.split("/", 1)
            return parts[0], parts[1]
        for step in ctx.pipeline_spec.steps:
            for task in step.tasks:
                if task.id == locator:
                    return step.id, task.id
        raise PipelineError(
            f"task '{locator}' 在 pipeline '{ctx.pipeline_id}' 中未找到",
            pipeline_id=ctx.pipeline_id,
        )

    def _find_task_spec(self, ctx: RunContext, step_id: str, task_id: str):
        """查找指定 task 的 TaskSpec，未找到返回 None。"""
        for step in ctx.pipeline_spec.steps:
            if step.id == step_id:
                for task in step.tasks:
                    if task.id == task_id:
                        return task
        return None
