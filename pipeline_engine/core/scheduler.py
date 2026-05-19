"""异步调度器：驱动单次 pipeline run 的完整执行流程。

架构说明
--------
- 按拓扑排序依次执行 step；同代 step（无相互依赖）可并行运行。
- step 内依据 task 依赖图，使用 asyncio.gather 并发分发就绪 task。
- 依赖就绪判定：上游 output.json 文件存在即视为就绪（不依赖状态字段），
  这样 FIXED 任务的 output.json 同样可供下游消费。
- 进程级 Semaphore（由 RunManager 注入）限制所有 run 的总并发线程数。
- abort_event 信号触发有序关闭：不再分发新任务，已在途任务自然完成后
  将状态置为 PAUSED。
- 插件加载失败（B7）统一走 fail_task 路径，不向上冒泡崩溃整个 pipeline。
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pipeline_engine.core import storage
from pipeline_engine.core.dag_validator import build_task_graph, build_step_graph
from pipeline_engine.core.errors import PipelineError
from pipeline_engine.core.plugin_loader import instantiate_task
from pipeline_engine.core.run_logger import RunLogger
from pipeline_engine.models.runtime_state import Status

if TYPE_CHECKING:
    from pipeline_engine.core.state_manager import StateManager
    from pipeline_engine.models.pipeline_spec import PipelineSpec, StepSpec

logger = logging.getLogger(__name__)


class AsyncScheduler:
    """单次 PipelineRunState 的异步调度器。

    外部代码通过 ``run()`` / ``run_step()`` / ``run_task()`` 触发执行，
    不应直接操作内部方法。
    """

    def __init__(
        self,
        spec: "PipelineSpec",
        state_manager: "StateManager",
        workspace: str | Path,
        abort_event: asyncio.Event,
        global_semaphore: asyncio.Semaphore,
        run_logger: RunLogger | None = None,
    ) -> None:
        self._spec = spec
        self._sm = state_manager
        self._workspace = Path(workspace)
        self._abort_event = abort_event
        self._global_sem = global_semaphore
        self._run_logger = run_logger

    @property
    def _pipeline_id(self) -> str:
        return self._spec.pipeline.id

    @property
    def _run_id(self) -> str:
        # StateManager._state 为内部属性；调度器与 SM 同属一个 RunContext，允许访问
        return self._sm._state.run_id  # type: ignore[attr-defined]

    # ─── 公共 API ──────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """端到端执行整个 pipeline（所有 step 按拓扑排序）。"""
        if self._run_logger:
            self._run_logger.attach()
        try:
            await self._sm.start_pipeline()
            if self._run_logger:
                self._run_logger.info("pipeline execution started")
            import networkx as nx
            step_graph = build_step_graph(self._spec)
            for step_gen in nx.topological_generations(step_graph):
                for step_id in step_gen:
                    step_spec = self._get_step(step_id)
                    await self._run_step(step_spec)
            # 检查整体结果：所有 step 均成功/跳过/已恢复才算 SUCCESS
            run_state = await self._sm.get_run_state()
            all_ok = all(
                step.status in (Status.SUCCESS, Status.SKIPPED, Status.FIXED)
                for step in run_state.steps.values()
            )
            if self._run_logger:
                status_word = "SUCCESS" if all_ok else "FAILED"
                self._run_logger.info("pipeline finished — status=%s", status_word)
            # 将 pipeline 聚合输出写入用户声明的路径（写失败不阻塞 pipeline 完成）
            pipeline_output_path = storage.resolve_output_path(
                self._workspace, self._spec.pipeline.output
            )
            if pipeline_output_path is not None and all_ok:
                aggregated = {s.id: self._collect_step_outputs(s.id) for s in self._spec.steps}
                try:
                    pipeline_output_path.parent.mkdir(parents=True, exist_ok=True)
                    storage.atomic_write_json(pipeline_output_path, aggregated)
                    if self._run_logger:
                        self._run_logger.info("pipeline output written: %s", pipeline_output_path)
                except OSError as exc:
                    if self._run_logger:
                        self._run_logger.warning(
                            "pipeline output write failed: %s (%s)", pipeline_output_path, exc
                        )
            await self._sm.finish_pipeline(success=all_ok)
        except Exception:
            if self._run_logger:
                self._run_logger.error("pipeline aborted with exception")
            await self._sm.finish_pipeline(success=False)
            raise
        finally:
            if self._run_logger:
                self._run_logger.detach()

    async def run_step(self, step_id: str) -> None:
        """仅执行指定 step（``run --step`` 模式）。"""
        if self._run_logger:
            self._run_logger.attach()
        try:
            step_spec = self._get_step(step_id)
            await self._run_step(step_spec)
        finally:
            if self._run_logger:
                self._run_logger.detach()

    async def run_task(self, step_id: str, task_id: str) -> None:
        """仅执行指定 task（``run --task`` 模式）。"""
        if self._run_logger:
            self._run_logger.attach()
        try:
            step_spec = self._get_step(step_id)
            task_spec = next((t for t in step_spec.tasks if t.id == task_id), None)
            if task_spec is None:
                raise PipelineError(
                    f"task '{task_id}' not found in step '{step_id}'",
                    pipeline_id=self._pipeline_id,
                    step_id=step_id,
                )
            await self._sm.init_step(step_id, [task_id])
            await self._dispatch_task(step_spec, task_spec)
        finally:
            if self._run_logger:
                self._run_logger.detach()

    # ─── step 执行 ────────────────────────────────────────────────────────────

    async def _run_step(self, step: "StepSpec") -> None:
        """执行单个 step：初始化 → skip 检测 → 并发分发 task → 收尾。"""
        task_ids = [t.id for t in step.tasks]
        await self._sm.init_step(step.id, task_ids)

        # abort 已触发：将所有 PENDING task 置为 PAUSED，不再分发
        if self._abort_event.is_set():
            for tid in task_ids:
                await self._sm.pause_task(step.id, tid)
            await self._sm.finish_step(step.id, success=False)
            return

        if step.skip:
            await self._handle_skip(step)
            return

        await self._sm.start_step(step.id)

        # step 级并发限制（来自 YAML max_parallelism 字段）
        step_sem = asyncio.Semaphore(
            step.max_parallelism or self._spec.pipeline.max_parallelism
        )
        import networkx as nx
        task_graph = build_task_graph(step)

        # 仅跳过 FIXED / SKIPPED 终态 task；SUCCESS 任务可能需要消费修正后的上游
        _skip_statuses = (Status.FIXED, Status.SKIPPED)
        pre_state = await self._sm.get_run_state()
        pre_step = pre_state.steps.get(step.id)
        already_done: set[str] = {
            t.id for t in step.tasks
            if pre_step and pre_step.tasks.get(t.id)
            and pre_step.tasks[t.id].status in _skip_statuses
        }

        # 每个 task 对应一个完成事件，供下游等待
        completion_events: dict[str, asyncio.Event] = {
            t.id: asyncio.Event() for t in step.tasks
        }

        async def run_with_sem(task_spec) -> None:
            # 等待 step 内上游依赖完成
            for dep_id in task_spec.depends_on:
                await completion_events[dep_id].wait()

            # 收到 abort 信号：将本 task 置为 PAUSED
            if self._abort_event.is_set():
                await self._sm.pause_task(step.id, task_spec.id)
                completion_events[task_spec.id].set()
                return

            # 已处于终态（RECOVERED/SKIPPED），直接跳过
            if task_spec.id in already_done:
                completion_events[task_spec.id].set()
                return

            async with step_sem:
                async with self._global_sem:
                    await self._dispatch_task(step, task_spec)
            completion_events[task_spec.id].set()

        await asyncio.gather(*[run_with_sem(t) for t in step.tasks])

        # 汇总 step 结果
        run_state = await self._sm.get_run_state()
        step_state = run_state.steps[step.id]
        success = all(
            ts.status in (Status.SUCCESS, Status.FIXED)
            for ts in step_state.tasks.values()
        )
        # 将 step 聚合输出写入用户声明的路径（写失败不阻塞 step 完成）
        step_output_path = storage.resolve_output_path(self._workspace, step.output)
        if step_output_path is not None and success:
            aggregated = self._collect_step_outputs(step.id)
            try:
                step_output_path.parent.mkdir(parents=True, exist_ok=True)
                storage.atomic_write_json(step_output_path, aggregated)
                if self._run_logger:
                    self._run_logger.info(
                        "step output written: %s → %s", step.id, step_output_path
                    )
            except OSError as exc:
                if self._run_logger:
                    self._run_logger.warning(
                        "step output write failed: %s (%s)", step_output_path, exc
                    )
        await self._sm.finish_step(step.id, success=success)

    async def _handle_skip(self, step: "StepSpec") -> None:
        """处理 skip=true 的 step：校验预置数据存在后置为 SKIPPED。

        若 step 配置了 output: PATH，从该路径读取预置数据；否则回退到
        manual_data/<step_id>/output.json（向后兼容）。
        """
        if step.output:
            path = storage.resolve_output_path(self._workspace, step.output)
            if not path.exists():  # type: ignore[union-attr]
                raise PipelineError(
                    f"step '{step.id}' 标记为 skip=true，但 output: 路径 {path} 不存在",
                    pipeline_id=self._pipeline_id,
                    step_id=step.id,
                )
        else:
            try:
                storage.load_manual_data(self._workspace, step.id)
            except PipelineError:
                raise PipelineError(
                    f"step '{step.id}' 标记为 skip=true，但 manual_data/{step.id}/output.json 不存在",
                    pipeline_id=self._pipeline_id,
                    step_id=step.id,
                )
        if self._run_logger:
            self._run_logger.info("step skipped: %s", step.id)
        await self._sm.skip_step(step.id)

    # ─── task 执行 ────────────────────────────────────────────────────────────

    async def _dispatch_task(self, step: "StepSpec", task_spec) -> None:
        """加载插件、执行 task、写结果、更新状态。

        B7 修复：插件加载失败（PipelineError）不再向上冒泡，而是走 fail_task
        路径，令用户可通过 fix/resume 单点修复，不影响其他 task。
        """
        step_id = step.id
        task_id = task_spec.id

        inputs = await self._build_inputs(step, task_spec)
        task_dir = storage.init_task_dir(
            self._workspace, self._pipeline_id, self._run_id, step_id, task_id
        )
        input_path = task_dir / "input.json"
        output_path = task_dir / "output.json"
        log_path = task_dir / "log.txt"

        storage.atomic_write_json(input_path, inputs)
        await self._sm.start_task(step_id, task_id)

        async def progress_cb(value: int) -> None:
            await self._sm.update_progress(step_id, task_id, value)

        from contextlib import nullcontext
        _log_ctx = (
            self._run_logger.task_context(step_id, task_id)
            if self._run_logger
            else nullcontext()
        )
        try:
            # Exceptions are caught below; abort_event check takes priority over fail_task
            with _log_ctx:
                task_instance = instantiate_task(task_spec.plugin, task_id, task_spec.config)
                validated_inputs = task_instance.validate_input(inputs)
                output = await task_instance.execute(validated_inputs, progress_cb)
                validated_output = task_instance.validate_output(output)
                # 先原子写盘，再更新内存状态（保证崩溃后可从文件恢复）
                storage.atomic_write_json(output_path, validated_output)
                # MIRROR：将结果额外复制到用户声明的路径（写失败不阻塞 task 完成）
                mirror_dest = storage.resolve_output_path(self._workspace, task_spec.output)
                if mirror_dest is not None:
                    try:
                        mirror_dest.parent.mkdir(parents=True, exist_ok=True)
                        storage.atomic_write_json(mirror_dest, validated_output)
                        if self._run_logger:
                            self._run_logger.info(
                                "task output mirrored: %s → %s", task_id, mirror_dest
                            )
                    except OSError as exc:
                        if self._run_logger:
                            self._run_logger.warning(
                                "task output mirror failed: %s (%s)", mirror_dest, exc
                            )
            await self._sm.finish_task(
                step_id,
                task_id,
                input_path=str(input_path),
                output_path=str(output_path),
                log_path=str(log_path) if log_path.exists() else None,
            )
        except asyncio.CancelledError:
            # CancelledError is BaseException; if abort was requested, record PAUSED not FAILED
            if self._abort_event.is_set():
                await self._sm.pause_task(step_id, task_id)
                return
            await self._sm.fail_task(step_id, task_id, error="CancelledError: task was cancelled unexpectedly")
            raise
        except Exception as exc:
            # H6: a plugin may catch CancelledError and re-raise as PipelineError;
            # honour the abort signal so the task lands PAUSED, not FAILED.
            if self._abort_event.is_set():
                await self._sm.pause_task(step_id, task_id)
                return
            error_msg = f"{type(exc).__name__}: {exc}"
            await self._sm.fail_task(step_id, task_id, error=error_msg, exc=exc)
            logger.error("Task %s/%s failed: %s", step_id, task_id, error_msg)

    # ─── 输入组装 ────────────────────────────────────────────────────────────

    async def _build_inputs(self, step: "StepSpec", task_spec) -> dict[str, Any]:
        """合并静态 inputs + step 内依赖输出 + 跨 step 依赖输出。"""
        inputs: dict[str, Any] = dict(task_spec.inputs)

        # step 内任务依赖：从同 step 下上游 task 的 output.json 读取
        for dep_task_id in task_spec.depends_on:
            if storage.task_output_exists(
                self._workspace, self._pipeline_id, self._run_id, step.id, dep_task_id
            ):
                inputs[dep_task_id] = storage.load_task_output(
                    self._workspace, self._pipeline_id, self._run_id, step.id, dep_task_id
                )

        # 跨 step 依赖：从上游 step 的叶子任务（或 manual_data）聚合输出
        for dep_step_id in task_spec.depends_on_steps:
            step_outputs = self._collect_step_outputs(dep_step_id)
            if step_outputs:
                inputs[dep_step_id] = step_outputs

        return inputs

    def _collect_step_outputs(self, step_id: str) -> dict[str, Any]:
        """聚合上游 step 的输出。

        - skip=true 且有 output: 的 step：从 output: 路径读取预置数据。
        - skip=true 无 output: 的 step：从 manual_data/<step_id>/output.json 读取。
        - 普通 step：收集叶子 task 的 output.json，以 task_id 为键聚合。
        """
        dep_step_spec = self._get_step(step_id)

        if dep_step_spec.skip:
            if dep_step_spec.output:
                path = storage.resolve_output_path(self._workspace, dep_step_spec.output)
                try:
                    return storage.read_json(path)  # type: ignore[arg-type]
                except (OSError, ValueError):
                    return {}
            try:
                return storage.load_manual_data(self._workspace, step_id)
            except PipelineError:
                return {}

        import networkx as nx
        g = build_task_graph(dep_step_spec)
        # 叶子节点：无出边的节点（即无下游依赖的 task）
        leaf_ids = [n for n in g.nodes if g.out_degree(n) == 0]
        if not leaf_ids:
            leaf_ids = [t.id for t in dep_step_spec.tasks]

        result: dict[str, Any] = {}
        for tid in leaf_ids:
            if storage.task_output_exists(
                self._workspace, self._pipeline_id, self._run_id, step_id, tid
            ):
                result[tid] = storage.load_task_output(
                    self._workspace, self._pipeline_id, self._run_id, step_id, tid
                )
        return result

    def _get_step(self, step_id: str) -> "StepSpec":
        """按 step_id 查找 StepSpec，不存在则抛出 PipelineError。"""
        for s in self._spec.steps:
            if s.id == step_id:
                return s
        raise PipelineError(
            f"step '{step_id}' not found in pipeline",
            pipeline_id=self._pipeline_id,
        )
