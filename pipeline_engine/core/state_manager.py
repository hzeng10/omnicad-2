"""状态管理器：单次 pipeline run 的运行时状态的唯一可信来源（Single Source of Truth）。

设计原则
--------
- **线程安全**：所有读写操作均通过 ``asyncio.Lock`` 序列化，REPL 读取与后台任务写入不会发生竞态。
- **原子持久化**：每次状态变更后通过 ``storage.persist_state`` 写盘（write-tmp + os.replace），
  进程崩溃后可从 ``state.json`` 完整恢复。
- **锁外 I/O（H5）**：磁盘写入在 ``_lock`` 外进行（通过独立的 ``_persist_lock`` 序列化），
  避免 50+ 并行 task 的状态切换被磁盘 latency 串行化。
  ``_notify`` 仍在 ``_lock`` 内执行，保证订阅者看到的事件与内存状态一致。
  ``asyncio.Lock`` 是 FIFO，因此两把锁的获取顺序完全一致，写入顺序与突变顺序相同，
  不会出现旧快照覆盖新快照的 ABA 问题。
- **隔离性**：每个 run 持有独立的 StateManager 实例，多 Pipeline 并行运行时互不干扰。
- **状态机守卫**：``finish_task``、``fail_task``、``update_progress`` 内置非法迁移检查，
  防止竞态（如任务已被暂停后线程仍试图写入 SUCCESS）导致状态混乱。

合法的任务状态转换
------------------
::

    NEW     → RUNNING → SUCCESS
                      → FAILED
                      → PAUSED   (abort_event 触发时)
    FAILED  → NEW      (reset_for_resume)
    PAUSED  → NEW      (reset_for_resume --include-paused)
    任意    → FIXED    (fix --output 后调用 recover_task)
"""
from __future__ import annotations

import asyncio
import logging
import traceback
from datetime import datetime, timezone

_logger = logging.getLogger(__name__)

from pipeline_engine.core import storage
from pipeline_engine.core.errors import PipelineError
from pipeline_engine.models.runtime_state import (
    PipelineRunState,
    Status,
    StepState,
    TaskState,
)


def _now() -> datetime:
    """返回当前 UTC 时间（带时区信息）。"""
    return datetime.now(tz=timezone.utc)


class StateManager:
    """单次 pipeline run 的运行时状态管理器。

    外部代码应通过本类的公共 async 方法读写状态，不应直接访问 ``_state`` / ``_lock``
    等私有属性（RunManager 内部有两处遗留的直接访问，已在 C1 修复计划中标记）。
    """

    def __init__(self, run_state: PipelineRunState) -> None:
        self._state = run_state
        self._lock = asyncio.Lock()
        # H5: separate lock serialises disk writes without blocking state mutations.
        # Acquired immediately after releasing _lock (no await between them), so
        # asyncio.Lock's FIFO guarantee preserves write order == mutation order.
        self._persist_lock = asyncio.Lock()
        # SSE/事件订阅者列表；每个订阅者持有一个有界 Queue（容量 256）。
        # _notify 用 put_nowait — 不阻塞写者；队列满时丢弃（慢消费者无影响）。
        self._subscribers: list[asyncio.Queue[dict]] = []

    # ─── 事件订阅 ─────────────────────────────────────────────────────────────

    def subscribe(self) -> "asyncio.Queue[dict]":
        """注册订阅者，返回专属 Queue；每个事件会 put_nowait 到该 Queue。"""
        q: asyncio.Queue[dict] = asyncio.Queue(maxsize=256)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: "asyncio.Queue[dict]") -> None:
        """注销订阅者，移除对应 Queue。"""
        try:
            self._subscribers.remove(q)
        except ValueError:
            pass

    def _notify(self, event: dict) -> None:
        """向所有订阅者广播事件（put_nowait；队列满时静默丢弃）。

        必须在持有 _lock 的情况下调用，以确保订阅者拿到的事件与内存状态一致。
        使用快照迭代防止 unsubscribe 并发调用时触发 RuntimeError。
        """
        for q in list(self._subscribers):  # snapshot prevents RuntimeError on concurrent unsubscribe
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                _logger.warning("event dropped for slow SSE subscriber (queue=%d)", id(q))

    # ─── 读 API ───────────────────────────────────────────────────────────────

    async def get_run_state(self) -> PipelineRunState:
        """返回当前运行状态的深拷贝，防止调用方意外修改内部状态。"""
        async with self._lock:
            return self._state.model_copy(deep=True)

    async def get_task_state(self, step_id: str, task_id: str) -> TaskState:
        """返回指定任务状态的深拷贝。"""
        async with self._lock:
            return self._task(step_id, task_id).model_copy(deep=True)

    async def get_step_state(self, step_id: str) -> StepState:
        """返回指定步骤状态的深拷贝。"""
        async with self._lock:
            return self._step(step_id).model_copy(deep=True)

    # ─── 初始化 ───────────────────────────────────────────────────────────────

    async def init_step(self, step_id: str, task_ids: list[str]) -> None:
        """初始化步骤及其任务的状态记录。

        若步骤已存在，则保留各任务的现有状态（FIXED/SKIPPED 等终态不会被重置），
        以支持 resume 场景中跳过已完成的任务。
        """
        async with self._lock:
            existing = self._state.steps.get(step_id)
            tasks = {}
            for tid in task_ids:
                if existing and tid in existing.tasks:
                    # 保留终态（SUCCESS、FIXED、SKIPPED 等），避免 resume 时重置已完成任务
                    tasks[tid] = existing.tasks[tid]
                else:
                    tasks[tid] = TaskState(id=tid)
            self._state.steps[step_id] = StepState(id=step_id, tasks=tasks)
            snap = self._state.model_copy(deep=True)
        await self._async_persist(snap)

    # ─── pipeline 级别变更 ────────────────────────────────────────────────────

    async def start_pipeline(self) -> None:
        """将 pipeline 状态置为 RUNNING 并记录开始时间。

        M3 修复：仅允许从 NEW 状态启动，防止已完成（SUCCESS / FIXED / SKIPPED）
        或已在运行的 pipeline 被意外覆盖为 RUNNING，从而保证 SUCCESS → RUNNING → FAILED
        的非法迁移链不会发生。resume() 在调用本方法前会先调用 reset_pipeline_status(NEW)。
        """
        async with self._lock:
            if self._state.status != Status.NEW:
                raise PipelineError(
                    f"cannot start pipeline '{self._state.pipeline_id}': "
                    f"current status is '{self._state.status.value}', expected 'new'. "
                    "Call reset_pipeline_status(NEW) before starting.",
                    pipeline_id=self._state.pipeline_id,
                )
            self._state.status = Status.RUNNING
            self._state.started_at = _now()
            self._notify({"type": "pipeline_update", "status": "running"})
            snap = self._state.model_copy(deep=True)
        await self._async_persist(snap)

    async def finish_pipeline(self, success: bool) -> None:
        """将 pipeline 状态置为 SUCCESS 或 FAILED 并记录结束时间。"""
        async with self._lock:
            self._state.status = Status.SUCCESS if success else Status.FAILED
            self._state.finished_at = _now()
            self._notify({"type": "pipeline_update", "status": self._state.status.value})
            snap = self._state.model_copy(deep=True)
        await self._async_persist(snap)

    async def reset_pipeline_status(self, status: Status) -> None:
        """重置 pipeline 级别的状态（供 RunManager.resume 调用）。

        避免 RunManager 直接操作 ``_state`` 私有属性。
        H10 修复：reset 为 NEW/RUNNING 时同时清零 finished_at 和 started_at，
        避免 UI 显示"状态=RUNNING 但 finished_at/started_at=过去时间"的错配。
        start_pipeline() 会在调度器启动时写入新的 started_at。
        """
        async with self._lock:
            self._state.status = status
            if status in (Status.NEW, Status.RUNNING):
                self._state.finished_at = None
                self._state.started_at = None
            snap = self._state.model_copy(deep=True)
        await self._async_persist(snap)

    # ─── step 级别变更 ────────────────────────────────────────────────────────

    async def start_step(self, step_id: str) -> None:
        """将步骤状态置为 RUNNING 并记录开始时间。

        M3 修复：仅允许从 NEW 状态启动，与 start_pipeline 保持一致。
        init_step 始终为每个 step 创建全新的 StepState（NEW），正常流程下
        本方法不会收到非 NEW 的 step，此处是防御性守卫。
        """
        async with self._lock:
            step = self._step(step_id)
            if step.status != Status.NEW:
                raise PipelineError(
                    f"cannot start step '{step_id}': "
                    f"current status is '{step.status.value}', expected 'new'.",
                    pipeline_id=self._state.pipeline_id,
                    step_id=step_id,
                )
            step.status = Status.RUNNING
            step.started_at = _now()
            snap = self._state.model_copy(deep=True)
        await self._async_persist(snap)

    async def finish_step(self, step_id: str, success: bool) -> None:
        """将步骤状态置为 SUCCESS 或 FAILED 并记录结束时间。"""
        async with self._lock:
            step = self._step(step_id)
            step.status = Status.SUCCESS if success else Status.FAILED
            step.finished_at = _now()
            snap = self._state.model_copy(deep=True)
        await self._async_persist(snap)

    async def skip_step(self, step_id: str) -> None:
        """将步骤状态置为 SKIPPED（skip=true 时由调度器调用）。"""
        async with self._lock:
            step = self._step(step_id)
            step.status = Status.SKIPPED
            step.started_at = _now()
            step.finished_at = _now()
            snap = self._state.model_copy(deep=True)
        await self._async_persist(snap)

    # ─── task 级别变更 ────────────────────────────────────────────────────────

    async def start_task(self, step_id: str, task_id: str) -> None:
        """将任务状态置为 RUNNING 并记录开始时间。"""
        async with self._lock:
            task = self._task(step_id, task_id)
            task.status = Status.RUNNING
            task.started_at = _now()
            self._notify({"type": "task_update", "step_id": step_id, "task_id": task_id,
                          "status": "running", "progress": task.progress})
            snap = self._state.model_copy(deep=True)
        await self._async_persist(snap)

    async def finish_task(
        self,
        step_id: str,
        task_id: str,
        *,
        input_path: str | None = None,
        output_path: str | None = None,
        log_path: str | None = None,
    ) -> None:
        """将任务状态置为 SUCCESS 并更新文件路径信息。

        守卫：仅当任务处于 RUNNING 时才执行迁移。
        若任务已被 pause 或已处于终态（如 PAUSED / FIXED），则静默忽略，
        防止线程池任务完成时覆盖已暂停的状态（B2/B5 修复）。
        """
        snap = None
        async with self._lock:
            task = self._task(step_id, task_id)
            if task.status != Status.RUNNING:
                # 任务已不处于 RUNNING（可能被暂停或已通过 fix 置为 RECOVERED），丢弃结果
                return
            task.status = Status.SUCCESS
            task.progress = 100
            task.finished_at = _now()
            if input_path:
                task.input_path = input_path
            if output_path:
                task.output_path = output_path
            if log_path:
                task.log_path = log_path
            self._notify({"type": "task_update", "step_id": step_id, "task_id": task_id,
                          "status": "success", "progress": 100})
            snap = self._state.model_copy(deep=True)
        await self._async_persist(snap)

    async def fail_task(
        self,
        step_id: str,
        task_id: str,
        error: str,
        exc: BaseException | None = None,
    ) -> None:
        """将任务状态置为 FAILED 并记录错误信息及堆栈。

        守卫：仅当任务处于 RUNNING 或 NEW 时才执行迁移，
        避免覆盖已处于终态（SUCCESS / FIXED / SKIPPED）或 PAUSED 的任务（B5 修复）。
        """
        snap = None
        async with self._lock:
            task = self._task(step_id, task_id)
            if task.status not in (Status.RUNNING, Status.NEW):
                # 终态或 PAUSED 任务不可被 fail 覆盖
                return
            task.status = Status.FAILED
            task.finished_at = _now()
            task.error = error
            if exc is not None:
                task.stack_trace = traceback.format_exc()
            self._notify({"type": "task_update", "step_id": step_id, "task_id": task_id,
                          "status": "failed", "progress": task.progress, "error": error})
            snap = self._state.model_copy(deep=True)
        await self._async_persist(snap)

    async def pause_task(self, step_id: str, task_id: str) -> None:
        """将 RUNNING 或 NEW 的任务置为 PAUSED（abort_event 触发时使用）。"""
        snap = None
        async with self._lock:
            task = self._task(step_id, task_id)
            if task.status in (Status.RUNNING, Status.NEW):
                task.status = Status.PAUSED
                self._notify({"type": "task_update", "step_id": step_id, "task_id": task_id,
                              "status": "paused", "progress": task.progress})
                snap = self._state.model_copy(deep=True)
        if snap is not None:
            await self._async_persist(snap)

    async def update_progress(
        self, step_id: str, task_id: str, progress: int
    ) -> None:
        """更新任务进度（0–100）。

        守卫：仅当任务处于 RUNNING 时才更新，避免在任务暂停后仍收到线程的进度回调。
        """
        snap = None
        async with self._lock:
            task = self._task(step_id, task_id)
            if task.status != Status.RUNNING:
                return
            task.progress = max(0, min(100, progress))
            self._notify({"type": "task_update", "step_id": step_id, "task_id": task_id,
                          "status": "running", "progress": task.progress})
            snap = self._state.model_copy(deep=True)
        await self._async_persist(snap)

    async def recover_task(
        self,
        step_id: str,
        task_id: str,
        output_path: str,
        fixed_by: str,
    ) -> None:
        """将任务标记为 FIXED（fix --output 成功后调用）。

        ``fixed_by`` 记录补齐操作的审计信息（如 "fix@<timestamp>"）。
        FIXED 状态的任务在 resume 时会被 already_done 集合跳过，
        不会重新执行，但其 output.json 可供下游正常消费。

        H7 修复：仅允许对非 RUNNING 任务执行 fix，防止对 RUNNING 任务调用导致状态错乱。
        """
        async with self._lock:
            task = self._task(step_id, task_id)
            if task.status == Status.RUNNING:
                raise PipelineError(
                    f"task '{task_id}' is RUNNING; stop the run before applying fix --output",
                    pipeline_id=self._state.pipeline_id,
                    step_id=step_id,
                    task_id=task_id,
                )
            task.status = Status.FIXED
            task.output_path = output_path
            task.fixed_by = fixed_by
            task.finished_at = _now()
            task.error = None
            task.stack_trace = None
            snap = self._state.model_copy(deep=True)
        await self._async_persist(snap)

    async def replace_task_input(self, step_id: str, task_id: str) -> None:
        """将任务重置为 NEW，供 fix --input 后调用（C1 修复：替代直接访问 _state）。

        fix --input 操作将新的 input.json 写入磁盘后，需要把任务状态
        复位为 NEW 以便 resume 重新调度。

        H7 修复：仅允许对非 RUNNING 任务执行 fix，防止对 RUNNING 任务调用导致状态错乱。
        """
        async with self._lock:
            task = self._task(step_id, task_id)
            if task.status == Status.RUNNING:
                raise PipelineError(
                    f"task '{task_id}' is RUNNING; stop the run before applying fix --input",
                    pipeline_id=self._state.pipeline_id,
                    step_id=step_id,
                    task_id=task_id,
                )
            task.status = Status.NEW
            task.error = None
            task.stack_trace = None
            snap = self._state.model_copy(deep=True)
        await self._async_persist(snap)

    async def reset_for_resume(
        self, step_id: str, task_id: str, include_paused: bool = False
    ) -> bool:
        """将 FAILED（或可选 PAUSED）的任务复位为 NEW 以供重调度。

        Returns
        -------
        bool
            若任务被复位则为 True，否则为 False。
        """
        snap = None
        async with self._lock:
            task = self._task(step_id, task_id)
            eligible = {Status.FAILED}
            if include_paused:
                eligible.add(Status.PAUSED)
            if task.status in eligible:
                task.status = Status.NEW
                task.error = None
                task.stack_trace = None
                task.started_at = None
                task.finished_at = None
                task.progress = 0
                snap = self._state.model_copy(deep=True)
        if snap is not None:
            await self._async_persist(snap)
            return True
        return False

    def demote_orphans_sync(self) -> None:
        """将所有残留的 RUNNING 状态复位为 FAILED（进程恢复时调用，无需加锁）。

        ``restore_runs_from_disk`` 在注册 RunContext 之前调用本方法，
        此时尚无并发访问，故不使用 asyncio.Lock 而直接操作内部状态。
        此处保留同步 _persist() 调用，因为该方法在没有事件循环的上下文中运行。

        效果：进程崩溃/被杀时遗留的 RUNNING task / step / pipeline
        在下次 resume 时会被正确识别为 FAILED 并重新调度。
        """
        changed = False
        for step in self._state.steps.values():
            for task in step.tasks.values():
                if task.status == Status.RUNNING:
                    task.status = Status.FAILED
                    task.error = "interrupted: process exited before completion"
                    task.finished_at = _now()
                    changed = True
            if step.status == Status.RUNNING:
                step.status = Status.FAILED
                step.finished_at = _now()
                changed = True
        if self._state.status == Status.RUNNING:
            self._state.status = Status.FAILED
            self._state.finished_at = _now()
            changed = True
        if changed:
            self._persist()

    # ─── 内部工具方法 ─────────────────────────────────────────────────────────

    def _step(self, step_id: str) -> StepState:
        """按 step_id 查找步骤状态，不存在则抛出 PipelineError。"""
        try:
            return self._state.steps[step_id]
        except KeyError:
            raise PipelineError(
                f"step '{step_id}' not found in run state",
                pipeline_id=self._state.pipeline_id,
                step_id=step_id,
            )

    def _task(self, step_id: str, task_id: str) -> TaskState:
        """按 step_id + task_id 查找任务状态，不存在则抛出 PipelineError。"""
        step = self._step(step_id)
        try:
            return step.tasks[task_id]
        except KeyError:
            raise PipelineError(
                f"task '{task_id}' not found in step '{step_id}'",
                pipeline_id=self._state.pipeline_id,
                step_id=step_id,
                task_id=task_id,
            )

    def _persist(self) -> None:
        """同步写盘（仅供 demote_orphans_sync 使用；所有 async 方法改用 _async_persist）。"""
        storage.persist_state(self._state)

    async def _async_persist(self, snapshot: PipelineRunState) -> None:
        """将快照异步写盘，在 _persist_lock 内序列化以防止乱序写入（H5 修复）。

        通过 asyncio.to_thread 将 I/O 转到线程池，释放事件循环处理其他任务。
        _persist_lock 在 _lock 释放后立即获取（中间无 await），asyncio.Lock 的
        FIFO 保证写入顺序与突变顺序完全一致，不会出现旧快照覆盖新快照的情况。
        """
        async with self._persist_lock:
            await asyncio.to_thread(storage.persist_state, snapshot)
