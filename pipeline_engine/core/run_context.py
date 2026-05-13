"""运行上下文：封装单次 pipeline run 的所有运行时组件。

每个 run 拥有独立的 RunContext 实例，确保多 pipeline 并行运行时互不干扰：
- ``scheduler``：本次 run 的调度器（与 state_manager 绑定）。
- ``state_manager``：本次 run 的状态管理器（唯一可信状态来源）。
- ``abort_event``：中止信号；resume 时替换为新的 Event 以支持再次 stop。
- ``main_task``：驱动调度器的 asyncio.Task；None 表示未启动或已完成。

A4 修复：新增 ``await_main()`` 方法，供 CLI 安全等待 main_task 完成，
避免裸 ``await ctx.main_task``（main_task 为 None 时会抛 TypeError）。
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pipeline_engine.core.run_logger import RunLogger
    from pipeline_engine.core.scheduler import AsyncScheduler
    from pipeline_engine.core.state_manager import StateManager
    from pipeline_engine.models.pipeline_spec import PipelineSpec


class RunContext:
    """单次 pipeline run 的运行时容器。

    所有字段均为同一 run 内共享的可信实例；RunManager 负责创建和管理生命周期。
    """

    def __init__(
        self,
        pipeline_spec: "PipelineSpec",
        run_id: str,
        workspace: Path,
        scheduler: "AsyncScheduler",
        state_manager: "StateManager",
        abort_event: asyncio.Event,
        run_logger: "RunLogger | None" = None,
    ) -> None:
        self.pipeline_spec = pipeline_spec
        self.run_id = run_id
        self.workspace = workspace
        self.scheduler = scheduler
        self.state_manager = state_manager
        self.abort_event = abort_event
        self.run_logger = run_logger
        # main_task 由 RunManager.start_run() / resume() 赋值；None 表示尚未启动
        self.main_task: asyncio.Task | None = None

    @property
    def pipeline_id(self) -> str:
        return self.pipeline_spec.pipeline.id

    def is_active(self) -> bool:
        """若调度器 Task 存在且未完成，则认为 run 处于活跃状态。"""
        return self.main_task is not None and not self.main_task.done()

    async def await_main(self) -> None:
        """安全等待 main_task 完成。

        A4 修复：若 main_task 为 None（restore_runs_from_disk 后的重建场景），
        则直接返回，不抛 TypeError。
        """
        if self.main_task is not None:
            await self.main_task
