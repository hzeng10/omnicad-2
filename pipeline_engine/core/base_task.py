"""BaseTask：所有用户自定义任务必须继承的抽象基类。

设计原则
--------
- **异步优先**：重写 ``async def execute(...)`` 处理 IO 密集型任务。
- **同步兼容**：重写 ``def run_sync(...)`` 处理 CPU 密集型或遗留同步代码；
  引擎自动通过 ``asyncio.to_thread`` 在线程池中执行。
- **I/O 契约**：可选声明 ``InputModel`` / ``OutputModel``（Pydantic 模型类），
  引擎在执行前后自动校验数据结构。
- **进度回调**：``execute`` 接收 ``progress: ProgressCallback`` 参数，
  调用 ``await progress(value)`` 推送 0–100 的进度值到状态管理器。

C3 修复：``_SyncProgressAdapter`` 改为 fire-and-forget 模式——
仅将协程提交到事件循环，不阻塞 worker 线程等待完成。
进度更新为非关键路径，轻微延迟可接受，不应拖慢 CPU 密集型任务。
"""
from __future__ import annotations

import asyncio
from abc import ABC
from typing import Any, Awaitable, Callable

from pipeline_engine.core.errors import PipelineError

# 异步进度回调类型：接受 int（0–100），返回 Awaitable
ProgressCallback = Callable[[int], Awaitable[None]]


class _SyncProgressAdapter:
    """同步进度适配器：供线程池 worker 从同步代码推送进度。

    C3 修复：采用 fire-and-forget 模式（call_soon_threadsafe + ensure_future），
    不再 future.result() 阻塞 worker 线程，避免高进度回调频率拖慢 CPU 密集型任务。
    """

    def __init__(self, async_cb: ProgressCallback, loop: asyncio.AbstractEventLoop) -> None:
        self._cb = async_cb
        self._loop = loop

    def __call__(self, value: int) -> None:
        # 将协程提交到事件循环，不等待完成（fire-and-forget）
        self._loop.call_soon_threadsafe(
            lambda: asyncio.ensure_future(self._cb(value), loop=self._loop)
        )


class BaseTask(ABC):
    """所有用户任务的抽象基类。

    子类二选一实现：
    - ``async def execute(self, inputs, progress) -> dict``：适用于 IO 密集型任务。
    - ``def run_sync(self, inputs, progress) -> dict``：适用于 CPU 密集型 / 遗留同步代码，
      引擎自动通过 ``asyncio.to_thread`` 在线程池中执行，progress 为同步适配器。

    类变量
    ------
    InputModel：可选的 Pydantic 模型类，用于输入数据校验。
    OutputModel：可选的 Pydantic 模型类，用于输出数据校验。
    """

    # 子类可赋值为 Pydantic 模型类，None 表示跳过校验
    InputModel: type | None = None
    OutputModel: type | None = None

    def __init__(self, task_id: str, config: dict[str, Any]) -> None:
        self.task_id = task_id
        self.config = config

    async def execute(self, inputs: dict[str, Any], progress: ProgressCallback) -> dict[str, Any]:
        """默认实现：若子类重写了 run_sync，则自动委托到线程池执行。"""
        if type(self).run_sync is not BaseTask.run_sync:
            loop = asyncio.get_running_loop()
            adapter = _SyncProgressAdapter(progress, loop)
            return await asyncio.to_thread(self.run_sync, inputs, adapter)
        raise NotImplementedError(
            f"{type(self).__name__} 必须实现 execute() 或 run_sync()"
        )

    def run_sync(self, inputs: dict[str, Any], progress: Any) -> dict[str, Any]:
        """CPU 密集型 / 同步任务入口，由 execute() 通过线程池调用。"""
        raise NotImplementedError

    def validate_input(self, inputs: dict[str, Any]) -> dict[str, Any]:
        """若声明了 InputModel，则对输入数据做 Pydantic 校验。"""
        if self.InputModel is None:
            return inputs
        try:
            return self.InputModel.model_validate(inputs).model_dump()
        except Exception as exc:
            raise PipelineError(
                f"input validation failed for task '{self.task_id}': {exc}",
                task_id=self.task_id,
            ) from exc

    def validate_output(self, output: dict[str, Any]) -> dict[str, Any]:
        """若声明了 OutputModel，则对输出数据做 Pydantic 校验。"""
        if self.OutputModel is None:
            return output
        try:
            return self.OutputModel.model_validate(output).model_dump()
        except Exception as exc:
            raise PipelineError(
                f"output validation failed for task '{self.task_id}': {exc}",
                task_id=self.task_id,
            ) from exc
