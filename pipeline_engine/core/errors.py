"""引擎内部异常体系。

所有引擎错误统一使用 ``PipelineError``，可携带 pipeline_id / step_id / task_id
上下文信息，方便在日志和终端输出中快速定位问题来源。

使用原则
--------
- 引擎内部代码永远不应抛出裸 ``Exception``，应使用 ``PipelineError``。
- 用户任务（BaseTask 子类）的异常由调度器捕获并转换为 ``PipelineError`` 记录到
  对应 task 的 ``error`` 字段，不会向上传播导致 REPL 进程崩溃。
"""
from __future__ import annotations


class PipelineError(Exception):
    """引擎内部统一异常，携带 pipeline / step / task 上下文。

    Parameters
    ----------
    message:
        人类可读的错误描述。
    pipeline_id:
        出错所在的 pipeline ID（可选）。
    step_id:
        出错所在的 step ID（可选）。
    task_id:
        出错所在的 task ID（可选）。
    cause:
        原始异常（可选，便于链式追踪）。
    """

    def __init__(
        self,
        message: str,
        *,
        pipeline_id: str | None = None,
        step_id: str | None = None,
        task_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        self.pipeline_id = pipeline_id
        self.step_id = step_id
        self.task_id = task_id
        self.cause = cause
        # 将上下文信息拼接到消息字符串，便于直接打印
        parts = [message]
        if pipeline_id:
            parts.append(f"pipeline={pipeline_id}")
        if step_id:
            parts.append(f"step={step_id}")
        if task_id:
            parts.append(f"task={task_id}")
        super().__init__(" | ".join(parts))
