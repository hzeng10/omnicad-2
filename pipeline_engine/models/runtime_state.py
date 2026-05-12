"""运行时状态模型：Pipeline / Step / Task 的状态机定义。

状态机
------
::

    PENDING → RUNNING → SUCCESS
                      → FAILED
                      → PAUSED   （abort_event 触发时）
    任意    → SKIPPED  （step 级别 skip=true）
    任意    → RECOVERED（fix --output 后调用 recover_task）

    FAILED  → PENDING  （reset_for_resume）
    PAUSED  → PENDING  （reset_for_resume --include-paused）

合法迁移由 StateManager 的守卫方法（finish_task / fail_task / update_progress）
强制执行，非法迁移静默忽略，防止并发竞争导致状态混乱。

RECOVERED 语义
--------------
任务通过 ``fix --output`` 手动补充输出后进入 RECOVERED 状态：
- resume 时被 already_done 集合跳过，不会重新执行。
- 其 output.json 对下游任务完全透明，可被正常消费。
- ``recovered_by`` 字段记录补齐操作的审计信息。
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class Status(str, Enum):
    """Pipeline / Step / Task 的状态枚举。"""

    PENDING = "pending"       # 等待调度
    RUNNING = "running"       # 正在执行
    PAUSED = "paused"         # 因 abort_event 暂停
    SUCCESS = "success"       # 成功完成
    FAILED = "failed"         # 执行失败
    SKIPPED = "skipped"       # step 级别 skip=true 跳过
    RECOVERED = "recovered"   # 通过 fix --output 手动恢复


class TaskState(BaseModel):
    """单个 task 的运行时状态快照。

    字段说明
    --------
    id:            task 唯一标识。
    status:        当前状态（见 Status 枚举）。
    progress:      进度值，0–100。
    started_at:    开始时间（UTC）。
    finished_at:   完成时间（UTC）。
    error:         失败时的错误摘要。
    stack_trace:   失败时的完整堆栈（可选）。
    input_path:    input.json 的磁盘路径。
    output_path:   output.json 的磁盘路径。
    log_path:      log.txt 的磁盘路径（可选）。
    recovered_by:  fix --output 操作的审计信息（如 "fix-output@<timestamp>"）。
    """

    id: str
    status: Status = Status.PENDING
    progress: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    stack_trace: str | None = None
    input_path: str | None = None
    output_path: str | None = None
    log_path: str | None = None
    recovered_by: str | None = None


class StepState(BaseModel):
    """单个 step 的运行时状态快照。

    字段说明
    --------
    id:          step 唯一标识。
    status:      当前状态（见 Status 枚举）。
    tasks:       step 内所有 task 的状态字典（task_id → TaskState）。
    started_at:  开始时间（UTC）。
    finished_at: 完成时间（UTC）。
    """

    id: str
    status: Status = Status.PENDING
    tasks: dict[str, TaskState] = {}
    started_at: datetime | None = None
    finished_at: datetime | None = None


class PipelineRunState(BaseModel):
    """单次 pipeline run 的完整运行时状态。

    字段说明
    --------
    pipeline_id: 所属 pipeline 的 ID。
    run_id:      本次 run 的唯一 ID（时间戳 + 随机后缀）。
    status:      pipeline 级别的整体状态。
    steps:       所有 step 的状态字典（step_id → StepState）。
    started_at:  pipeline 开始时间（UTC）。
    finished_at: pipeline 完成时间（UTC）。
    workspace:   本次 run 的工作目录绝对路径。
    """

    pipeline_id: str
    run_id: str
    status: Status = Status.PENDING
    steps: dict[str, StepState] = {}
    started_at: datetime | None = None
    finished_at: datetime | None = None
    workspace: str
