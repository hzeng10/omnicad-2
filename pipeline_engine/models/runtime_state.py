"""运行时状态模型：Pipeline / Step / Task 的状态机定义。

状态机
------
::

    NEW     → RUNNING → SUCCESS
                      → FAILED
                      → PAUSED   （abort_event 触发时）
    任意    → SKIPPED  （step 级别 skip=true）
    任意    → FIXED    （fix --output 后调用 recover_task）

    FAILED  → NEW      （reset_for_resume）
    PAUSED  → NEW      （reset_for_resume --include-paused）

合法迁移由 StateManager 的守卫方法（finish_task / fail_task / update_progress）
强制执行，非法迁移静默忽略，防止并发竞争导致状态混乱。

FIXED 语义
----------
任务通过 ``fix --output`` 手动补充输出后进入 FIXED 状态：
- resume 时被 already_done 集合跳过，不会重新执行。
- 其 output.json 对下游任务完全透明，可被正常消费。
- ``fixed_by`` 字段记录补齐操作的审计信息。

向后兼容
--------
磁盘持久化的旧版 state.json 可能含有 ``"pending"`` / ``"recovered"``
字符串值（重命名前的旧格式）；各状态模型的 ``_migrate_legacy`` validator
在反序列化时自动将其映射为 ``"new"`` / ``"fixed"``，确保升级无感。
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, model_validator


class Status(str, Enum):
    """Pipeline / Step / Task 的状态枚举。"""

    NEW = "new"              # 等待调度
    RUNNING = "running"      # 正在执行
    PAUSED = "paused"        # 因 abort_event 暂停
    SUCCESS = "success"      # 成功完成
    FAILED = "failed"        # 执行失败
    SKIPPED = "skipped"      # step 级别 skip=true 跳过
    FIXED = "fixed"          # 通过 fix --output 手动恢复


# 旧枚举值到新枚举值的映射（用于 state.json 向后兼容迁移）
_LEGACY_STATUS_MAP = {
    "pending": "new",
    "recovered": "fixed",
}


def _migrate_status(data: dict[str, Any]) -> dict[str, Any]:
    """将 state.json 中的旧 status 字符串映射为新值。"""
    raw = data.get("status")
    if isinstance(raw, str) and raw in _LEGACY_STATUS_MAP:
        data["status"] = _LEGACY_STATUS_MAP[raw]
    return data


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
    fixed_by:      fix --output 操作的审计信息（如 "fix-output@<timestamp>"）。
    """

    id: str
    status: Status = Status.NEW
    progress: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    stack_trace: str | None = None
    input_path: str | None = None
    output_path: str | None = None
    log_path: str | None = None
    fixed_by: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy(cls, data: Any) -> Any:
        """将旧版 state.json 中的 pending/recovered 状态值及 recovered_by 字段迁移。"""
        if isinstance(data, dict):
            data = _migrate_status(dict(data))
            # recovered_by 字段重命名为 fixed_by
            if "recovered_by" in data and "fixed_by" not in data:
                data["fixed_by"] = data.pop("recovered_by")
        return data


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
    status: Status = Status.NEW
    tasks: dict[str, TaskState] = {}
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy(cls, data: Any) -> Any:
        """将旧版 state.json 中的 pending/recovered 状态值迁移。"""
        if isinstance(data, dict):
            data = _migrate_status(dict(data))
        return data


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
    status: Status = Status.NEW
    steps: dict[str, StepState] = {}
    started_at: datetime | None = None
    finished_at: datetime | None = None
    workspace: str

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy(cls, data: Any) -> Any:
        """将旧版 state.json 中的 pending/recovered 状态值迁移。"""
        if isinstance(data, dict):
            data = _migrate_status(dict(data))
        return data
