"""Pipeline YAML 规格模型：定义 pipeline、step、task 的配置结构。

模型层级
--------
::

    PipelineSpec
      └── pipeline: PipelineMeta     （全局元信息：id / name / 并发上限）
      └── steps: list[StepSpec]      （按数组顺序或 depends_on_steps 执行）
            └── tasks: list[TaskSpec] （step 内 DAG 任务，可并行）

校验规则
--------
- ``id`` 字段必须匹配 ``[a-z][a-z0-9_]*``（小写字母开头，仅含小写字母/数字/下划线）。
- step 内 task id 不能重复；pipeline 内 step id 不能重复。
- ``max_parallelism`` 必须 ≥ 1。
- step 至少包含 1 个 task；pipeline 至少包含 1 个 step。
"""
from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, field_validator, model_validator

# ID 合法性正则：小写字母开头，仅含小写字母/数字/下划线
ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def _check_id(v: str) -> str:
    """校验 ID 格式，不合法则抛出 ValueError。"""
    if not ID_PATTERN.match(v):
        raise ValueError(
            f"id '{v}' 必须匹配 [a-z][a-z0-9_]*（小写字母开头）"
        )
    return v


class TaskSpec(BaseModel):
    """单个任务的配置规格。

    字段说明
    --------
    id:
        task 唯一标识，在所属 step 内不能重复。
    plugin:
        任务实现类的点分路径，如 ``mypackage.tasks.ParseDXF``。
    depends_on:
        step 内的上游 task id 列表（DAG 依赖，用于控制执行顺序）。
    depends_on_steps:
        跨 step 依赖的 step id 列表；引擎会将此上提到 step 级别，
        确保该 step 在所有声明的上游 step 完成后才开始。
    config:
        传递给任务构造函数的静态配置字典（不经过数据流路由）。
    inputs:
        静态输入字典，在任务执行前注入 inputs 中。
    """

    id: str
    plugin: str
    depends_on: list[str] = []
    depends_on_steps: list[str] = []
    config: dict[str, Any] = {}
    inputs: dict[str, Any] = {}
    output: str | None = None
    output_mode: Literal["overwrite", "accumulate"] = "overwrite"

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        return _check_id(v)


class StepSpec(BaseModel):
    """单个步骤的配置规格。

    字段说明
    --------
    id:
        step 唯一标识，在 pipeline 内不能重复。
    name:
        可读名称（可选，仅用于展示）。
    skip:
        若为 True，跳过执行并从 ``manual_data/<step_id>/output.json`` 加载预置输出。
    max_parallelism:
        本 step 内最大并发 task 数；None 时使用 pipeline 级别配置。
    depends_on_steps:
        显式声明的上游 step 依赖；若为空则默认依赖数组中前一个 step。
    tasks:
        step 内的 task 列表（至少一个）。
    """

    id: str
    name: str | None = None
    skip: bool = False
    max_parallelism: int | None = None
    depends_on_steps: list[str] = []
    tasks: list[TaskSpec]
    output: str | None = None

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        return _check_id(v)

    @field_validator("max_parallelism")
    @classmethod
    def parallelism_positive(cls, v: int | None) -> int | None:
        if v is not None and v < 1:
            raise ValueError("max_parallelism 必须 >= 1")
        return v

    @field_validator("tasks")
    @classmethod
    def tasks_not_empty(cls, v: list[TaskSpec]) -> list[TaskSpec]:
        if not v:
            raise ValueError("step 至少需要包含一个 task")
        return v

    @model_validator(mode="after")
    def unique_task_ids(self) -> "StepSpec":
        """校验 step 内 task id 唯一性。"""
        seen: set[str] = set()
        for task in self.tasks:
            if task.id in seen:
                raise ValueError(f"step '{self.id}' 中存在重复的 task id '{task.id}'")
            seen.add(task.id)
        return self

    @model_validator(mode="after")
    def check_shared_output_paths(self) -> "StepSpec":
        """多个 task 共享同一 output 路径时，所有相关 task 必须声明 output_mode: accumulate。

        若两个 task 声明了相同的 output 路径但未明确使用 accumulate 模式，
        最终只有最后完成的 task 的结果会保留（静默丢失），故在加载阶段即报错。
        """
        from collections import defaultdict
        path_tasks: defaultdict[str, list[str]] = defaultdict(list)
        for task in self.tasks:
            if task.output:
                path_tasks[task.output].append(task.id)
        for path, ids in path_tasks.items():
            if len(ids) > 1:
                bad = [
                    t.id for t in self.tasks
                    if t.output == path and t.output_mode != "accumulate"
                ]
                if bad:
                    raise ValueError(
                        f"Tasks {bad} in step '{self.id}' share output path '{path}' "
                        f"but don't declare output_mode: accumulate — "
                        f"this would silently discard all but the last writer's result"
                    )
        return self


class PipelineMeta(BaseModel):
    """Pipeline 全局元信息。

    字段说明
    --------
    id:
        pipeline 唯一标识（注册表中的 key）。
    name:
        可读名称。
    type:
        pipeline 业务类型，例如 "CAD图识别及算量" / "CAD生成" / "AI数据工程"。必填。
    description:
        可选描述。
    max_parallelism:
        进程级最大并发 task 数（默认 8）；各 step 可通过 max_parallelism 覆盖。
    """

    id: str
    name: str
    type: str
    description: str | None = None
    max_parallelism: int = 8
    output: str | None = None

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        return _check_id(v)

    @field_validator("max_parallelism")
    @classmethod
    def parallelism_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("max_parallelism 必须 >= 1")
        return v


class PipelineSpec(BaseModel):
    """Pipeline YAML 的顶层模型。

    字段说明
    --------
    version:
        Schema 版本号（当前为 "1.0"）。
    pipeline:
        全局元信息（id / name / max_parallelism）。
    steps:
        步骤列表，至少包含一个 step；step id 在 pipeline 内唯一。
    """

    version: str
    pipeline: PipelineMeta
    steps: list[StepSpec]

    @field_validator("steps")
    @classmethod
    def steps_not_empty(cls, v: list[StepSpec]) -> list[StepSpec]:
        if not v:
            raise ValueError("pipeline 至少需要包含一个 step")
        return v

    @model_validator(mode="after")
    def unique_step_ids(self) -> "PipelineSpec":
        """校验 pipeline 内 step id 唯一性。"""
        seen: set[str] = set()
        for step in self.steps:
            if step.id in seen:
                raise ValueError(f"pipeline 中存在重复的 step id '{step.id}'")
            seen.add(step.id)
        return self
