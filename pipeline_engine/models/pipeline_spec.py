from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, field_validator, model_validator

ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def _check_id(v: str) -> str:
    if not ID_PATTERN.match(v):
        raise ValueError(
            f"id '{v}' must match [a-z][a-z0-9_]* (lowercase, start with letter)"
        )
    return v


class TaskSpec(BaseModel):
    id: str
    plugin: str  # dotted path: "module.submodule.ClassName"
    depends_on: list[str] = []
    depends_on_steps: list[str] = []
    config: dict[str, Any] = {}
    inputs: dict[str, Any] = {}

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        return _check_id(v)


class StepSpec(BaseModel):
    id: str
    name: str | None = None
    skip: bool = False
    max_parallelism: int | None = None
    depends_on_steps: list[str] = []
    tasks: list[TaskSpec]

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        return _check_id(v)

    @field_validator("tasks")
    @classmethod
    def tasks_not_empty(cls, v: list[TaskSpec]) -> list[TaskSpec]:
        if not v:
            raise ValueError("step must contain at least one task")
        return v

    @model_validator(mode="after")
    def unique_task_ids(self) -> "StepSpec":
        seen: set[str] = set()
        for task in self.tasks:
            if task.id in seen:
                raise ValueError(f"duplicate task id '{task.id}' in step '{self.id}'")
            seen.add(task.id)
        return self


class PipelineMeta(BaseModel):
    id: str
    name: str
    description: str | None = None
    max_parallelism: int = 8

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        return _check_id(v)


class PipelineSpec(BaseModel):
    version: str
    pipeline: PipelineMeta
    steps: list[StepSpec]

    @field_validator("steps")
    @classmethod
    def steps_not_empty(cls, v: list[StepSpec]) -> list[StepSpec]:
        if not v:
            raise ValueError("pipeline must contain at least one step")
        return v

    @model_validator(mode="after")
    def unique_step_ids(self) -> "PipelineSpec":
        seen: set[str] = set()
        for step in self.steps:
            if step.id in seen:
                raise ValueError(f"duplicate step id '{step.id}'")
            seen.add(step.id)
        return self
