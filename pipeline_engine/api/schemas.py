"""Request body Pydantic models for the REST API.

Each model maps 1-to-1 to the arguments of the corresponding CLI command.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field


class LintRequest(BaseModel):
    path: str = Field(..., description="Path to the pipeline YAML file to validate.")


class LoadRequest(BaseModel):
    paths: list[str] = Field(..., min_length=1, description="YAML file paths to register.")


class StartRequest(BaseModel):
    pipeline_ids: list[str] = Field(..., min_length=1, description="Pipeline IDs to start.")
    step: Optional[str] = Field(None, description="Run only this step.")
    task: Optional[str] = Field(None, description="Run only this task (requires step).")


class ResumeRequest(BaseModel):
    include_paused: bool = Field(False, description="Also resume PAUSED tasks.")


class FixRequest(BaseModel):
    task: str = Field(..., description="Task locator: 'step_id/task_id' or 'task_id'.")
    mode: str = Field(..., pattern="^(output|input)$", description="Fix mode: 'output' or 'input'.")
    path: str = Field(..., description="Path to the replacement JSON file.")
