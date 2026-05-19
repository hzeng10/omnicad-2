"""Request/response helpers for the REST API.

Request models map 1-to-1 to the corresponding CLI command arguments.
Response envelope helpers keep the ok/command wrapper DRY across routers.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field


# ── response envelope helpers ─────────────────────────────────────────────────

def envelope_ok(command: str, **payload: Any) -> dict[str, Any]:
    """Return a successful JSON envelope: {ok: True, command: ..., ...payload}."""
    return {"ok": True, "command": command, **payload}


def envelope_err(command: str, message: str, error_type: str, **payload: Any) -> dict[str, Any]:
    """Return an error JSON envelope: {ok: False, command: ..., error: {...}, ...payload}."""
    return {
        "ok": False,
        "command": command,
        **payload,
        "error": {"message": message, "type": error_type},
    }


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
    mode: str = Field(..., pattern="^(output|input)$", description="Fix mode: 'output' or 'input'.")
    path: str = Field(..., description="Path to the replacement JSON file.")
