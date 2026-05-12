"""Utility for loading pre-supplied outputs for skipped steps."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from pipeline_engine.core import storage
from pipeline_engine.core.errors import PipelineError


def load_skip_output(workspace: str | Path, step_id: str) -> dict[str, Any]:
    """Load the manual output for a skipped step.

    Expects: <workspace>/manual_data/<step_id>/output.json
    Raises PipelineError if the file is missing or malformed.
    """
    return storage.load_manual_data(workspace, step_id)
