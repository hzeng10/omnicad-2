from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError

from pipeline_engine.core.errors import PipelineError
from pipeline_engine.models.pipeline_spec import PipelineSpec


def load_pipeline_spec(path: str | Path) -> PipelineSpec:
    """Parse a YAML file into a validated PipelineSpec.

    Raises PipelineError on missing file, invalid YAML, or schema violations.
    """
    path = Path(path)
    if not path.exists():
        raise PipelineError(f"YAML file not found: {path}")

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise PipelineError(f"Failed to parse YAML: {exc}") from exc

    if not isinstance(raw, dict):
        raise PipelineError("YAML root must be a mapping")

    try:
        return PipelineSpec.model_validate(raw)
    except ValidationError as exc:
        # Surface the first validation error with a clean message
        first = exc.errors()[0]
        loc = " -> ".join(str(p) for p in first["loc"])
        raise PipelineError(
            f"YAML schema error at '{loc}': {first['msg']}"
        ) from exc
