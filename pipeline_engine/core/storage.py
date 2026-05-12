from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

from pipeline_engine.core.errors import PipelineError
from pipeline_engine.models.runtime_state import PipelineRunState


RUNS_DIR = ".pipeline_runs"


def get_runs_root(workspace: str | Path) -> Path:
    return Path(workspace) / RUNS_DIR


def get_run_dir(workspace: str | Path, pipeline_id: str, run_id: str) -> Path:
    return get_runs_root(workspace) / pipeline_id / run_id


def get_task_dir(
    workspace: str | Path, pipeline_id: str, run_id: str, step_id: str, task_id: str
) -> Path:
    return get_run_dir(workspace, pipeline_id, run_id) / step_id / task_id


def init_run_dir(workspace: str | Path, pipeline_id: str, run_id: str) -> Path:
    run_dir = get_run_dir(workspace, pipeline_id, run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def init_task_dir(
    workspace: str | Path, pipeline_id: str, run_id: str, step_id: str, task_id: str
) -> Path:
    task_dir = get_task_dir(workspace, pipeline_id, run_id, step_id, task_id)
    task_dir.mkdir(parents=True, exist_ok=True)
    return task_dir


def atomic_write_json(path: str | Path, obj: Any) -> None:
    """Write obj as JSON atomically (write to .tmp then os.replace)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(path) + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")
    os.replace(tmp, path)


def read_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def persist_state(run_state: PipelineRunState) -> None:
    """Atomically write the run's state.json snapshot."""
    run_dir = Path(run_state.workspace)
    state_path = run_dir / "state.json"
    atomic_write_json(state_path, run_state.model_dump(mode="json"))


def load_state(workspace: str | Path, pipeline_id: str, run_id: str) -> PipelineRunState:
    state_path = get_run_dir(workspace, pipeline_id, run_id) / "state.json"
    if not state_path.exists():
        raise PipelineError(
            f"state.json not found for run '{run_id}'",
            pipeline_id=pipeline_id,
        )
    return PipelineRunState.model_validate(read_json(state_path))


def load_manual_data(workspace: str | Path, step_id: str) -> dict[str, Any]:
    """Load pre-supplied output for a skipped step from manual_data/."""
    path = Path(workspace) / "manual_data" / step_id / "output.json"
    if not path.exists():
        raise PipelineError(
            f"manual_data not found for step '{step_id}': expected {path}",
            step_id=step_id,
        )
    try:
        data = read_json(path)
    except json.JSONDecodeError as exc:
        raise PipelineError(
            f"manual_data for step '{step_id}' is not valid JSON: {exc}",
            step_id=step_id,
        ) from exc
    if not isinstance(data, dict):
        raise PipelineError(
            f"manual_data for step '{step_id}' must be a JSON object",
            step_id=step_id,
        )
    return data


def fix_output(
    workspace: str | Path,
    pipeline_id: str,
    run_id: str,
    step_id: str,
    task_id: str,
    src_path: str | Path,
) -> Path:
    """Atomically copy src_path into the task's output.json."""
    src = Path(src_path)
    if not src.exists():
        raise PipelineError(f"fix source file not found: {src}")
    try:
        data = read_json(src)
    except json.JSONDecodeError as exc:
        raise PipelineError(f"fix source is not valid JSON: {exc}") from exc

    task_dir = init_task_dir(workspace, pipeline_id, run_id, step_id, task_id)
    dest = task_dir / "output.json"
    atomic_write_json(dest, data)
    return dest


def get_task_output_path(
    workspace: str | Path, pipeline_id: str, run_id: str, step_id: str, task_id: str
) -> Path:
    return get_task_dir(workspace, pipeline_id, run_id, step_id, task_id) / "output.json"


def task_output_exists(
    workspace: str | Path, pipeline_id: str, run_id: str, step_id: str, task_id: str
) -> bool:
    return get_task_output_path(workspace, pipeline_id, run_id, step_id, task_id).exists()


def load_task_output(
    workspace: str | Path, pipeline_id: str, run_id: str, step_id: str, task_id: str
) -> dict[str, Any]:
    path = get_task_output_path(workspace, pipeline_id, run_id, step_id, task_id)
    if not path.exists():
        raise PipelineError(
            f"output.json not found for task '{task_id}'",
            step_id=step_id,
            task_id=task_id,
        )
    return read_json(path)


def registry_path(workspace: str | Path) -> Path:
    return get_runs_root(workspace) / "registry.json"


def load_registry(workspace: str | Path) -> dict[str, Any]:
    p = registry_path(workspace)
    if not p.exists():
        return {}
    return read_json(p)


def save_registry(workspace: str | Path, registry: dict[str, Any]) -> None:
    p = registry_path(workspace)
    p.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(p, registry)
