"""存储层：所有磁盘 I/O 的统一入口。

设计原则
--------
- **原子写入**：``atomic_write_json`` 先写 ``.tmp`` 再 ``os.replace``，
  进程在写盘过程中崩溃不会产生损坏的 JSON 文件。
- **目录隔离**：每个 run 对应 ``.pipeline_runs/<pipeline_id>/<run_id>/`` 目录；
  每个 task 在其下建立 ``<step_id>/<task_id>/`` 子目录。
- **注册表**：``registry.json`` 存储所有已加载 pipeline 的 YAML 路径和元信息，
  供 CLI 一次性子命令在新进程中重建 RunManager 状态。
- **手动数据**：skip=true 的 step 从 ``manual_data/<step_id>/output.json`` 读取
  预置输出；该文件必须在 skip step 执行前存在，否则报 PipelineError。

不做什么
--------
- 本层不持有任何状态，所有函数均为无状态纯函数（路径计算 + 文件操作）。
- 不直接修改 StateManager 或 RunContext，只操作文件系统。
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from pipeline_engine.core.errors import PipelineError
from pipeline_engine.models.runtime_state import PipelineRunState


# 所有 run 数据存储在 workspace 下的此子目录
RUNS_DIR = ".pipeline_runs"


# ─── 路径计算 ──────────────────────────────────────────────────────────────────

def get_runs_root(workspace: str | Path) -> Path:
    """返回所有 run 的根目录：``<workspace>/.pipeline_runs``。"""
    return Path(workspace) / RUNS_DIR


def get_run_dir(workspace: str | Path, pipeline_id: str, run_id: str) -> Path:
    """返回指定 run 的目录。"""
    return get_runs_root(workspace) / pipeline_id / run_id


def get_task_dir(
    workspace: str | Path, pipeline_id: str, run_id: str, step_id: str, task_id: str
) -> Path:
    """返回指定 task 的工作目录。"""
    return get_run_dir(workspace, pipeline_id, run_id) / step_id / task_id


# ─── 目录初始化 ────────────────────────────────────────────────────────────────

def init_run_dir(workspace: str | Path, pipeline_id: str, run_id: str) -> Path:
    """创建并返回 run 目录（已存在则静默）。"""
    run_dir = get_run_dir(workspace, pipeline_id, run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def init_task_dir(
    workspace: str | Path, pipeline_id: str, run_id: str, step_id: str, task_id: str
) -> Path:
    """创建并返回 task 目录（已存在则静默）。"""
    task_dir = get_task_dir(workspace, pipeline_id, run_id, step_id, task_id)
    task_dir.mkdir(parents=True, exist_ok=True)
    return task_dir


# ─── 原子 JSON I/O ─────────────────────────────────────────────────────────────

def atomic_write_json(path: str | Path, obj: Any) -> None:
    """将 obj 以 JSON 格式原子写入 path（先写 .tmp 再 os.replace）。

    保证进程崩溃时不会产生半写的损坏文件。
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(path) + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")
    os.replace(tmp, path)


def read_json(path: str | Path) -> Any:
    """读取并解析 JSON 文件。"""
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_json_safe(path: str | Path) -> Any | None:
    """读取并解析 JSON 文件；文件不存在时返回 None（不抛异常）。"""
    p = Path(path)
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


# ─── 状态持久化 ────────────────────────────────────────────────────────────────

def persist_state(run_state: PipelineRunState) -> None:
    """将 run 状态原子写入 state.json（由 StateManager._persist 调用）。"""
    run_dir = Path(run_state.workspace)
    state_path = run_dir / "state.json"
    atomic_write_json(state_path, run_state.model_dump(mode="json"))


def load_state(workspace: str | Path, pipeline_id: str, run_id: str) -> PipelineRunState:
    """从磁盘加载并反序列化 run 的状态快照。

    Raises
    ------
    PipelineError
        state.json 不存在时（通常表示 run 从未持久化过）。
    """
    state_path = get_run_dir(workspace, pipeline_id, run_id) / "state.json"
    if not state_path.exists():
        raise PipelineError(
            f"run '{run_id}' 的 state.json 不存在",
            pipeline_id=pipeline_id,
        )
    return PipelineRunState.model_validate(read_json(state_path))


# ─── 手动数据（skip=true 步骤）────────────────────────────────────────────────

def load_manual_data(workspace: str | Path, step_id: str) -> dict[str, Any]:
    """加载 skip=true 步骤的预置输出数据。

    文件路径：``<workspace>/manual_data/<step_id>/output.json``。

    Raises
    ------
    PipelineError
        文件不存在、非合法 JSON 或内容不是对象时。
    """
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


# ─── fix --output 支持 ─────────────────────────────────────────────────────────

def fix_output(
    workspace: str | Path,
    pipeline_id: str,
    run_id: str,
    step_id: str,
    task_id: str,
    src_path: str | Path,
) -> Path:
    """将 src_path 的 JSON 内容原子写入 task 的 output.json。

    供 ``RunManager.fix(--output)`` 调用；写盘前由调用方完成 OutputModel 校验。

    Returns
    -------
    Path
        写入成功后的 output.json 路径。
    """
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


# ─── task output 访问 ──────────────────────────────────────────────────────────

def get_run_log_path(workspace: str | Path, pipeline_id: str, run_id: str) -> Path:
    """返回指定 run 的统一日志文件路径：<run_dir>/run.log。"""
    return get_run_dir(workspace, pipeline_id, run_id) / "run.log"


def get_task_output_path(
    workspace: str | Path, pipeline_id: str, run_id: str, step_id: str, task_id: str
) -> Path:
    """返回 task output.json 的完整路径（不检查是否存在）。"""
    return get_task_dir(workspace, pipeline_id, run_id, step_id, task_id) / "output.json"


def task_output_exists(
    workspace: str | Path, pipeline_id: str, run_id: str, step_id: str, task_id: str
) -> bool:
    """检查 task output.json 是否存在（依赖就绪判定的唯一依据）。"""
    return get_task_output_path(workspace, pipeline_id, run_id, step_id, task_id).exists()


def load_task_output(
    workspace: str | Path, pipeline_id: str, run_id: str, step_id: str, task_id: str
) -> dict[str, Any]:
    """加载并返回 task 的 output.json 内容。

    Raises
    ------
    PipelineError
        output.json 不存在时。
    """
    path = get_task_output_path(workspace, pipeline_id, run_id, step_id, task_id)
    if not path.exists():
        raise PipelineError(
            f"task '{task_id}' 的 output.json 不存在",
            step_id=step_id,
            task_id=task_id,
        )
    return read_json(path)


# ─── output 路径解析 ───────────────────────────────────────────────────────────

def resolve_output_path(workspace: str | Path, output: str | None) -> Path | None:
    """将 YAML 中 output 字符串解析为绝对路径;None → None。

    相对路径相对 workspace 解析;绝对路径原样使用。
    """
    if not output:
        return None
    p = Path(output)
    return p if p.is_absolute() else Path(workspace) / p


# ─── 注册表 ────────────────────────────────────────────────────────────────────

def registry_path(workspace: str | Path) -> Path:
    """返回 pipeline 注册表文件路径：``<workspace>/.pipeline_runs/registry.json``。"""
    return get_runs_root(workspace) / "registry.json"


def load_registry(workspace: str | Path) -> dict[str, Any]:
    """加载 pipeline 注册表，不存在则返回空字典。"""
    p = registry_path(workspace)
    if not p.exists():
        return {}
    return read_json(p)


def save_registry(workspace: str | Path, registry: dict[str, Any]) -> None:
    """将 pipeline 注册表原子写入磁盘。"""
    p = registry_path(workspace)
    p.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(p, registry)
