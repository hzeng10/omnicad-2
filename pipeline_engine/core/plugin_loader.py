from __future__ import annotations

import importlib
from typing import Any

from pipeline_engine.core.base_task import BaseTask
from pipeline_engine.core.errors import PipelineError


def load_task_class(dotted_path: str) -> type[BaseTask]:
    """Dynamically load a BaseTask subclass from a dotted module path.

    Args:
        dotted_path: e.g. "mypackage.tasks.MyTask"

    Returns:
        The task class (not an instance).

    Raises:
        PipelineError if the module can't be imported, the class doesn't exist,
        or the class is not a BaseTask subclass.
    """
    if "." not in dotted_path:
        raise PipelineError(
            f"plugin path '{dotted_path}' must be 'module.ClassName'"
        )

    module_path, class_name = dotted_path.rsplit(".", 1)

    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise PipelineError(
            f"cannot import plugin module '{module_path}': {exc}"
        ) from exc

    cls = getattr(module, class_name, None)
    if cls is None:
        raise PipelineError(
            f"class '{class_name}' not found in module '{module_path}'"
        )

    if not (isinstance(cls, type) and issubclass(cls, BaseTask)):
        raise PipelineError(
            f"'{dotted_path}' is not a subclass of BaseTask"
        )

    return cls


def instantiate_task(
    dotted_path: str,
    task_id: str,
    config: dict[str, Any],
) -> BaseTask:
    """Load and instantiate a task plugin."""
    cls = load_task_class(dotted_path)
    return cls(task_id=task_id, config=config)
