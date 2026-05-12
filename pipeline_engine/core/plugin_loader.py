"""插件加载器：在运行时从点分路径动态加载 BaseTask 子类。

设计原则
--------
- **解耦**：引擎只依赖 ``BaseTask`` 接口，不直接导入任何业务代码。
- **快速失败**：模块不存在、类名不存在、非 BaseTask 子类，三种情况均立即抛出
  ``PipelineError``，错误消息精确指向失败原因。
- **B7 兼容**：调度器的 ``_dispatch_task`` 捕获所有异常（包括 ``PipelineError``），
  因此本模块抛出的错误会被自动转为 task 的 FAILED 状态，不会崩溃整条 pipeline。
"""
from __future__ import annotations

import importlib
from typing import Any

from pipeline_engine.core.base_task import BaseTask
from pipeline_engine.core.errors import PipelineError


def load_task_class(dotted_path: str) -> type[BaseTask]:
    """从点分模块路径动态加载 BaseTask 子类，返回类对象（非实例）。

    Parameters
    ----------
    dotted_path:
        格式为 ``module.path.ClassName``，例如 ``mypackage.tasks.ParseDXF``。

    Raises
    ------
    PipelineError
        - 路径格式不含 ``.``（无法拆分模块名与类名）
        - 模块无法导入（ImportError）
        - 类名在模块中不存在
        - 类不是 BaseTask 的子类
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
    """加载并实例化任务插件。

    Parameters
    ----------
    dotted_path:
        任务类的点分路径（来自 YAML ``plugin`` 字段）。
    task_id:
        任务 ID，用于构造实例和错误信息。
    config:
        任务静态配置（来自 YAML ``config`` 字段）。
    """
    cls = load_task_class(dotted_path)
    return cls(task_id=task_id, config=config)
