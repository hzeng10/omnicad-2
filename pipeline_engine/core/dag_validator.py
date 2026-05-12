"""DAG 验证模块：校验 step 依赖图与 step 内任务依赖图，检测循环并提取拓扑排序。

设计原则
--------
- step 图：默认按数组顺序线性执行；显式 ``depends_on_steps`` 可打破默认顺序。
  任务级别声明的 ``depends_on_steps`` 会被上提（promote）到所在 step，确保调度顺序正确。
- task 图：仅处理 step 内的 ``depends_on``（step 内任务依赖）。
- 所有图必须是 DAG（有向无环图）；发现环路时给出详细的环路描述。
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import networkx as nx

from pipeline_engine.core.errors import PipelineError

if TYPE_CHECKING:
    from pipeline_engine.models.pipeline_spec import PipelineSpec, StepSpec


def build_task_graph(step: "StepSpec") -> nx.DiGraph:
    """构建 step 内的任务依赖有向图。

    节点 = task.id；边 A→B 表示 B 依赖 A（A 必须先完成）。
    若 ``depends_on`` 引用了不存在的 task_id，立即报错。
    """
    g: nx.DiGraph = nx.DiGraph()
    task_ids = {t.id for t in step.tasks}
    for task in step.tasks:
        g.add_node(task.id)
        for dep in task.depends_on:
            if dep not in task_ids:
                raise PipelineError(
                    f"task '{task.id}' depends_on unknown task '{dep}'",
                    step_id=step.id,
                )
            g.add_edge(dep, task.id)
    return g


def build_step_graph(spec: "PipelineSpec") -> nx.DiGraph:
    """构建 pipeline 级别的 step 依赖有向图。

    排序规则（按优先级）
    --------------------
    1. 若 step 或其下任何 task 声明了 ``depends_on_steps``，
       则以这些显式依赖为准，同时跳过默认的「前一 step 依赖」。
    2. 否则若该 step 不是第一个，则默认依赖数组中前一个 step（线性顺序）。

    任务级别的 ``depends_on_steps`` 会被上提到 step 级别：
    这保证即使只在 task 上声明跨 step 依赖，调度器也不会提前运行该 step。
    同时对任务级声明的目标 step 做存在性校验。
    """
    g: nx.DiGraph = nx.DiGraph()
    step_ids = [s.id for s in spec.steps]
    step_id_set = set(step_ids)

    for i, step in enumerate(spec.steps):
        g.add_node(step.id)

        # 收集所有显式依赖：step 级 + 任务级（上提后合并）
        explicit_deps: set[str] = set(step.depends_on_steps)

        for task in step.tasks:
            for dep in task.depends_on_steps:
                if dep not in step_id_set:
                    raise PipelineError(
                        f"task '{task.id}' in step '{step.id}' "
                        f"depends_on_steps unknown step '{dep}'",
                        pipeline_id=spec.pipeline.id,
                        step_id=step.id,
                    )
                explicit_deps.add(dep)

        if explicit_deps:
            # 显式依赖模式：校验目标存在并连边
            for dep in explicit_deps:
                if dep not in step_id_set:
                    raise PipelineError(
                        f"step '{step.id}' depends_on_steps unknown step '{dep}'",
                        pipeline_id=spec.pipeline.id,
                    )
                g.add_edge(dep, step.id)
        elif i > 0:
            # 默认模式：依赖上一个 step（数组顺序）
            g.add_edge(step_ids[i - 1], step.id)
    return g


def validate_task_dag(step: "StepSpec") -> list[list[str]]:
    """校验 step 内任务 DAG，返回拓扑分代（同代任务可并行）。

    若存在环路则抛出 PipelineError。
    """
    g = build_task_graph(step)
    _assert_acyclic(g, context=f"step '{step.id}' tasks")
    return [list(gen) for gen in nx.topological_generations(g)]


def validate_step_dag(spec: "PipelineSpec") -> list[list[str]]:
    """校验 pipeline step DAG，返回拓扑分代。"""
    g = build_step_graph(spec)
    _assert_acyclic(g, context=f"pipeline '{spec.pipeline.id}' steps")
    return [list(gen) for gen in nx.topological_generations(g)]


def validate_pipeline(spec: "PipelineSpec") -> None:
    """完整 DAG 校验入口：依次校验 step 图和所有 step 内的 task 图。"""
    validate_step_dag(spec)
    for step in spec.steps:
        validate_task_dag(step)


def _assert_acyclic(g: nx.DiGraph, context: str) -> None:
    """断言图为 DAG；若有环路则打印环路详情并抛出 PipelineError。"""
    if not nx.is_directed_acyclic_graph(g):
        try:
            cycle = nx.find_cycle(g)
            edges = " -> ".join(f"{u}->{v}" for u, v in cycle)
            raise PipelineError(f"Cycle detected in {context}: {edges}")
        except nx.NetworkXNoCycle:
            raise PipelineError(f"Cycle detected in {context}")
