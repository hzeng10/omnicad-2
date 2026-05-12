from __future__ import annotations

from typing import TYPE_CHECKING

import networkx as nx

from pipeline_engine.core.errors import PipelineError

if TYPE_CHECKING:
    from pipeline_engine.models.pipeline_spec import PipelineSpec, StepSpec


def build_task_graph(step: "StepSpec") -> nx.DiGraph:
    """Build a dependency graph of tasks within a single step."""
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
    """Build a dependency graph of steps within the pipeline.

    By default steps depend on the immediately preceding step (array order),
    unless depends_on_steps is explicitly declared (which replaces the default).
    """
    g: nx.DiGraph = nx.DiGraph()
    step_ids = [s.id for s in spec.steps]
    step_id_set = set(step_ids)

    for i, step in enumerate(spec.steps):
        g.add_node(step.id)
        if step.depends_on_steps:
            for dep in step.depends_on_steps:
                if dep not in step_id_set:
                    raise PipelineError(
                        f"step '{step.id}' depends_on_steps unknown step '{dep}'",
                        pipeline_id=spec.pipeline.id,
                    )
                g.add_edge(dep, step.id)
        elif i > 0:
            # Default: depend on previous step
            g.add_edge(step_ids[i - 1], step.id)
    return g


def validate_task_dag(step: "StepSpec") -> list[list[str]]:
    """Validate DAG and return topological generations (each layer runs in parallel).

    Raises PipelineError if a cycle is found.
    """
    g = build_task_graph(step)
    _assert_acyclic(g, context=f"step '{step.id}' tasks")
    return [list(gen) for gen in nx.topological_generations(g)]


def validate_step_dag(spec: "PipelineSpec") -> list[list[str]]:
    """Validate step DAG and return topological generations."""
    g = build_step_graph(spec)
    _assert_acyclic(g, context=f"pipeline '{spec.pipeline.id}' steps")
    return [list(gen) for gen in nx.topological_generations(g)]


def validate_pipeline(spec: "PipelineSpec") -> None:
    """Full DAG validation: steps + all task graphs."""
    validate_step_dag(spec)
    for step in spec.steps:
        validate_task_dag(step)


def _assert_acyclic(g: nx.DiGraph, context: str) -> None:
    if not nx.is_directed_acyclic_graph(g):
        try:
            cycle = nx.find_cycle(g)
            edges = " -> ".join(f"{u}->{v}" for u, v in cycle)
            raise PipelineError(f"Cycle detected in {context}: {edges}")
        except nx.NetworkXNoCycle:
            raise PipelineError(f"Cycle detected in {context}")
