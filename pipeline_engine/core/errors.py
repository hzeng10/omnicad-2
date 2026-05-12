from __future__ import annotations


class PipelineError(Exception):
    """All engine-internal errors carry pipeline/step/task context."""

    def __init__(
        self,
        message: str,
        *,
        pipeline_id: str | None = None,
        step_id: str | None = None,
        task_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        self.pipeline_id = pipeline_id
        self.step_id = step_id
        self.task_id = task_id
        self.cause = cause
        parts = [message]
        if pipeline_id:
            parts.append(f"pipeline={pipeline_id}")
        if step_id:
            parts.append(f"step={step_id}")
        if task_id:
            parts.append(f"task={task_id}")
        super().__init__(" | ".join(parts))
