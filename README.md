# Pipeline DAG Engine

A Python 3.10+ CLI tool for orchestrating DAG-based workflows defined in YAML. The engine is fully decoupled from business logic — it schedules tasks, routes data between them, and manages state. Concrete tasks are loaded as plugins at runtime.

## Features

- **YAML-defined pipelines** — declare steps, tasks, dependencies, and parallelism limits in a single file; Pydantic v2 validates the schema on load
- **DAG scheduling** — NetworkX detects cycles and resolves topological order; tasks with no dependencies within a step run in parallel automatically
- **Async + thread pool** — I/O-bound tasks use `async def execute()`; CPU-bound tasks use `def run_sync()` and are offloaded to a thread pool via `asyncio.to_thread`
- **Atomic state persistence** — every task transition is written to disk (`os.replace`) before the in-memory state updates, so a crash mid-run leaves a recoverable snapshot
- **Failure recovery** — `fix --output` supplies a replacement `output.json` for a failed task; downstream tasks proceed as normal because dependency readiness is determined by file existence, not status fields
- **Abort / resume** — `stop` signals a graceful abort via `asyncio.Event`; `resume` re-schedules only failed (or optionally paused) tasks, preserving completed work
- **Multi-pipeline concurrency** — multiple pipelines run in the same process, each with an isolated `RunContext`; a shared `asyncio.Semaphore` caps total thread-pool usage across all runs
- **Interactive REPL** — `prompt_toolkit`-based REPL runs alongside the scheduler in the same event loop; `status --watch` live-refreshes a Rich table while tasks are running
- **91% test coverage** — 152 tests across unit, integration, and end-to-end layers

## Quick Start

```bash
pip install -e .

# Validate a pipeline YAML
pipeline_cli lint examples/cad_pipeline/pipeline.yaml

# Run and wait for completion
pipeline_cli run cad_cost_estimation \
  --workspace /tmp/demo \
  --wait

# Interactive REPL
pipeline_cli --workspace /tmp/demo
```

## REPL Commands

```
load <path>                          Register a pipeline YAML
list [--runs]                        List pipelines or active runs
run <pipeline_id> [--step S] [--task T]   Start a run (non-blocking)
status <ref> [--watch] [--all]       Show run state; --watch live-refreshes
inspect <ref> --step S --task T      Show input / output / log / stack trace
stop <ref>                           Abort a run gracefully
resume <ref> [--include-paused]      Re-schedule failed (+ optionally paused) tasks
fix <ref> --task S/T --output PATH   Supply recovered output.json → RECOVERED
fix <ref> --task S/T --input PATH    Replace input.json → reset to PENDING
help / exit
```

## Pipeline YAML Schema

```yaml
version: "1.0"
pipeline:
  id: my_pipeline
  name: "My Pipeline"
  max_parallelism: 4        # process-level thread cap

steps:
  - id: step_one
    tasks:
      - id: task_a
        plugin: mypackage.tasks.TaskA   # dotted path to a BaseTask subclass
        config: { key: value }
        inputs: { static_param: 42 }

      - id: task_b
        plugin: mypackage.tasks.TaskB
        depends_on: [task_a]            # within-step dependency

  - id: step_two
    depends_on_steps: [step_one]        # cross-step dependency
    tasks:
      - id: task_c
        plugin: mypackage.tasks.TaskC
        depends_on_steps: [step_one]    # injects step_one leaf outputs as inputs
```

## Writing a Task

```python
from pipeline_engine.core.base_task import BaseTask

class MyTask(BaseTask):
    # Optional: enforce I/O contracts with Pydantic models
    # InputModel  = MyInputModel
    # OutputModel = MyOutputModel

    async def execute(self, inputs: dict, progress) -> dict:
        await progress(50)
        result = do_work(inputs)
        await progress(100)
        return {"result": result}

    # CPU-bound alternative — engine calls asyncio.to_thread automatically
    def run_sync(self, inputs: dict, progress) -> dict:
        progress(100)
        return {"result": do_heavy_work(inputs)}
```

## Failure Recovery Workflow

```
# 1. A task fails mid-run
pipeline> status my_pipeline
#  recognize / rec_cable   failed   RuntimeError: ...

# 2. Inspect the error
pipeline> inspect <run_id> --step recognize --task rec_cable

# 3. Supply a corrected output
pipeline> fix <run_id> --task recognize/rec_cable --output ./recovered.json
#  rec_cable → RECOVERED

# 4. Resume — completed tasks are skipped; recovered tasks are not re-run
pipeline> resume <run_id>
pipeline> status <run_id>
#  pipeline   success
```

## CAD Pipeline Example

`examples/cad_pipeline/` contains a full mock pipeline (7 tasks, 4 steps) that demonstrates every engine feature: serial steps, within-step parallelism, long-running tasks with live progress, `run_sync` vs `async execute`, and the fix/resume recovery path.

```bash
# Fast mode for quick exploration (sleeps scaled to 10%)
PIPELINE_DEMO_FAST=1 pipeline_cli --workspace /tmp/cad_demo
pipeline> load examples/cad_pipeline/pipeline.yaml
pipeline> run cad_cost_estimation
pipeline> status cad_cost_estimation --watch
```

## Tech Stack

| Concern | Library |
|---|---|
| Async scheduling | `asyncio` (stdlib) |
| DAG validation | `NetworkX >= 3.0` |
| Schema validation | `Pydantic >= 2.5` |
| YAML parsing | `PyYAML >= 6.0` |
| Terminal UI | `Rich >= 13.0` |
| Interactive REPL | `prompt_toolkit >= 3.0` |
| CLI | `Typer >= 0.12` |

## Running Tests

```bash
pytest                                          # all 152 tests
pytest tests/unit/                             # unit tests only
pytest tests/e2e/ -v                           # end-to-end (CAD example)
PIPELINE_DEMO_FAST=1 pytest --cov=pipeline_engine --cov-fail-under=90
```
