# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Vision

A Python 3.10+ CLI tool for orchestrating DAG-based workflows defined in YAML. The engine is fully decoupled from business logic — it only schedules tasks, routes data, and manages state. Concrete tasks are loaded as plugins at runtime.

## Tech Stack

| Concern | Library | Notes |
|---|---|---|
| Async scheduling | `asyncio` | Core engine; REPL and tasks share the same event loop |
| DAG validation | `NetworkX` | Cycle detection + topological sort |
| Schema validation | `Pydantic` | YAML schema + task I/O JSON contracts |
| YAML parsing | `PyYAML` | Pipeline definition files |
| Terminal UI | `Rich` | Progress bars, status tables, colored logs |
| Interactive REPL | `prompt_toolkit` | Command completion + history |
| State persistence | JSON files | Atomic per-task writes to survive crashes |

## Development Commands

```bash
pip install -e .                                          # install with dev deps
pytest                                                    # run all tests
pytest tests/test_scheduler.py::test_parallel_tasks -v   # run a single test
pipeline_cli start path/to/pipeline.yaml                 # execute a pipeline
pipeline_cli lint path/to/pipeline.yaml                  # validate YAML schema
```

## Architecture Overview

### Three-layer hierarchy

```
Pipeline
  └── Step[]          (linear sequence; can be skipped → loads from ./manual_data/)
        └── Task[]    (DAG within a step; no-dep tasks run in parallel)
```

Demo pipelines live under `pipelines/<name>/`. Each pipeline directory contains
`pipeline.yaml`, `tasks.py`, `schemas.py`, `mock_data/`, and `README.md`.
See `pipelines/cad_drawing_pipeline/README.md` for the full interface reference.

Instance ID format: `<pipeline_id>_yyyyMMdd-hhmmss_<4digit>` (UTC, random suffix).

### State machine

Every Pipeline, Step, and Task tracks one of these statuses:

```
New → Running → Success
              → Failed
              → Paused (user abort)
     Skipped  (step-level only; requires manual_data pre-check)
     Fixed    (task manually recovered via fix --output)
```

State transitions must be atomic: write task result to disk **before** marking it `Success`.

### Key components (to be implemented under `pipeline_engine/`)

- **`scheduler.py` — AsyncScheduler**: Drives the event loop. Resolves topo order via NetworkX, dispatches ready tasks as `asyncio.Task`s, handles abort/resume signals via `asyncio.Event`.
- **`state_manager.py` — StateManager**: Single source of truth for all runtime state. Protected by `asyncio.Lock` so REPL reads never race with task writes. Persists snapshots atomically after each task completes.
- **`plugin_loader.py`**: Dynamically imports task classes from dotted paths (e.g. `mymodule.tasks.ParseDXF`). Validates that each class inherits `BaseTask`.
- **`base_task.py` — BaseTask**: Abstract base that all user tasks must subclass. Exposes `async def execute(self, inputs: dict) -> dict` and a `progress` callback.
- **`yaml_parser.py`**: Loads and validates pipeline YAML against the Pydantic schema. Builds the internal Pipeline/Step/Task data structures.
- **`repl.py`**: `prompt_toolkit`-based non-blocking REPL running as an `asyncio` coroutine alongside the scheduler.
- **`cli.py`**: Entry point; defines subcommands (`load`, `lint`, `list`, `start`, `stop`, `resume`, `status`, `inspect`, `fix`).

### Data flow

Tasks exchange data via `input.json` / `output.json` stored under the run's workspace directory. A downstream task declares `depends_on: [task_id]`; the scheduler injects the upstream `output.json` as the downstream's input at dispatch time.

### Skip mode

When a Step is marked `skip: true`, the engine must verify that `./manual_data/<step_id>/output.json` exists **before** proceeding. Failure to find it is a hard error, not a warning.

### Error recovery (`fix` command)

`fix <task_id> --input path/to/data.json` or `fix <task_id> --output path/to/data.json` writes the supplied file into the workspace and transitions the task from `Failed` → `New` (input injection) or `Failed` → `Fixed` (output injection), so `resume` can re-schedule it.

## Code Style

- All function signatures must carry type annotations.
- Never raise bare `Exception`. Use a custom `PipelineError(step_id, task_id, message)` that captures context.
- Comments only where the *why* is non-obvious (e.g., why a lock is held across a specific block).

## Design Constraints (non-negotiable)

1. The engine must never import or reference business-domain code directly.
2. Task exceptions must be caught and recorded as `Failed` state — they must not propagate to and crash the REPL process.
3. `asyncio.Lock` must guard every StateManager mutation; REPL reads go through the same lock.
4. Each task's output must be written to disk atomically (write to `.tmp` then `os.replace`) before the in-memory state is marked `Success`.
