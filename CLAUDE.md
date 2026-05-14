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
pip install -e .                                                   # install with dev deps
pytest                                                             # run all tests
pytest tests/unit/test_cli.py::test_load_single_pipeline -v       # run a single test
pytest --cov=pipeline_engine --cov-fail-under=90                  # run with coverage gate
pipeline_cli lint path/to/pipeline.yaml                           # validate YAML schema (JSON out)
pipeline_cli list                                                  # list registered pipelines (JSON)
pipeline_cli --no-autoload list                                    # skip autoload discovery
```

### CLI output modes

All `pipeline_cli <subcommand>` one-shot invocations output a **single JSON object** to stdout
(flat envelope with `ok` field), suitable for `json.loads()` by AI Agents:

```json
{"ok": true,  "command": "list", "scope": "pipeline", "pipelines": [...]}
{"ok": false, "command": "start", "error": {"message": "...", "type": "PipelineError", ...}}
```

Running `pipeline_cli` without a subcommand enters the REPL — Rich text rendering, behaviour unchanged.

**New CLI subcommands must call `cli_json.emit()` / `cli_json.emit_error()` for stdout output.**
Output is formatted with `indent=2` by default; `json.loads()` handles multi-line JSON fine.

**`start` defaults to `--wait` (blocks until run completes).** Use `--no-wait` for fire-and-forget
(the run is cancelled when the CLI process exits — not suitable for long jobs).

**New subcommands that read run state**: call `_bootstrap(rm, ctx, restore_runs=True)` with the
default `restore_writeback=False` (read-only — does not demote RUNNING→FAILED). Only `resume`
and `fix` should pass `restore_writeback=True`.

### Autoload

On startup, the CLI auto-discovers `./pipelines/*/pipeline.yaml` (one-level deep) and registers
each as if `load` were called. Override with `--pipelines-dir DIR` (or `PIPELINE_AUTOLOAD_DIR`).
Disable entirely with `--no-autoload` (or `PIPELINE_NO_AUTOLOAD=1`).

All unit tests that invoke the CLI must pass `--no-autoload` as the first argument (before the
subcommand name) to prevent real `./pipelines/` from polluting tmp-path test workspaces.

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

Each run produces a single `run.log` under `.pipeline_runs/<pipeline_id>/<run_id>/`, viewable via REPL `log <instance_id>`. Resume appends to the same file.

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

### View-model layer

CLI JSON output and REPL rendering both go through `pipeline_engine.view_model`. Use
`build_pipeline_status_view(state)` / `build_task_detail_view(ts, log_tail_size=...)` rather
than calling `state.model_dump()` or constructing ad-hoc dicts directly.

Field set and order must remain in sync with runtime models (`pipeline_engine.models.runtime_state`);
the transparency invariant is enforced by `tests/unit/test_view_model.py`.

`log_tail_size` convention: CLI callers pass `100` (default); REPL callers pass `200`.

## Code Style

- All function signatures must carry type annotations.
- Never raise bare `Exception`. Use a custom `PipelineError(step_id, task_id, message)` that captures context.
- Comments only where the *why* is non-obvious (e.g., why a lock is held across a specific block).

## Design Constraints (non-negotiable)

1. The engine must never import or reference business-domain code directly.
2. Task exceptions must be caught and recorded as `Failed` state — they must not propagate to and crash the REPL process.
3. `asyncio.Lock` must guard every StateManager mutation; REPL reads go through the same lock.
4. Each task's output must be written to disk atomically (write to `.tmp` then `os.replace`) before the in-memory state is marked `Success`.
