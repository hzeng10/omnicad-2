# CAD Pipeline Example

A mock CAD cost-estimation pipeline demonstrating:
- 4-step DAG with serial + parallel execution
- Long-running tasks (8s, 10s) with live progress
- Mixed `execute` (async) and `run_sync` (thread pool) task entry points
- `fix --output` failure recovery

## Pipeline Structure

```
parse_dxf  →  split_subgraph  →  recognize (3 parallel)  →  aggregate
              (2s)               (8s | 10s | 6s)
  (2s + 3s)                                                   (2s)
```

The `recognize` step runs 3 tasks in parallel; wall-clock time ≈ 10s (the slowest).

## Quick Start

```bash
# Lint (validate YAML without running)
python -m pipeline_engine.cli lint examples/cad_pipeline/pipeline.yaml

# Run with blocking wait
python -m pipeline_engine.cli start cad_cost_estimation \
  --workspace /tmp/demo \
  --wait

# Interactive REPL
python -m pipeline_engine.cli --workspace /tmp/demo
pipeline> load examples/cad_pipeline/pipeline.yaml
pipeline> start cad_cost_estimation
pipeline> status cad_cost_estimation --watch    # live Rich table
pipeline> inspect cad_cost_estimation --step recognize --task rec_cable
```

## Failure Recovery Demo

```bash
# Force rec_cable to fail
PIPELINE_DEMO_FAIL=rec_cable python -m pipeline_engine.cli \
  --workspace /tmp/demo_fail

pipeline> load examples/cad_pipeline/pipeline.yaml
pipeline> start cad_cost_estimation
pipeline> status cad_cost_estimation
# rec_cable shows FAILED

pipeline> fix cad_cost_estimation --task recognize/rec_cable \
           --output examples/cad_pipeline/mock_data/recover_cable.json
# rec_cable → FIXED

pipeline> resume cad_cost_estimation
pipeline> status cad_cost_estimation
# aggregate/merge → SUCCESS, grand_total computed
```

## Fast Mode (CI)

Set `PIPELINE_DEMO_FAST=1` to scale all sleep durations by 0.1×:

```bash
PIPELINE_DEMO_FAST=1 pytest tests/e2e/
```
