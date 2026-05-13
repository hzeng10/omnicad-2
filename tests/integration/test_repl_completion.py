"""Integration tests for PipelineReplCompleter."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from prompt_toolkit.document import Document

from pipeline_engine.core.run_manager import RunManager
from pipeline_engine.models.pipeline_spec import (
    PipelineMeta,
    PipelineSpec,
    StepSpec,
    TaskSpec,
)
from pipeline_engine.models.runtime_state import Status
from pipeline_engine.repl_completion import PipelineReplCompleter


# ─── fixtures ────────────────────────────────────────────────────────────────

def _make_spec(pid: str, steps_tasks: dict[str, list[str]] | None = None) -> PipelineSpec:
    """Build a PipelineSpec with the given step/task structure."""
    if steps_tasks is None:
        steps_tasks = {"step_a": ["t1"]}
    steps = [
        StepSpec(
            id=sid,
            tasks=[TaskSpec(id=tid, plugin="tests.fake.FakeTask") for tid in tids],
        )
        for sid, tids in steps_tasks.items()
    ]
    return PipelineSpec(
        version="1.0",
        pipeline=PipelineMeta(id=pid, name=f"Pipeline {pid}", type="测试类型"),
        steps=steps,
    )


def _make_ctx(pipeline_id: str, run_id: str, status: Status = Status.RUNNING) -> MagicMock:
    """Build a minimal RunContext mock (no asyncio overhead)."""
    ctx = MagicMock()
    ctx.pipeline_id = pipeline_id
    ctx.run_id = run_id
    # Assign the real Status enum so `.value` works naturally
    ctx.state_manager._state.status = status
    return ctx


def _completer(tmp_path) -> PipelineReplCompleter:
    rm = RunManager(tmp_path)
    return PipelineReplCompleter(rm)


def _complete(completer: PipelineReplCompleter, text: str) -> list[str]:
    """Return list of display strings for completions of `text`."""
    doc = Document(text)
    return [c.display if isinstance(c.display, str) else c.display[0][1] for c in completer.get_completions(doc, None)]


# ─── command name completion ──────────────────────────────────────────────────

def test_command_completion_empty_prefix(tmp_path):
    c = _completer(tmp_path)
    results = _complete(c, "")
    from pipeline_engine.repl_completion import COMMANDS
    assert set(results) == set(COMMANDS.keys())


def test_command_completion_partial_prefix(tmp_path):
    c = _completer(tmp_path)
    results = _complete(c, "sta")
    assert "start" in results
    assert "status" in results
    assert "stop" not in results   # "stop" starts with "sto", not "sta"
    assert "load" not in results


def test_command_completion_unique_prefix(tmp_path):
    c = _completer(tmp_path)
    results = _complete(c, "lo")
    assert results == ["load"]


# ─── pipeline_id completion (start command) ───────────────────────────────────

def test_start_completes_loaded_pipeline_ids(tmp_path):
    rm = RunManager(tmp_path)
    rm._registry["cad_cost_estimation"] = _make_spec("cad_cost_estimation")
    rm._registry["cad_generation"] = _make_spec("cad_generation")
    c = PipelineReplCompleter(rm)

    results = _complete(c, "start ")
    assert "cad_cost_estimation" in results
    assert "cad_generation" in results


def test_start_completes_pipeline_id_with_prefix(tmp_path):
    rm = RunManager(tmp_path)
    rm._registry["cad_cost_estimation"] = _make_spec("cad_cost_estimation")
    rm._registry["cad_generation"] = _make_spec("cad_generation")
    rm._registry["other_pipe"] = _make_spec("other_pipe")
    c = PipelineReplCompleter(rm)

    results = _complete(c, "start cad_")
    assert "cad_cost_estimation" in results
    assert "cad_generation" in results
    assert "other_pipe" not in results


def test_start_completes_no_candidates_when_registry_empty(tmp_path):
    c = _completer(tmp_path)
    results = _complete(c, "start ")
    assert results == []


def test_start_display_meta_contains_type_and_name(tmp_path):
    rm = RunManager(tmp_path)
    rm._registry["cad_cost_estimation"] = _make_spec("cad_cost_estimation")
    c = PipelineReplCompleter(rm)

    doc = Document("start ")
    completions = list(c.get_completions(doc, None))
    assert len(completions) == 1
    meta = completions[0].display_meta
    meta_str = meta if isinstance(meta, str) else meta[0][1]
    assert "测试类型" in meta_str
    assert "Pipeline cad_cost_estimation" in meta_str


# ─── instance_id completion (ref commands) ────────────────────────────────────

def test_ref_completion_lists_instance_ids(tmp_path):
    rm = RunManager(tmp_path)
    rm._registry["cad_cost_estimation"] = _make_spec("cad_cost_estimation")
    rm._runs["cad_cost_estimation_20260513-093024_7392"] = _make_ctx(
        "cad_cost_estimation", "cad_cost_estimation_20260513-093024_7392", Status.RUNNING
    )
    c = PipelineReplCompleter(rm)

    for cmd in ("stop", "resume", "status", "inspect", "fix"):
        results = _complete(c, f"{cmd} ")
        assert "cad_cost_estimation_20260513-093024_7392" in results, f"Failed for cmd={cmd}"


def test_ref_completion_display_meta_has_pipeline_and_status(tmp_path):
    rm = RunManager(tmp_path)
    rm._registry["cad_cost_estimation"] = _make_spec("cad_cost_estimation")
    run_id = "cad_cost_estimation_20260513-093024_7392"
    rm._runs[run_id] = _make_ctx("cad_cost_estimation", run_id, Status.SUCCESS)
    c = PipelineReplCompleter(rm)

    doc = Document("status ")
    completions = list(c.get_completions(doc, None))
    assert completions
    meta = completions[0].display_meta
    meta_str = meta if isinstance(meta, str) else meta[0][1]
    assert "pipeline=cad_cost_estimation" in meta_str
    assert "status=success" in meta_str


def test_ref_completion_empty_when_no_runs(tmp_path):
    c = _completer(tmp_path)
    results = _complete(c, "stop ")
    assert results == []


# ─── step_id completion ───────────────────────────────────────────────────────

def test_step_flag_completes_step_ids_with_pipeline_context(tmp_path):
    rm = RunManager(tmp_path)
    spec = _make_spec("cad_cost_estimation", {
        "parse_dxf": ["t1"],
        "split_subgraph": ["t1"],
        "recognize": ["rec_building", "rec_cable"],
        "aggregate": ["t1"],
    })
    rm._registry["cad_cost_estimation"] = spec
    c = PipelineReplCompleter(rm)

    results = _complete(c, "inspect cad_cost_estimation --step ")
    assert set(results) == {"parse_dxf", "split_subgraph", "recognize", "aggregate"}


def test_step_flag_completion_via_instance_id(tmp_path):
    rm = RunManager(tmp_path)
    spec = _make_spec("cad_cost_estimation", {"step_a": ["t1"], "step_b": ["t2"]})
    rm._registry["cad_cost_estimation"] = spec
    run_id = "cad_cost_estimation_20260513-093024_0001"
    rm._runs[run_id] = _make_ctx("cad_cost_estimation", run_id)
    c = PipelineReplCompleter(rm)

    results = _complete(c, f"inspect {run_id} --step ")
    assert "step_a" in results
    assert "step_b" in results


def test_step_flag_no_context_returns_empty(tmp_path):
    rm = RunManager(tmp_path)
    c = PipelineReplCompleter(rm)
    results = _complete(c, "inspect --step ")
    assert results == []


# ─── task_ref completion ──────────────────────────────────────────────────────

def test_task_flag_completes_all_step_slash_task_refs(tmp_path):
    rm = RunManager(tmp_path)
    spec = _make_spec("cad_cost_estimation", {
        "recognize": ["rec_building", "rec_cable", "rec_panel"],
    })
    rm._registry["cad_cost_estimation"] = spec
    run_id = "cad_cost_estimation_20260513-093024_0002"
    rm._runs[run_id] = _make_ctx("cad_cost_estimation", run_id)
    c = PipelineReplCompleter(rm)

    results = _complete(c, f"fix {run_id} --task ")
    assert "recognize/rec_building" in results
    assert "recognize/rec_cable" in results
    assert "recognize/rec_panel" in results


def test_task_flag_completes_after_step_slash(tmp_path):
    rm = RunManager(tmp_path)
    spec = _make_spec("cad_cost_estimation", {
        "recognize": ["rec_building", "rec_cable", "rec_panel"],
    })
    rm._registry["cad_cost_estimation"] = spec
    run_id = "cad_cost_estimation_20260513-093024_0003"
    rm._runs[run_id] = _make_ctx("cad_cost_estimation", run_id)
    c = PipelineReplCompleter(rm)

    results = _complete(c, f"fix {run_id} --task recognize/rec_")
    assert "recognize/rec_building" in results
    assert "recognize/rec_cable" in results
    assert "recognize/rec_panel" in results
    # Should not include tasks from other non-matching steps (there are none here)


def test_task_flag_step_slash_exact_match(tmp_path):
    rm = RunManager(tmp_path)
    spec = _make_spec("cad_cost_estimation", {
        "step_a": ["task_x", "task_y"],
        "step_b": ["task_z"],
    })
    rm._registry["cad_cost_estimation"] = spec
    run_id = "cad_cost_estimation_20260513-093024_0004"
    rm._runs[run_id] = _make_ctx("cad_cost_estimation", run_id)
    c = PipelineReplCompleter(rm)

    # Typing "step_a/" should show only step_a tasks
    results = _complete(c, f"fix {run_id} --task step_a/")
    assert "step_a/task_x" in results
    assert "step_a/task_y" in results
    assert "step_b/task_z" not in results


# ─── flag completion ──────────────────────────────────────────────────────────

def test_flag_completion_shows_available_flags(tmp_path):
    c = _completer(tmp_path)
    results = _complete(c, "list ")
    assert "--pipeline" in results
    assert "--instance" in results


def test_flag_completion_skips_already_present(tmp_path):
    c = _completer(tmp_path)
    results = _complete(c, "list --pipeline ")
    # --pipeline already used, only --instance should remain
    assert "--instance" in results
    assert "--pipeline" not in results


def test_flag_partial_prefix(tmp_path):
    c = _completer(tmp_path)
    results = _complete(c, "status --w")
    assert "--watch" in results


# ─── path completion ──────────────────────────────────────────────────────────

def test_load_path_delegated_to_path_completer(tmp_path):
    # Create a subdirectory so PathCompleter has something to find
    (tmp_path / "cad_pipeline").mkdir()
    (tmp_path / "cad_pipeline" / "pipeline.yaml").write_text("x")
    c = _completer(tmp_path)

    # PathCompleter works with real filesystem; just verify no exception
    import os
    orig = os.getcwd()
    try:
        os.chdir(tmp_path)
        results = _complete(c, "load cad_pipeline/")
        assert any("pipeline.yaml" in r for r in results)
    finally:
        os.chdir(orig)


def test_fix_output_path_delegated_to_path_completer(tmp_path):
    (tmp_path / "recovered.json").write_text("{}")
    rm = RunManager(tmp_path)
    rm._registry["mypipe"] = _make_spec("mypipe")
    run_id = "mypipe_20260513-093024_0001"
    rm._runs[run_id] = _make_ctx("mypipe", run_id)
    c = PipelineReplCompleter(rm)

    import os
    orig = os.getcwd()
    try:
        os.chdir(tmp_path)
        results = _complete(c, f"fix {run_id} --output recovered")
        assert any("recovered.json" in r for r in results)
    finally:
        os.chdir(orig)


# ─── edge cases ───────────────────────────────────────────────────────────────

def test_unknown_command_returns_no_completions(tmp_path):
    c = _completer(tmp_path)
    results = _complete(c, "foobar ")
    assert results == []


def test_get_status_handles_missing_state(tmp_path):
    """_get_status must return '?' when state_manager raises on _state access."""

    class _FailingSM:
        @property
        def _state(self):
            raise AttributeError("no state")

    class _Ctx:
        state_manager = _FailingSM()

    result = PipelineReplCompleter._get_status(_Ctx())
    assert result == "?"


def test_status_fallback_when_snapshot_raises(tmp_path):
    """If state_manager._state.status.value throws, display_meta contains 'status=?'."""
    rm = RunManager(tmp_path)
    rm._registry["mypipe"] = _make_spec("mypipe")
    run_id = "mypipe_20260513-093024_0002"

    class _FailingStatus:
        @property
        def value(self) -> str:
            raise RuntimeError("broken")

    ctx = MagicMock()
    ctx.pipeline_id = "mypipe"
    ctx.run_id = run_id
    ctx.state_manager._state.status = _FailingStatus()
    rm._runs[run_id] = ctx
    c = PipelineReplCompleter(rm)

    doc = Document("stop ")
    completions = list(c.get_completions(doc, None))
    assert completions, "expected at least one completion"
    meta_values = [
        (comp.display_meta if isinstance(comp.display_meta, str) else comp.display_meta[0][1])
        for comp in completions
    ]
    assert any("status=?" in m for m in meta_values)
