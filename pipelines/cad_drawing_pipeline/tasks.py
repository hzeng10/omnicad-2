"""CAD drawing pipeline tasks — developer reference covering all BaseTask interfaces.

Interface coverage matrix
-------------------------
Task                    | Entry point   | InputModel        | OutputModel      | progress
------------------------|---------------|-------------------|------------------|----------
ParseRequirement        | async execute | ParseRequirement  | ParseRequirement | -
GenerateFloorLayout     | async execute | -                 | LayoutOutput     | YES (x4)
GenerateElectricalLayout| run_sync      | -                 | LayoutOutput     | -
RefineDrawing           | (skip stub)   | -                 | RefineDrawing    | -
ExportDXF               | async execute | ExportDXFInput    | DXFExportOutput  | YES (x4)
ValidateDXF             | async execute | ValidateDXFInput  | ValidationOutput | -

How cross-step inputs are structured
--------------------------------------
depends_on_steps: [parse_requirement]
  → inputs["parse_requirement"] = {"parse_req": {...ParseRequirementOutput...}}
  → Access: inputs["parse_requirement"]["parse_req"]["rooms"]

depends_on_steps: [refine_drawing]  (skip=true step)
  → inputs["refine_drawing"] = full manual_data JSON = {...RefineDrawingOutput...}
  → ExportDXFInput wraps this: ExportDXFInput(refine_drawing={...})

depends_on: [export]  (same-step dependency)
  → inputs["export"] = {...DXFExportOutput...}
  → ValidateDXFInput wraps this: ValidateDXFInput(export={...})

Environment variables
---------------------
PIPELINE_DEMO_FAST=1           : scale all sleeps to 0.1× for CI
PIPELINE_DEMO_FAIL=validate_dxf: force ValidateDXF to raise PipelineError
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

from pipeline_engine.core.base_task import BaseTask
from pipeline_engine.core.errors import PipelineError
from .schemas import (
    ParseRequirementInput, ParseRequirementOutput,
    GeometryElement, LayoutOutput,
    RefineDrawingOutput,
    ExportDXFInput, DXFExportOutput,
    ValidateDXFInput, ValidationOutput,
)


def _sleep_scale(seconds: float) -> float:
    return seconds * 0.1 if os.environ.get("PIPELINE_DEMO_FAST") == "1" else seconds


# ─── Step 1: parse_requirement ────────────────────────────────────────────────

class ParseRequirement(BaseTask):
    """Read and validate a JSON requirement file.

    InputModel demo:
      YAML declares ``inputs: {requirement_path: "..."}`` (NOT config:).
      The engine merges this into inputs, then calls
      ``InputModel.model_validate(inputs)`` before execute().
      This validates that requirement_path is present and is a string.

    OutputModel demo:
      Returned dict is validated against ParseRequirementOutput before being
      written to disk and passed to downstream tasks.
    """
    InputModel = ParseRequirementInput
    OutputModel = ParseRequirementOutput

    async def execute(self, inputs: dict[str, Any], progress) -> dict[str, Any]:
        req_path = inputs["requirement_path"]
        await asyncio.sleep(_sleep_scale(0.05))
        with open(req_path) as f:
            return json.load(f)


# ─── Step 2: generate_layout ──────────────────────────────────────────────────

class GenerateFloorLayout(BaseTask):
    """Generate floor plan geometry from parsed requirements.

    Progress demo: pushes 25 / 50 / 75 / 100 during the 4-stage layout pass.

    Cross-step input demo:
      YAML: depends_on_steps: [parse_requirement]
      Runtime: inputs["parse_requirement"] = {"parse_req": {ParseRequirementOutput fields}}
      Access:  inputs["parse_requirement"]["parse_req"]["floor_size_m"]

    No InputModel is declared here — cross-step data arrives nested under
    the step_id key, which doesn't map cleanly to a flat Pydantic model.
    OutputModel is still declared to validate the returned geometry dict.
    """
    OutputModel = LayoutOutput

    async def execute(self, inputs: dict[str, Any], progress) -> dict[str, Any]:
        req = inputs["parse_requirement"]["parse_req"]
        floor_size = req["floor_size_m"]
        rooms = req["rooms"]

        for pct in (25, 50, 75, 100):
            await asyncio.sleep(_sleep_scale(0.3))
            await progress(pct)

        elements = [
            {"id": f"floor_wall_{i}", "element_type": "wall",
             "coords": [[0.0, float(i)], [float(floor_size[0]), float(i)]]}
            for i in range(len(rooms))
        ]
        return {
            "layer": "FLOOR",
            "elements": elements,
            "bbox": (0.0, 0.0, float(floor_size[0]), float(floor_size[1])),
        }


class GenerateElectricalLayout(BaseTask):
    """Generate electrical schematic geometry from parsed requirements.

    run_sync demo:
      Override run_sync() for blocking / CPU-bound operations (e.g. third-party
      CAD SDKs that don't support async). The engine automatically wraps it in
      asyncio.to_thread so the event loop is never blocked.
      progress here is a _SyncProgressAdapter — call it directly (no await).
    """
    OutputModel = LayoutOutput

    def run_sync(self, inputs: dict[str, Any], progress) -> dict[str, Any]:
        req = inputs["parse_requirement"]["parse_req"]
        circuits = req["circuits"]
        floor_size = req["floor_size_m"]

        time.sleep(_sleep_scale(0.3))

        elements = [
            {"id": f"elec_circuit_{c['id']}", "element_type": "circuit",
             "coords": [[float(i) * 0.5, 0.0], [float(i) * 0.5, float(floor_size[1])]]}
            for i, c in enumerate(circuits)
        ]
        return {
            "layer": "ELEC",
            "elements": elements,
            "bbox": (0.0, 0.0, float(floor_size[0]), float(floor_size[1])),
        }


# ─── Step 3: refine_drawing (skip stub) ───────────────────────────────────────

class RefineDrawing(BaseTask):
    """Merge floor and electrical layouts, add annotations.

    The containing step is marked skip: true in pipeline.yaml.
    The engine loads <workspace>/manual_data/refine_drawing/output.json instead
    of calling execute().  This class only needs to be importable by the plugin
    loader; execute() is a stub that raises if unexpectedly invoked.
    """
    OutputModel = RefineDrawingOutput

    async def execute(self, inputs: dict[str, Any], progress) -> dict[str, Any]:
        raise PipelineError(
            "RefineDrawing.execute() must not be called: "
            "step refine_drawing is marked skip=true",
            step_id="refine_drawing",
            task_id="refine",
        )


# ─── Step 4: export_dxf ──────────────────────────────────────────────────────

class ExportDXF(BaseTask):
    """Render the refined drawing to a DXF file.

    InputModel demo (cross-step skip output):
      YAML: depends_on_steps: [refine_drawing]
      Since refine_drawing is skip=true, the engine calls load_manual_data()
      and stores the result as inputs["refine_drawing"] = full JSON dict.
      ExportDXFInput wraps it: ExportDXFInput(refine_drawing={...}).
      This is the pattern for validating skip-step outputs with InputModel.

    Progress demo: 4-stage render progress (25/50/75/100).
    """
    InputModel = ExportDXFInput
    OutputModel = DXFExportOutput

    async def execute(self, inputs: dict[str, Any], progress) -> dict[str, Any]:
        drawing = inputs["refine_drawing"]
        drawing_id = drawing["drawing_id"]
        elements = drawing["elements"]

        for pct in (25, 50, 75, 100):
            await asyncio.sleep(_sleep_scale(0.3))
            await progress(pct)

        entity_count = len(elements)
        return {
            "file_path": f"/tmp/{drawing_id}.dxf",
            "entity_count": entity_count,
            "file_size_bytes": entity_count * 128,
        }


class ValidateDXF(BaseTask):
    """Validate the exported DXF file against standard rules.

    InputModel demo (same-step depends_on):
      YAML: depends_on: [export]
      inputs["export"] = DXFExportOutput dict from the preceding ExportDXF task.
      ValidateDXFInput wraps it: ValidateDXFInput(export={...}).
      This is the cleanest InputModel pattern: within-step dependency outputs
      map 1:1 to named InputModel fields.

    PIPELINE_DEMO_FAIL demo:
      Set PIPELINE_DEMO_FAIL=validate_dxf to trigger intentional failure.
      Recover via:
        fix <instance_id> --task export_dxf/validate --output recovered.json
        resume <instance_id>
    """
    InputModel = ValidateDXFInput
    OutputModel = ValidationOutput

    async def execute(self, inputs: dict[str, Any], progress) -> dict[str, Any]:
        if os.environ.get("PIPELINE_DEMO_FAIL") == "validate_dxf":
            raise PipelineError(
                "DEMO_FAIL: validate_dxf forced to fail via PIPELINE_DEMO_FAIL env var",
                step_id="export_dxf",
                task_id="validate",
            )

        await asyncio.sleep(_sleep_scale(0.1))
        export = inputs["export"]
        checked_rules = ["entity_count_positive", "file_size_nonzero", "dxf_version_check"]
        issues: list[str] = []

        if export.get("entity_count", 0) <= 0:
            issues.append("entity_count must be > 0")
        if export.get("file_size_bytes", 0) <= 0:
            issues.append("file_size_bytes must be > 0")

        return {
            "is_valid": len(issues) == 0,
            "checked_rules": checked_rules,
            "issues": issues,
        }
