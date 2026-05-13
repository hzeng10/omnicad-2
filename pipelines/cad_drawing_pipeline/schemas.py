"""Pydantic I/O schemas for CAD drawing pipeline tasks.

InputModel / OutputModel 用法说明
-----------------------------------
- InputModel: 声明为任务类变量 InputModel = XxxInput。
  引擎在调用 execute() 前用 InputModel.model_validate(inputs) 校验整个 inputs 字典。
  inputs 的结构由以下来源合并而来：
    * YAML inputs: 静态字段（最适合 InputModel 校验，字段名直接对应）
    * depends_on 同 step 内上游任务输出（inputs[task_id] = 上游 output dict）
    * depends_on_steps 跨 step 输出（inputs[step_id] = dict of outputs or manual_data）
- OutputModel: 声明为任务类变量 OutputModel = XxxOutput。
  引擎在 execute() 返回后用 OutputModel.model_validate(result) 校验输出字典。
"""
from __future__ import annotations

from pydantic import BaseModel, Field


# ─── Step 1: parse_requirement ────────────────────────────────────────────────

class Room(BaseModel):
    name: str
    area_m2: float
    room_type: str


class Circuit(BaseModel):
    id: str
    load_kw: float
    rooms: list[str]


class ParseRequirementInput(BaseModel):
    """InputModel for ParseRequirement.

    YAML 中通过 inputs: {requirement_path: ...} 注入，Pydantic 在执行前校验。
    这是 InputModel 的典型用法：静态配置通过 inputs: 传入而非 config:，
    从而可以被 InputModel 校验和文档化。
    """
    requirement_path: str


class ParseRequirementOutput(BaseModel):
    """Parsed building requirement — consumed by downstream layout tasks."""
    project_name: str
    rooms: list[Room]
    circuits: list[Circuit]
    floor_size_m: tuple[float, float]


# ─── Step 2: generate_layout ──────────────────────────────────────────────────

class GeometryElement(BaseModel):
    id: str
    element_type: str
    coords: list[list[float]]


class LayoutOutput(BaseModel):
    """Geometric layout produced by a layout generation task.

    Gen floor/electrical tasks use this as OutputModel.
    No InputModel is declared here because the cross-step input
    (inputs["parse_requirement"]["parse_req"]) is a nested structure that
    doesn't map 1:1 to a flat Pydantic model without extra wrapping.
    """
    layer: str
    elements: list[GeometryElement]
    bbox: tuple[float, float, float, float]


# ─── Step 3: refine_drawing (skipped — output loaded from manual_data) ────────

class DrawingElement(BaseModel):
    element_id: str
    layer: str
    annotation: str | None = None


class RefineDrawingOutput(BaseModel):
    """Merged and annotated drawing.

    When step is skipped, the engine loads this from
    <workspace>/manual_data/refine_drawing/output.json.
    The JSON file must conform to this schema.
    """
    drawing_id: str
    layers: list[str]
    elements: list[DrawingElement]
    scale: float = Field(default=1.0)


# ─── Step 4: export_dxf ──────────────────────────────────────────────────────

class ExportDXFInput(BaseModel):
    """InputModel for ExportDXF.

    inputs["refine_drawing"] is the manual_data JSON for the skipped step.
    Since the skipped step's manual_data is loaded as a flat dict (not nested
    under a task key), ExportDXFInput wraps it as a nested RefineDrawingOutput.
    """
    refine_drawing: RefineDrawingOutput


class DXFExportOutput(BaseModel):
    file_path: str
    entity_count: int
    file_size_bytes: int


class ValidateDXFInput(BaseModel):
    """InputModel for ValidateDXF.

    inputs["export"] is the output of the same-step ExportDXF task.
    Declaring an 'export' field here demonstrates how within-step
    depends_on output maps cleanly to an InputModel field.
    """
    export: DXFExportOutput


class ValidationOutput(BaseModel):
    is_valid: bool
    checked_rules: list[str]
    issues: list[str]
