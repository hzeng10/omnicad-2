"""Pydantic I/O schemas for CAD pipeline tasks."""
from __future__ import annotations

from pydantic import BaseModel


class Entity(BaseModel):
    id: str
    layer: str
    type: str
    bbox: tuple[float, float, float, float]


class ReadDxfOutput(BaseModel):
    file_path: str
    entity_count: int
    raw_path: str


class ParseEntitiesOutput(BaseModel):
    entities: list[Entity]


class Subgraph(BaseModel):
    id: str
    bbox: tuple[float, float, float, float]
    entity_ids: list[str]


class SplitSubgraphOutput(BaseModel):
    subgraphs: list[Subgraph]


class RecognizedItem(BaseModel):
    category: str
    name: str
    count: int
    subgraph_id: str


class RecognizeOutput(BaseModel):
    items: list[RecognizedItem]


class CostSummaryItem(BaseModel):
    category: str
    name: str
    total_count: int
    unit_price: float
    subtotal: float


class MergeOutput(BaseModel):
    summary: list[CostSummaryItem]
    grand_total: float
