"""Mock CAD pipeline tasks — 7 tasks demonstrating both BaseTask entry points.

Set PIPELINE_DEMO_FAST=1 to scale all sleeps by 0.1 (for CI / fast testing).
Set PIPELINE_DEMO_FAIL=<task_id> to force that task to raise RuntimeError.
"""
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Any

from pipeline_engine.core.base_task import BaseTask
from .schemas import (
    ReadDxfOutput, ParseEntitiesOutput, SplitSubgraphOutput,
    RecognizeOutput, MergeOutput,
    Entity, Subgraph, RecognizedItem, CostSummaryItem,
)

_MOCK_DIR = Path(__file__).parent / "mock_data"
def _sleep_scale(seconds: float) -> float:
    return seconds * 0.1 if os.environ.get("PIPELINE_DEMO_FAST") == "1" else seconds


def _check_fail(task_id: str) -> None:
    fail_target = os.environ.get("PIPELINE_DEMO_FAIL", "")
    if fail_target and fail_target == task_id:
        raise RuntimeError(f"Intentional failure injected via PIPELINE_DEMO_FAIL={task_id}")


# ─── Step 1: parse_dxf ────────────────────────────────────────────────────────

class ReadDxfTask(BaseTask):
    """Read DXF file — uses run_sync (I/O simulation via thread pool)."""
    OutputModel = ReadDxfOutput

    def run_sync(self, inputs: dict[str, Any], progress) -> dict[str, Any]:
        _check_fail(self.task_id)
        dxf_path = self.config.get("dxf_path", "sample.dxf")
        steps = 10
        for i in range(steps):
            time.sleep(_sleep_scale(0.2))
            progress(int((i + 1) / steps * 100))
        return {
            "file_path": dxf_path,
            "entity_count": 1234,
            "raw_path": str(_MOCK_DIR / "dxf_entities.json"),
        }


class ParseEntitiesTask(BaseTask):
    """Parse DXF entities — async entry point."""
    OutputModel = ParseEntitiesOutput

    async def execute(self, inputs: dict[str, Any], progress) -> dict[str, Any]:
        _check_fail(self.task_id)
        steps = 10
        for i in range(steps):
            await asyncio.sleep(_sleep_scale(0.3))
            await progress(int((i + 1) / steps * 100))
        entities = [
            {"id": f"e{i}", "layer": "0", "type": "LINE", "bbox": (0.0, 0.0, 1.0, 1.0)}
            for i in range(50)
        ]
        return {"entities": entities}


# ─── Step 2: split_subgraph ───────────────────────────────────────────────────

class SplitSubgraphTask(BaseTask):
    """Split entities into spatial subgraphs — run_sync."""
    OutputModel = SplitSubgraphOutput

    def run_sync(self, inputs: dict[str, Any], progress) -> dict[str, Any]:
        _check_fail(self.task_id)
        steps = 10
        for i in range(steps):
            time.sleep(_sleep_scale(0.2))
            progress(int((i + 1) / steps * 100))
        subgraphs = [
            {"id": f"sg_{j}", "bbox": (0.0, 0.0, 10.0, 10.0), "entity_ids": [f"e{j*5+k}" for k in range(5)]}
            for j in range(4)
        ]
        return {"subgraphs": subgraphs}


# ─── Step 3: recognize (3 parallel tasks) ─────────────────────────────────────

class RecBuildingTask(BaseTask):
    """Building recognition — run_sync, 8s (CPU-bound simulation)."""
    OutputModel = RecognizeOutput

    def run_sync(self, inputs: dict[str, Any], progress) -> dict[str, Any]:
        _check_fail(self.task_id)
        steps = 10
        for i in range(steps):
            time.sleep(_sleep_scale(0.8))
            progress(int((i + 1) / steps * 100))
        return {
            "items": [
                {"category": "building", "name": "Office Block A", "count": 3, "subgraph_id": "sg_0"},
                {"category": "building", "name": "Warehouse B",    "count": 1, "subgraph_id": "sg_1"},
            ]
        }


class RecCableTask(BaseTask):
    """Cable recognition — async, 10s (long-running I/O simulation)."""
    OutputModel = RecognizeOutput

    async def execute(self, inputs: dict[str, Any], progress) -> dict[str, Any]:
        _check_fail(self.task_id)
        steps = 10
        for i in range(steps):
            await asyncio.sleep(_sleep_scale(1.0))
            await progress(int((i + 1) / steps * 100))
        return {
            "items": [
                {"category": "cable", "name": "YJV-4x16", "count": 42, "subgraph_id": "sg_2"},
                {"category": "cable", "name": "YJV-3x10", "count": 18, "subgraph_id": "sg_3"},
            ]
        }


class RecSchematicTask(BaseTask):
    """Schematic recognition — run_sync, 6s."""
    OutputModel = RecognizeOutput

    def run_sync(self, inputs: dict[str, Any], progress) -> dict[str, Any]:
        _check_fail(self.task_id)
        steps = 10
        for i in range(steps):
            time.sleep(_sleep_scale(0.6))
            progress(int((i + 1) / steps * 100))
        return {
            "items": [
                {"category": "schematic", "name": "Panel-MCC1", "count": 2, "subgraph_id": "sg_0"},
                {"category": "schematic", "name": "Panel-MCC2", "count": 1, "subgraph_id": "sg_1"},
            ]
        }


# ─── Step 4: aggregate ────────────────────────────────────────────────────────

_UNIT_PRICES = {
    "Office Block A": 500_000.0,
    "Warehouse B":    200_000.0,
    "YJV-4x16":          45.0,
    "YJV-3x10":          32.0,
    "Panel-MCC1":     15_000.0,
    "Panel-MCC2":     12_000.0,
}


class MergeAndDedupTask(BaseTask):
    """Merge all recognition results and compute cost summary — async."""
    OutputModel = MergeOutput

    async def execute(self, inputs: dict[str, Any], progress) -> dict[str, Any]:
        _check_fail(self.task_id)
        steps = 10
        for i in range(steps):
            await asyncio.sleep(_sleep_scale(0.2))
            await progress(int((i + 1) / steps * 100))

        # Gather items from all recognition tasks via cross-step input
        all_items: list[dict] = []
        recognize_data = inputs.get("recognize", {})
        for task_output in recognize_data.values():
            all_items.extend(task_output.get("items", []))

        # Deduplicate by (category, name), summing counts
        merged: dict[tuple[str, str], int] = {}
        for item in all_items:
            key = (item["category"], item["name"])
            merged[key] = merged.get(key, 0) + item["count"]

        summary = []
        grand_total = 0.0
        for (category, name), count in sorted(merged.items()):
            unit_price = _UNIT_PRICES.get(name, 0.0)
            subtotal = unit_price * count
            grand_total += subtotal
            summary.append({
                "category": category,
                "name": name,
                "total_count": count,
                "unit_price": unit_price,
                "subtotal": subtotal,
            })

        return {"summary": summary, "grand_total": grand_total}
