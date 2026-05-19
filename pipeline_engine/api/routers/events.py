"""SSE endpoint: GET /runs/{run_id}/events

Streams state-change events for a given run using Server-Sent Events.
The client receives task_update and pipeline_update events until the run
reaches a terminal state (success / failed / paused), at which point a
final 'terminal' event is sent and the stream closes.

Event format (text/event-stream):
    event: task_update
    data: {"step_id":"...","task_id":"...","status":"running","progress":42}

    event: pipeline_update
    data: {"status": "success"}

    event: terminal
    data: {"status": "success"}
"""
from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from pipeline_engine.service import PipelineService

router = APIRouter()

_TERMINAL_STATUSES = frozenset({"success", "failed", "paused", "fixed", "skipped"})


@router.get(
    "/runs/{run_id}/events",
    summary="Stream run state-change events (SSE)",
    response_class=StreamingResponse,
)
async def run_events(run_id: str, request: Request):
    svc: PipelineService = request.app.state.svc
    rm = svc.rm

    # Resolve the run and get its state_manager
    try:
        ctx = rm._resolve_run(run_id)
    except Exception as exc:
        # Capture message before Python deletes exc at end of except block
        _msg = str(exc)
        async def _err():
            yield f"event: error\ndata: {json.dumps({'message': _msg})}\n\n"
        return StreamingResponse(_err(), media_type="text/event-stream")

    sm = ctx.state_manager

    async def _stream():
        # Subscribe inside the generator so the finally block always runs,
        # preventing queue leak when the client disconnects before first yield (H2 fix).
        q = sm.subscribe()
        try:
            # Check whether the run is already in a terminal state before streaming
            state = await sm.get_run_state()
            if state.status.value in _TERMINAL_STATUSES:
                yield (
                    f"event: terminal\n"
                    f"data: {json.dumps({'status': state.status.value})}\n\n"
                )
                return

            while True:
                # Check if the client disconnected
                if await request.is_disconnected():
                    break

                try:
                    event = await asyncio.wait_for(q.get(), timeout=25.0)
                except asyncio.TimeoutError:
                    # Heartbeat to keep the connection alive
                    yield ": heartbeat\n\n"
                    continue

                event_type = event.get("type", "update")
                yield f"event: {event_type}\ndata: {json.dumps(event)}\n\n"

                # Close the stream once the pipeline reaches a terminal state
                if event_type == "pipeline_update" and event.get("status") in _TERMINAL_STATUSES:
                    yield (
                        f"event: terminal\n"
                        f"data: {json.dumps({'status': event['status']})}\n\n"
                    )
                    break
        finally:
            sm.unsubscribe(q)

    return StreamingResponse(_stream(), media_type="text/event-stream")
