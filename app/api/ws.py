"""WebSocket progress stream: `/ws/bulk/{batch_id}`.

Protocol (server -> client JSON messages):
* `snapshot`: sent first; the full current `BatchResult`;
* `row`: a row changed state (`row` holds its full state, plus aggregate `counts`);
* `job`: the batch status changed; when terminal it includes `result` and the server closes.

Subscribing *before* reading the snapshot guarantees no event is lost between the two. Events are
state-carrying, so any overlap with the snapshot is harmless.
"""

import contextlib
from uuid import UUID

import anyio
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status
from starlette.websockets import WebSocketState

from app.api.deps import ServicesDep
from app.services.progress import Event, snapshot_event

router = APIRouter(tags=["Progress"])


def _is_terminal(event: Event) -> bool:
    return event["type"] == "job" and event.get("result") is not None


@router.websocket("/ws/bulk/{batch_id}")
async def batch_progress(websocket: WebSocket, batch_id: UUID, services: ServicesDep) -> None:
    await websocket.accept()
    queue = services.broker.subscribe(batch_id)
    try:
        job = await services.repository.get(batch_id)
        if job is None:
            await websocket.send_json(
                {"type": "error", "code": "batch_not_found", "message": f"Batch {batch_id}"}
            )
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        await websocket.send_json(snapshot_event(job))
        if job.status.is_terminal:
            await websocket.close()
            return

        # Structured concurrency: both children live inside the task group, so if either
        # finishes, or the connection handler itself is cancelled, the other is cancelled too.
        # No orphaned tasks.
        async with anyio.create_task_group() as tg:

            async def pump() -> None:
                while True:
                    event = await queue.get()
                    await websocket.send_json(event)
                    if _is_terminal(event):
                        break
                tg.cancel_scope.cancel()

            async def watch_disconnect() -> None:
                # Detect a client that goes away while no events are flowing.
                while (await websocket.receive())["type"] != "websocket.disconnect":
                    pass
                tg.cancel_scope.cancel()

            tg.start_soon(pump)
            tg.start_soon(watch_disconnect)

        if websocket.application_state is WebSocketState.CONNECTED:
            with contextlib.suppress(RuntimeError, WebSocketDisconnect):
                await websocket.close()
    except WebSocketDisconnect:
        pass
    finally:
        services.broker.unsubscribe(batch_id, queue)
