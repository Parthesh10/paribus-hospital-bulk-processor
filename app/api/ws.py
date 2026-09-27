"""WebSocket progress stream: `/ws/bulk/{batch_id}`.

Protocol (server -> client JSON messages):
* `snapshot`: sent first; the full current `BatchResult`;
* `row`: a row changed state (`row` holds its full state, plus aggregate `counts`);
* `job`: the batch status changed; when terminal it includes `result` and the server closes.

Subscribing *before* reading the snapshot guarantees no event is lost between the two. Events are
state-carrying, so any overlap with the snapshot is harmless.
"""

import asyncio
import contextlib
from uuid import UUID

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status

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

        async def pump() -> None:
            while True:
                event = await queue.get()
                await websocket.send_json(event)
                if _is_terminal(event):
                    return

        async def watch_disconnect() -> None:
            # Detect a client that goes away while no events are flowing.
            while (await websocket.receive())["type"] != "websocket.disconnect":
                pass

        tasks = {asyncio.create_task(pump()), asyncio.create_task(watch_disconnect())}
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            task.result()  # surface unexpected errors
        with contextlib.suppress(RuntimeError, WebSocketDisconnect):
            await websocket.close()
    except WebSocketDisconnect:
        pass
    finally:
        services.broker.unsubscribe(batch_id, queue)
