"""In-process pub/sub for batch progress events (feeds the WebSocket endpoint).

Events carry *state*, not deltas (a `row` event holds the row's full current state), so a
subscriber that also reads a snapshot can apply events in any order without double-counting.
With several app instances this would become Redis pub/sub on a `batch:{id}` channel; the
publisher API would not change.
"""

import asyncio
import logging
from collections import defaultdict
from typing import Any
from uuid import UUID

from app.models.domain import BatchJob
from app.models.schemas import BatchResult, HospitalResult

logger = logging.getLogger(__name__)

Event = dict[str, Any]


class ProgressBroker:
    def __init__(self, queue_size: int = 1000) -> None:
        self._subscribers: defaultdict[UUID, set[asyncio.Queue[Event]]] = defaultdict(set)
        self._queue_size = queue_size

    def subscribe(self, batch_id: UUID) -> asyncio.Queue[Event]:
        queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=self._queue_size)
        self._subscribers[batch_id].add(queue)
        return queue

    def unsubscribe(self, batch_id: UUID, queue: asyncio.Queue[Event]) -> None:
        subscribers = self._subscribers.get(batch_id)
        if subscribers is None:
            return
        subscribers.discard(queue)
        if not subscribers:
            del self._subscribers[batch_id]

    def subscriber_count(self, batch_id: UUID) -> int:
        return len(self._subscribers.get(batch_id, ()))

    def publish(self, batch_id: UUID, event: Event) -> None:
        for queue in self._subscribers.get(batch_id, ()):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:  # a stuck consumer must never block processing
                logger.warning("progress subscriber queue full; dropping event")


def _counts(job: BatchJob) -> dict[str, int]:
    return job.counts().model_dump()


def job_event(job: BatchJob) -> Event:
    """Job status changed. Terminal events include the full result."""
    return {
        "type": "job",
        "batch_id": str(job.batch_id),
        "status": job.status.value,
        "counts": _counts(job),
        "result": BatchResult.from_job(job).model_dump(mode="json")
        if job.status.is_terminal
        else None,
    }


def row_event(job: BatchJob, row_number: int) -> Event:
    row = job.row(row_number)
    return {
        "type": "row",
        "batch_id": str(job.batch_id),
        "row": HospitalResult(
            row=row.row,
            hospital_id=row.hospital_id,
            name=row.name,
            status=row.status,
            error=row.error,
            attempts=row.attempts,
        ).model_dump(mode="json"),
        "counts": _counts(job),
    }


def snapshot_event(job: BatchJob) -> Event:
    return {
        "type": "snapshot",
        "batch_id": str(job.batch_id),
        "status": job.status.value,
        "result": BatchResult.from_job(job).model_dump(mode="json"),
    }
