"""Batch job storage.

The interface is deliberately made of *atomic operations* (`transition`, `update_row`) rather than
get-mutate-save. Twenty row tasks update the same job concurrently, and two HTTP requests may race
to resume it. A load/modify/store API would lose updates as soon as the store is out of process
(Redis, Postgres). Each operation here maps onto one atomic primitive in those stores:

* `transition`  -> `UPDATE jobs SET status=... WHERE id=... AND status IN (...)` / a Lua script
* `update_row`  -> `UPDATE job_rows SET ... WHERE job_id=... AND row=...` / `HSET`

`get` returns a snapshot (deep copy), so callers can never mutate stored state by accident. That is
also what an out-of-process store would give them.
"""

import threading
from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from app.errors import BatchNotFoundError, InvalidStateError
from app.models.domain import BatchJob, JobStatus, utcnow

RowUpdates = Mapping[int, Mapping[str, Any]]


class BatchRepository(ABC):
    @abstractmethod
    async def add(self, job: BatchJob) -> None: ...

    @abstractmethod
    async def get(self, batch_id: UUID) -> BatchJob | None: ...

    @abstractmethod
    async def transition(
        self,
        batch_id: UUID,
        target: JobStatus,
        *,
        row_updates: RowUpdates | None = None,
        **fields: Any,
    ) -> tuple[BatchJob, JobStatus]:
        """Atomically move a job to `target` if the state machine allows it.

        In the same atomic step, sets extra job `fields` and applies `row_updates`, so observers
        never see e.g. a `completed` job whose rows are not yet `created_and_activated`.
        Returns `(new snapshot, previous status)`. Raises `InvalidStateError` if the transition
        is not allowed from the current status.
        """

    @abstractmethod
    async def update_rows(self, batch_id: UUID, updates: RowUpdates) -> BatchJob:
        """Atomically update several rows (`{row_number: {field: value}}`); returns a snapshot."""

    async def update_row(self, batch_id: UUID, row_number: int, **fields: Any) -> BatchJob:
        return await self.update_rows(batch_id, {row_number: fields})

    async def require(self, batch_id: UUID) -> BatchJob:
        job = await self.get(batch_id)
        if job is None:
            raise BatchNotFoundError(f"Batch {batch_id} not found.")
        return job


class InMemoryBatchRepository(BatchRepository):
    """Process-local store. Thread- and asyncio-safe.

    A `threading.Lock` (not `asyncio.Lock`) guards the dict: critical sections never `await`, so
    holding it cannot block the event loop for long, and it also protects against access from
    other threads (e.g. sync endpoints running in the threadpool). Bounded: once more than
    `max_batches` jobs are stored, the oldest *finished* jobs are evicted.
    """

    def __init__(self, max_batches: int = 1000) -> None:
        self._jobs: OrderedDict[UUID, BatchJob] = OrderedDict()
        self._lock = threading.Lock()
        self._max_batches = max_batches

    async def add(self, job: BatchJob) -> None:
        with self._lock:
            if job.batch_id in self._jobs:
                raise ValueError(f"batch {job.batch_id} already exists")
            self._jobs[job.batch_id] = job.model_copy(deep=True)
            self._evict_locked()

    async def get(self, batch_id: UUID) -> BatchJob | None:
        with self._lock:
            job = self._jobs.get(batch_id)
            return job.model_copy(deep=True) if job else None

    async def transition(
        self,
        batch_id: UUID,
        target: JobStatus,
        *,
        row_updates: RowUpdates | None = None,
        **fields: Any,
    ) -> tuple[BatchJob, JobStatus]:
        with self._lock:
            job = self._require_locked(batch_id)
            previous = job.status
            if not previous.can_transition_to(target):
                raise InvalidStateError(
                    f"Batch {batch_id} is '{previous}'; cannot move to '{target}'.",
                    details={"current_status": previous, "requested_status": target},
                )
            for key, value in fields.items():
                setattr(job, key, value)
            self._apply_row_updates(job, row_updates or {})
            job.status = target
            job.updated_at = utcnow()
            return job.model_copy(deep=True), previous

    async def update_rows(self, batch_id: UUID, updates: RowUpdates) -> BatchJob:
        with self._lock:
            job = self._require_locked(batch_id)
            self._apply_row_updates(job, updates)
            job.updated_at = utcnow()
            return job.model_copy(deep=True)

    @staticmethod
    def _apply_row_updates(job: BatchJob, updates: RowUpdates) -> None:
        for row_number, row_fields in updates.items():
            row = job.row(row_number)
            for key, value in row_fields.items():
                setattr(row, key, value)

    def _require_locked(self, batch_id: UUID) -> BatchJob:
        job = self._jobs.get(batch_id)
        if job is None:
            raise BatchNotFoundError(f"Batch {batch_id} not found.")
        return job

    def _evict_locked(self) -> None:
        if len(self._jobs) <= self._max_batches:
            return
        for batch_id in [b for b, j in self._jobs.items() if j.status.is_terminal]:
            if len(self._jobs) <= self._max_batches:
                break
            del self._jobs[batch_id]
