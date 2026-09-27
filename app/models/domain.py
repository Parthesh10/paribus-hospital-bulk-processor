"""Domain model: a bulk batch job, its rows, and the job state machine.

A `BatchJob` is the unit of work behind one CSV upload. Its `batch_id` is also the upstream
`creation_batch_id`, so the same id identifies the job here and the hospitals upstream.
"""

from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, Field


def utcnow() -> datetime:
    return datetime.now(UTC)


class RowStatus(StrEnum):
    PENDING = "pending"
    """Queued for creation (not yet attempted in the current run)."""
    IN_PROGRESS = "in_progress"
    """Upstream create request in flight."""
    CREATED = "created"
    """Exists upstream but inactive: the batch has not been activated (yet)."""
    CREATED_AND_ACTIVATED = "created_and_activated"
    """Exists upstream and the batch was activated."""
    FAILED = "failed"
    """Upstream create failed after retries; see `error`. Retried by resume."""
    SKIPPED_INVALID = "skipped_invalid"
    """Row failed CSV validation and was never sent upstream (only with `skip_invalid=true`)."""
    ROLLED_BACK = "rolled_back"
    """Was created upstream, then deleted by an explicit rollback."""


class JobStatus(StrEnum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    """Every row created and the batch activated."""
    PARTIAL_FAILURE = "partial_failure"
    """At least one row failed; batch left inactive. Resumable or roll-back-able."""
    ACTIVATION_FAILED = "activation_failed"
    """All rows created but activation failed; resume retries only the activation."""
    ROLLING_BACK = "rolling_back"
    ROLLED_BACK = "rolled_back"

    @property
    def is_terminal(self) -> bool:
        """No work is running and none will start without an explicit resume/rollback."""
        return self not in _ACTIVE_STATUSES

    @property
    def is_resumable(self) -> bool:
        return self in (JobStatus.PARTIAL_FAILURE, JobStatus.ACTIVATION_FAILED)

    def can_transition_to(self, target: "JobStatus") -> bool:
        return target in ALLOWED_TRANSITIONS[self]


_ACTIVE_STATUSES = frozenset({JobStatus.QUEUED, JobStatus.PROCESSING, JobStatus.ROLLING_BACK})

ALLOWED_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.QUEUED: frozenset({JobStatus.PROCESSING}),
    JobStatus.PROCESSING: frozenset(
        {JobStatus.COMPLETED, JobStatus.PARTIAL_FAILURE, JobStatus.ACTIVATION_FAILED}
    ),
    JobStatus.PARTIAL_FAILURE: frozenset({JobStatus.QUEUED, JobStatus.ROLLING_BACK}),
    JobStatus.ACTIVATION_FAILED: frozenset({JobStatus.QUEUED, JobStatus.ROLLING_BACK}),
    JobStatus.COMPLETED: frozenset({JobStatus.ROLLING_BACK}),
    # A failed rollback restores whichever status the job had before it.
    JobStatus.ROLLING_BACK: frozenset(
        {
            JobStatus.ROLLED_BACK,
            JobStatus.COMPLETED,
            JobStatus.PARTIAL_FAILURE,
            JobStatus.ACTIVATION_FAILED,
        }
    ),
    JobStatus.ROLLED_BACK: frozenset(),
}


class HospitalRow(BaseModel):
    """One CSV data row plus everything we know about its upstream fate."""

    row: int = Field(description="1-based data row number (header and blank lines excluded).")
    name: str
    address: str
    phone: str | None = None
    status: RowStatus = RowStatus.PENDING
    hospital_id: int | None = None
    error: str | None = None
    retryable: bool | None = None
    attempts: int = Field(default=0, description="Processing runs that attempted this row.")

    @property
    def fingerprint(self) -> tuple[str, str, str]:
        return fingerprint(self.name, self.address, self.phone)


def fingerprint(name: str, address: str, phone: str | None) -> tuple[str, str, str]:
    """Content identity used to match upstream records back to CSV rows during reconciliation."""
    return (name.strip().casefold(), address.strip().casefold(), (phone or "").strip())


class JobCounts(BaseModel):
    total: int
    processed: int
    failed: int
    skipped: int
    pending: int


class BatchJob(BaseModel):
    batch_id: UUID
    status: JobStatus = JobStatus.QUEUED
    rows: list[HospitalRow]
    batch_activated: bool = False
    activation_error: str | None = None
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    run_started_at: datetime | None = None
    runs: int = 0
    processing_seconds: float = Field(
        default=0.0, description="Wall time of all finished runs (the current run is added live)."
    )

    def counts(self) -> JobCounts:
        processed = failed = skipped = pending = 0
        for r in self.rows:
            if r.status in (RowStatus.CREATED, RowStatus.CREATED_AND_ACTIVATED):
                processed += 1
            elif r.status is RowStatus.FAILED:
                failed += 1
            elif r.status is RowStatus.SKIPPED_INVALID:
                skipped += 1
            elif r.status in (RowStatus.PENDING, RowStatus.IN_PROGRESS):
                pending += 1
        return JobCounts(
            total=len(self.rows),
            processed=processed,
            failed=failed,
            skipped=skipped,
            pending=pending,
        )

    def elapsed_seconds(self, now: datetime | None = None) -> float:
        total = self.processing_seconds
        if self.status is JobStatus.PROCESSING and self.run_started_at is not None:
            total += ((now or utcnow()) - self.run_started_at).total_seconds()
        return round(total, 3)

    def row(self, row_number: int) -> HospitalRow:
        # Rows are stored in order and numbered from 1, so this is O(1).
        found = self.rows[row_number - 1]
        assert found.row == row_number, "rows must be stored in row order"
        return found

    @property
    def workable_rows(self) -> list[HospitalRow]:
        """Rows that belong upstream (everything except rows skipped by validation)."""
        return [r for r in self.rows if r.status is not RowStatus.SKIPPED_INVALID]
