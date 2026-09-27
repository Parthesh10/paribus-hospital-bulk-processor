"""Public request/response schemas of this service (what `/docs` shows)."""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from app.models.domain import BatchJob, JobStatus, RowStatus

# ---------------------------------------------------------------------------------------------
# Bulk results
# ---------------------------------------------------------------------------------------------


class HospitalResult(BaseModel):
    row: int = Field(description="1-based data row number in the uploaded CSV.")
    hospital_id: int | None = Field(description="Upstream hospital id, once created.")
    name: str
    status: RowStatus
    error: str | None = Field(default=None, description="Why the row failed or was skipped.")
    attempts: int = Field(default=0, description="Processing runs that attempted this row.")


class BatchLinks(BaseModel):
    status: str
    websocket: str
    resume: str
    rollback: str

    @classmethod
    def for_batch(cls, batch_id: UUID) -> "BatchLinks":
        # Relative URLs: correct behind any proxy/TLS terminator without scheme guessing.
        base = f"/hospitals/bulk/{batch_id}"
        return cls(
            status=f"{base}/status",
            websocket=f"/ws/bulk/{batch_id}",
            resume=f"{base}/resume",
            rollback=base,
        )


class BatchResult(BaseModel):
    """The spec's bulk response, plus extra (additive) fields for status tracking."""

    batch_id: UUID
    total_hospitals: int
    processed_hospitals: int = Field(description="Rows successfully created upstream.")
    failed_hospitals: int = Field(description="Rows whose upstream creation failed.")
    processing_time_seconds: float
    batch_activated: bool
    hospitals: list[HospitalResult]
    # --- additive fields ---
    status: JobStatus
    skipped_hospitals: int = Field(description="Invalid rows skipped (skip_invalid=true only).")
    pending_hospitals: int = Field(description="Rows not yet processed (live runs only).")
    resumable: bool = Field(description="True if POST .../resume would do useful work.")
    activation_error: str | None = None
    runs: int = Field(description="Number of processing runs (1 + resumes).")
    created_at: datetime
    updated_at: datetime
    links: BatchLinks

    @classmethod
    def from_job(cls, job: BatchJob) -> "BatchResult":
        counts = job.counts()
        return cls(
            batch_id=job.batch_id,
            total_hospitals=counts.total,
            processed_hospitals=counts.processed,
            failed_hospitals=counts.failed,
            processing_time_seconds=job.elapsed_seconds(),
            batch_activated=job.batch_activated,
            hospitals=[
                HospitalResult(
                    row=r.row,
                    hospital_id=r.hospital_id,
                    name=r.name,
                    status=r.status,
                    error=r.error,
                    attempts=r.attempts,
                )
                for r in job.rows
            ],
            status=job.status,
            skipped_hospitals=counts.skipped,
            pending_hospitals=counts.pending,
            resumable=job.status.is_resumable,
            activation_error=job.activation_error,
            runs=job.runs,
            created_at=job.created_at,
            updated_at=job.updated_at,
            links=BatchLinks.for_batch(job.batch_id),
        )


class BatchAccepted(BaseModel):
    """202 response for `?async=true`: work continues in the background."""

    batch_id: UUID
    status: JobStatus
    total_hospitals: int
    links: BatchLinks


# ---------------------------------------------------------------------------------------------
# CSV validation
# ---------------------------------------------------------------------------------------------


class ValidationIssue(BaseModel):
    code: str = Field(description="Stable machine-readable code, e.g. `missing_required_field`.")
    message: str
    row: int | None = Field(default=None, description="Data row number; null for file issues.")
    line: int | None = Field(default=None, description="Physical line number in the file.")
    column: str | None = None


class RowPreview(BaseModel):
    row: int
    line: int
    name: str
    address: str
    phone: str | None
    valid: bool


class ValidationReport(BaseModel):
    valid: bool = Field(description="True when the file can be processed as-is.")
    total_rows: int
    valid_rows: int
    invalid_rows: int
    errors: list[ValidationIssue]
    warnings: list[ValidationIssue]
    rows: list[RowPreview] = Field(description="Parsed, normalised rows (for previews).")


# ---------------------------------------------------------------------------------------------
# Errors & health
# ---------------------------------------------------------------------------------------------


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: Any | None = None


class ErrorResponse(BaseModel):
    """Every non-2xx response from this service has this shape."""

    error: ErrorDetail


class UpstreamHealth(BaseModel):
    base_url: str
    warm: bool
    last_success_at: datetime | None
    reachable: bool | None = Field(
        default=None, description="Only set when `?deep=true` actively pinged upstream."
    )


class HealthResponse(BaseModel):
    status: str
    version: str
    upstream: UpstreamHealth
