"""Bulk endpoints: create, validate, status, resume, rollback."""

import asyncio
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, File, Query, UploadFile, status
from fastapi.responses import JSONResponse

from app.api.deps import ServicesDep
from app.errors import CsvValidationError, PayloadTooLargeError
from app.models.domain import BatchJob
from app.models.schemas import (
    BatchAccepted,
    BatchLinks,
    BatchResult,
    ErrorResponse,
    ValidationReport,
)
from app.services.csv_validator import CsvValidationResult

router = APIRouter(prefix="/hospitals/bulk", tags=["Bulk processing"])

CsvFile = Annotated[
    UploadFile,
    File(description="CSV file with header `name,address,phone` (phone optional), max 20 rows."),
]
AsyncFlag = Annotated[
    bool,
    Query(
        alias="async",
        description="Return `202 Accepted` immediately and process in the background. "
        "Track progress via the status endpoint or the WebSocket in `links`.",
    ),
]

_ERRORS: dict[int | str, dict[str, Any]] = {
    404: {"model": ErrorResponse, "description": "Unknown batch id."},
    409: {"model": ErrorResponse, "description": "Operation not allowed in the current state."},
}

_RESULT_EXAMPLE = {
    "batch_id": "550e8400-e29b-41d4-a716-446655440000",
    "total_hospitals": 2,
    "processed_hospitals": 2,
    "failed_hospitals": 0,
    "processing_time_seconds": 5.72,
    "batch_activated": True,
    "hospitals": [
        {
            "row": 1,
            "hospital_id": 101,
            "name": "General Hospital",
            "status": "created_and_activated",
            "error": None,
            "attempts": 1,
        },
        {
            "row": 2,
            "hospital_id": 102,
            "name": "City Clinic",
            "status": "created_and_activated",
            "error": None,
            "attempts": 1,
        },
    ],
    "status": "completed",
    "skipped_hospitals": 0,
    "pending_hospitals": 0,
    "resumable": False,
    "activation_error": None,
    "runs": 1,
    "created_at": "2026-09-27T10:00:00Z",
    "updated_at": "2026-09-27T10:00:05Z",
    "links": BatchLinks.for_batch(UUID("550e8400-e29b-41d4-a716-446655440000")).model_dump(),
}


async def _read_upload(file: UploadFile, max_bytes: int) -> bytes:
    # Read at most one byte past the limit: enough to detect "too large" without buffering
    # an arbitrarily large upload into memory.
    return await file.read(max_bytes + 1)


def _ensure_processable(result: CsvValidationResult, skip_invalid: bool) -> None:
    report = result.report.model_dump(mode="json")
    if any(e.code == "file_too_large" for e in result.file_errors):
        raise PayloadTooLargeError(result.file_errors[0].message, details=report)
    if result.file_errors:
        raise CsvValidationError("The CSV file cannot be processed.", details=report)
    if not result.valid_rows:
        raise CsvValidationError("The CSV file has no valid rows.", details=report)
    if result.report.invalid_rows and not skip_invalid:
        raise CsvValidationError(
            f"{result.report.invalid_rows} row(s) are invalid. Fix them, or pass "
            "`skip_invalid=true` to process only the valid rows.",
            details=report,
        )


def _accepted(job: BatchJob) -> JSONResponse:
    body = BatchAccepted(
        batch_id=job.batch_id,
        status=job.status,
        total_hospitals=len(job.rows),
        links=BatchLinks.for_batch(job.batch_id),
    )
    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=body.model_dump(mode="json"))


async def _await_run(task: "asyncio.Task[BatchJob]") -> BatchResult:
    # shield(): if the HTTP client disconnects mid-run, the batch keeps going and remains
    # observable via the status endpoint instead of being cancelled halfway.
    return BatchResult.from_job(await asyncio.shield(task))


@router.post(
    "",
    response_model=BatchResult,
    summary="Bulk-create hospitals from a CSV",
    description=(
        "Validates the CSV, creates every row upstream concurrently under one new batch id, "
        "and activates the batch once **all** rows were created.\n\n"
        "* Default: waits for processing to finish and returns the full result (`200`).\n"
        "* `?async=true`: returns `202` immediately with status/WebSocket links.\n"
        "* If any row fails, the batch is **not** activated (`status=partial_failure`); "
        "use `resume` to retry the failed rows, or `DELETE` to roll back.\n"
        "* Rows with validation errors reject the whole file (`422`) unless "
        "`skip_invalid=true`, in which case they are reported as `skipped_invalid`."
    ),
    responses={
        200: {"content": {"application/json": {"example": _RESULT_EXAMPLE}}},
        202: {"model": BatchAccepted, "description": "Accepted; processing in background."},
        413: {"model": ErrorResponse, "description": "File larger than the upload limit."},
        422: {"model": ErrorResponse, "description": "CSV invalid; details = validation report."},
    },
)
async def bulk_create(
    services: ServicesDep,
    file: CsvFile,
    run_async: AsyncFlag = False,
    skip_invalid: Annotated[
        bool, Query(description="Process valid rows and report invalid ones as skipped.")
    ] = False,
) -> Any:
    content = await _read_upload(file, services.settings.max_upload_bytes)
    result = services.validator.validate(
        content, filename=file.filename, content_type=file.content_type
    )
    _ensure_processable(result, skip_invalid)

    job = await services.processor.create_batch(result.rows)
    task = services.processor.start(job.batch_id)
    if run_async:
        return _accepted(job)
    return await _await_run(task)


@router.post(
    "/validate",
    response_model=ValidationReport,
    summary="Validate a CSV without calling upstream",
    description="Runs exactly the validation used by `POST /hospitals/bulk` and returns every "
    "file-level and row-level problem. Always `200`; check `valid`.",
)
async def validate_csv(services: ServicesDep, file: CsvFile) -> ValidationReport:
    content = await _read_upload(file, services.settings.max_upload_bytes)
    return services.validator.validate(
        content, filename=file.filename, content_type=file.content_type
    ).report


@router.get(
    "/{batch_id}/status",
    response_model=BatchResult,
    summary="Batch status and per-row progress (polling)",
    responses={404: _ERRORS[404]},
)
async def get_status(batch_id: UUID, services: ServicesDep) -> BatchResult:
    return BatchResult.from_job(await services.repository.require(batch_id))


@router.post(
    "/{batch_id}/resume",
    response_model=BatchResult,
    summary="Retry failed rows of a batch, then activate",
    description=(
        "Retries only rows in `failed` state under the **same batch id**, then activates if "
        "every row now exists. Before retrying, the batch is reconciled with upstream so rows "
        "whose earlier create actually succeeded are adopted, never duplicated. "
        "For `activation_failed` batches only the activation is retried. "
        "Idempotent: resuming a `completed` batch is a no-op. `409` while a run is in progress."
    ),
    responses={202: {"model": BatchAccepted}, **_ERRORS},
)
async def resume_batch(batch_id: UUID, services: ServicesDep, run_async: AsyncFlag = False) -> Any:
    job, task = await services.processor.resume(batch_id)
    if task is None:
        return BatchResult.from_job(job)
    if run_async:
        return _accepted(job)
    return await _await_run(task)


@router.delete(
    "/{batch_id}",
    response_model=BatchResult,
    summary="Roll back a batch (delete its hospitals upstream)",
    description="Calls upstream `DELETE /hospitals/batch/{batch_id}`. Allowed for "
    "`partial_failure`, `activation_failed` and `completed` batches. Idempotent.",
    responses={502: {"model": ErrorResponse, "description": "Upstream delete failed."}, **_ERRORS},
)
async def rollback_batch(batch_id: UUID, services: ServicesDep) -> BatchResult:
    return BatchResult.from_job(await services.processor.rollback(batch_id))
