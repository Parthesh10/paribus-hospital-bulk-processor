"""Bulk batch orchestration: create rows concurrently, reconcile, activate, resume, roll back.

Delivery semantics. Upstream `POST /hospitals/` is not idempotent, and we retry timeouts/5xx, so
a create that succeeded upstream but whose response was lost can be sent twice (at-least-once).
Before every activation (and at the start of every resume) the batch is *reconciled* against
`GET /hospitals/batch/{id}`, the source of truth:

* upstream records nobody claims are matched to failed rows by content and adopted;
* leftovers are true duplicates and are deleted before activation;
* rows we believe exist but upstream no longer has (the free-tier upstream keeps data in memory
  and loses it on restart) are marked failed, so resume re-creates them.

At-least-once delivery plus reconciliation gives effectively-once results.
"""

import asyncio
import logging
from collections.abc import Sequence
from typing import Any
from uuid import UUID, uuid4

from app.clients.hospital_directory import HospitalDirectoryClient, UpstreamError
from app.errors import UpstreamUnavailableError
from app.logging_config import batch_id_var
from app.models.domain import BatchJob, HospitalRow, JobStatus, RowStatus, fingerprint, utcnow
from app.models.upstream import HospitalCreate
from app.repositories.batch_repository import BatchRepository
from app.services.csv_validator import ParsedRow
from app.services.job_runner import JobRunner
from app.services.progress import ProgressBroker, job_event, row_event

logger = logging.getLogger(__name__)

_RETRY_STATUSES = frozenset({RowStatus.PENDING, RowStatus.IN_PROGRESS, RowStatus.FAILED})


class BulkProcessor:
    def __init__(
        self,
        client: HospitalDirectoryClient,
        repository: BatchRepository,
        broker: ProgressBroker,
        runner: JobRunner,
    ) -> None:
        self._client = client
        self._repo = repository
        self._broker = broker
        self._runner = runner

    # --- commands ------------------------------------------------------------------------------

    async def create_batch(self, parsed_rows: Sequence[ParsedRow]) -> BatchJob:
        """Register a new job (status `queued`). Invalid rows are kept as `skipped_invalid`."""
        rows = [
            HospitalRow(
                row=p.row,
                name=p.name,
                address=p.address,
                phone=p.phone,
                status=RowStatus.PENDING if p.is_valid else RowStatus.SKIPPED_INVALID,
                error=None if p.is_valid else "; ".join(e.message for e in p.errors),
            )
            for p in parsed_rows
        ]
        job = BatchJob(batch_id=uuid4(), rows=rows)
        await self._repo.add(job)
        return job

    def start(self, batch_id: UUID) -> asyncio.Task[BatchJob]:
        """Run a queued job in the background. The task survives client disconnects."""
        return self._runner.submit(self.run(batch_id), name=f"batch-{batch_id}")

    async def resume(self, batch_id: UUID) -> tuple[BatchJob, asyncio.Task[BatchJob] | None]:
        """Retry failed rows (or just the activation). Idempotent.

        A completed batch is a no-op (returns `None` for the task). The atomic transition to
        `queued` guarantees that two concurrent resumes cannot both start a run: the loser gets
        `InvalidStateError` (409).
        """
        job = await self._repo.require(batch_id)
        if job.status is JobStatus.COMPLETED:
            return job, None
        job, _ = await self._repo.transition(batch_id, JobStatus.QUEUED)
        self._publish_job(job)
        return job, self.start(batch_id)

    async def rollback(self, batch_id: UUID) -> BatchJob:
        """Delete the batch's hospitals upstream. Idempotent for already rolled-back batches."""
        current = await self._repo.require(batch_id)
        if current.status is JobStatus.ROLLED_BACK:
            return current
        job, previous = await self._repo.transition(batch_id, JobStatus.ROLLING_BACK)
        self._publish_job(job)
        token = batch_id_var.set(str(batch_id))
        try:
            try:
                deleted = await self._client.delete_batch(batch_id)
            except UpstreamError as exc:
                job, _ = await self._repo.transition(batch_id, previous)
                self._publish_job(job)
                raise UpstreamUnavailableError(
                    f"Rollback failed ({exc}). Batch left as '{previous}'; retry later.",
                ) from exc
            row_updates = {
                r.row: {"status": RowStatus.ROLLED_BACK}
                for r in job.rows
                if r.status in (RowStatus.CREATED, RowStatus.CREATED_AND_ACTIVATED)
            }
            job, _ = await self._repo.transition(
                batch_id, JobStatus.ROLLED_BACK, batch_activated=False, row_updates=row_updates
            )
            self._publish_job(job)
            logger.info("batch rolled back", extra={"deleted_count": deleted})
            return job
        finally:
            batch_id_var.reset(token)

    # --- the run -------------------------------------------------------------------------------

    async def run(self, batch_id: UUID) -> BatchJob:
        """One processing run: create outstanding rows, reconcile, activate if complete."""
        token = batch_id_var.set(str(batch_id))
        try:
            queued = await self._repo.require(batch_id)
            job, _ = await self._repo.transition(
                batch_id,
                JobStatus.PROCESSING,
                run_started_at=utcnow(),
                runs=queued.runs + 1,
                activation_error=None,
            )
            self._publish_job(job)
            await self._client.warm_up()

            if job.runs > 1:
                # Resume: first adopt rows whose "failed" create actually landed, and re-queue
                # rows upstream has lost since, so we never create duplicates.
                job = await self._reconcile(batch_id)

            todo = [r for r in job.workable_rows if r.status in _RETRY_STATUSES]
            logger.info(
                "batch run started",
                extra={"run": job.runs, "rows_to_create": len(todo), "rows": len(job.rows)},
            )
            await asyncio.gather(*(self._create_row(batch_id, row) for row in todo))
            return await self._finish(batch_id)
        except asyncio.CancelledError:
            await self._abort(batch_id, "Interrupted before completion (service shutdown).")
            raise
        except Exception:
            logger.exception("batch run crashed")
            await self._abort(batch_id, "Internal error during processing.")
            return await self._repo.require(batch_id)
        finally:
            batch_id_var.reset(token)

    async def _create_row(self, batch_id: UUID, row: HospitalRow) -> None:
        job = await self._repo.update_row(
            batch_id,
            row.row,
            status=RowStatus.IN_PROGRESS,
            attempts=row.attempts + 1,
            error=None,
            retryable=None,
        )
        self._publish_row(job, row.row)
        payload = HospitalCreate(
            name=row.name, address=row.address, phone=row.phone, creation_batch_id=batch_id
        )
        fields: dict[str, Any]
        try:
            hospital = await self._client.create_hospital(payload)
        except UpstreamError as exc:
            logger.warning("row failed", extra={"row": row.row, "error": str(exc)})
            fields = {"status": RowStatus.FAILED, "error": str(exc), "retryable": exc.retryable}
        except Exception:  # a bug must fail one row, not strand its siblings mid-flight
            logger.exception("unexpected error creating row", extra={"row": row.row})
            fields = {
                "status": RowStatus.FAILED,
                "error": "Internal error while creating hospital.",
                "retryable": True,
            }
        else:
            fields = {"status": RowStatus.CREATED, "hospital_id": hospital.id}
        job = await self._repo.update_row(batch_id, row.row, **fields)
        self._publish_row(job, row.row)

    async def _finish(self, batch_id: UUID) -> BatchJob:
        job = await self._reconcile(batch_id)
        if any(r.status is not RowStatus.CREATED for r in job.workable_rows):
            # Failure policy: never activate a partial batch. It stays inactive (invisible to
            # consumers), and the caller chooses to resume or roll back.
            return await self._end_run(job, JobStatus.PARTIAL_FAILURE)

        activated, error = await self._activate(batch_id)
        if not activated:
            return await self._end_run(job, JobStatus.ACTIVATION_FAILED, activation_error=error)
        activated_rows = {"status": RowStatus.CREATED_AND_ACTIVATED}
        row_updates = {r.row: activated_rows for r in job.workable_rows}
        return await self._end_run(
            job, JobStatus.COMPLETED, batch_activated=True, row_updates=row_updates
        )

    async def _activate(self, batch_id: UUID) -> tuple[bool, str | None]:
        try:
            count = await self._client.activate_batch(batch_id)
        except UpstreamError as exc:
            # Upstream activation is not idempotent: it returns 400 if anything in the batch is
            # already active. So a PATCH that was applied but whose response was lost (then
            # retried) looks like a failure. Ask upstream what actually happened.
            try:
                hospitals = await self._client.get_batch(batch_id)
            except UpstreamError:
                logger.error("activation failed", extra={"error": str(exc)})
                return False, str(exc)
            if hospitals and all(h.active for h in hospitals):
                logger.info("activation errored but batch is active upstream; treating as done")
                return True, None
            logger.error("activation failed", extra={"error": str(exc)})
            return False, str(exc)
        logger.info("batch activated", extra={"activated_count": count})
        return True, None

    async def _reconcile(self, batch_id: UUID) -> BatchJob:
        """Align our row records with upstream truth (see the module docstring). Best effort."""
        job = await self._repo.require(batch_id)
        try:
            upstream = await self._client.get_batch(batch_id)
        except UpstreamError as exc:
            logger.warning("reconcile skipped: upstream unavailable", extra={"error": str(exc)})
            return job

        by_id = {h.id: h for h in upstream}
        updates: dict[int, dict[str, Any]] = {}
        claimed: set[int] = set()
        for row in job.workable_rows:
            if row.hospital_id is None:
                continue
            if row.hospital_id in by_id:
                claimed.add(row.hospital_id)
            elif row.status is RowStatus.CREATED:
                updates[row.row] = {
                    "status": RowStatus.FAILED,
                    "hospital_id": None,
                    "retryable": True,
                    "error": f"Hospital {row.hospital_id} no longer exists upstream "
                    "(upstream data was reset); resume will re-create it.",
                }

        orphans = [h for h in upstream if h.id not in claimed]
        for row in job.workable_rows:
            if not orphans:
                break
            if row.status is not RowStatus.FAILED or row.row in updates:
                continue
            match = next(
                (h for h in orphans if fingerprint(h.name, h.address, h.phone) == row.fingerprint),
                None,
            )
            if match is not None:
                orphans.remove(match)
                updates[row.row] = {
                    "status": RowStatus.CREATED,
                    "hospital_id": match.id,
                    "error": None,
                    "retryable": None,
                }
                logger.info(
                    "adopted upstream record for failed row",
                    extra={"row": row.row, "hospital_id": match.id},
                )

        for orphan in orphans:  # duplicates from retried creates: remove before activation
            try:
                await self._client.delete_hospital(orphan.id)
                logger.warning("deleted duplicate record", extra={"hospital_id": orphan.id})
            except UpstreamError as exc:
                logger.warning(
                    "could not delete duplicate record",
                    extra={"hospital_id": orphan.id, "error": str(exc)},
                )

        if updates:
            job = await self._repo.update_rows(batch_id, updates)
            for row_number in updates:
                self._publish_row(job, row_number)
        return job

    async def _end_run(self, job: BatchJob, status: JobStatus, **fields: Any) -> BatchJob:
        elapsed = (utcnow() - job.run_started_at).total_seconds() if job.run_started_at else 0.0
        job, _ = await self._repo.transition(
            job.batch_id,
            status,
            processing_seconds=job.processing_seconds + elapsed,
            run_started_at=None,
            **fields,
        )
        self._publish_job(job)
        counts = job.counts()
        logger.info(
            "batch run finished",
            extra={
                "status": status.value,
                "processed": counts.processed,
                "failed": counts.failed,
                "skipped": counts.skipped,
                "run_seconds": round(elapsed, 3),
            },
        )
        return job

    async def _abort(self, batch_id: UUID, reason: str) -> None:
        """Leave an interrupted run resumable rather than stuck in `processing`."""
        job = await self._repo.get(batch_id)
        if job is None or job.status is not JobStatus.PROCESSING:
            return
        row_updates = {
            r.row: {"status": RowStatus.FAILED, "error": reason, "retryable": True}
            for r in job.rows
            if r.status in (RowStatus.PENDING, RowStatus.IN_PROGRESS)
        }
        await self._end_run(job, JobStatus.PARTIAL_FAILURE, row_updates=row_updates)

    # --- events --------------------------------------------------------------------------------

    def _publish_job(self, job: BatchJob) -> None:
        self._broker.publish(job.batch_id, job_event(job))

    def _publish_row(self, job: BatchJob, row_number: int) -> None:
        self._broker.publish(job.batch_id, row_event(job, row_number))
