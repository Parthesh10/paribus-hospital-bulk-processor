import asyncio
from uuid import uuid4

import pytest

from app.errors import BatchNotFoundError, InvalidStateError
from app.models.domain import ALLOWED_TRANSITIONS, BatchJob, HospitalRow, JobStatus, RowStatus
from app.models.schemas import BatchResult
from app.repositories.batch_repository import InMemoryBatchRepository


def job_with(*statuses: RowStatus) -> BatchJob:
    return BatchJob(
        batch_id=uuid4(),
        rows=[
            HospitalRow(row=i, name=f"H{i}", address="A", status=s)
            for i, s in enumerate(statuses, start=1)
        ],
    )


# --- state machine -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("source", "target", "allowed"),
    [
        (JobStatus.QUEUED, JobStatus.PROCESSING, True),
        (JobStatus.PROCESSING, JobStatus.COMPLETED, True),
        (JobStatus.PROCESSING, JobStatus.PARTIAL_FAILURE, True),
        (JobStatus.PARTIAL_FAILURE, JobStatus.QUEUED, True),
        (JobStatus.ACTIVATION_FAILED, JobStatus.QUEUED, True),
        (JobStatus.COMPLETED, JobStatus.ROLLING_BACK, True),
        (JobStatus.ROLLING_BACK, JobStatus.COMPLETED, True),  # failed rollback restores
        (JobStatus.COMPLETED, JobStatus.QUEUED, False),  # nothing to resume
        (JobStatus.PROCESSING, JobStatus.QUEUED, False),  # no double runs
        (JobStatus.PROCESSING, JobStatus.ROLLING_BACK, False),  # no rollback mid-run
        (JobStatus.ROLLED_BACK, JobStatus.QUEUED, False),  # terminal
        (JobStatus.QUEUED, JobStatus.COMPLETED, False),
    ],
)
def test_transitions(source: JobStatus, target: JobStatus, allowed: bool) -> None:
    assert source.can_transition_to(target) is allowed


def test_every_status_has_a_transition_entry() -> None:
    assert set(ALLOWED_TRANSITIONS) == set(JobStatus)


def test_terminal_and_resumable_flags() -> None:
    assert {s for s in JobStatus if not s.is_terminal} == {
        JobStatus.QUEUED,
        JobStatus.PROCESSING,
        JobStatus.ROLLING_BACK,
    }
    assert {s for s in JobStatus if s.is_resumable} == {
        JobStatus.PARTIAL_FAILURE,
        JobStatus.ACTIVATION_FAILED,
    }


# --- aggregation ---------------------------------------------------------------------------------


def test_counts_aggregate_row_statuses() -> None:
    job = job_with(
        RowStatus.CREATED_AND_ACTIVATED,
        RowStatus.CREATED,
        RowStatus.FAILED,
        RowStatus.SKIPPED_INVALID,
        RowStatus.PENDING,
        RowStatus.IN_PROGRESS,
        RowStatus.ROLLED_BACK,
    )
    counts = job.counts()
    assert (counts.total, counts.processed, counts.failed, counts.skipped, counts.pending) == (
        7,
        2,
        1,
        1,
        2,
    )
    assert [r.row for r in job.workable_rows] == [1, 2, 3, 5, 6, 7]


def test_batch_result_has_spec_fields() -> None:
    job = job_with(RowStatus.CREATED_AND_ACTIVATED)
    body = BatchResult.from_job(job).model_dump(mode="json")
    spec_keys = {
        "batch_id",
        "total_hospitals",
        "processed_hospitals",
        "failed_hospitals",
        "processing_time_seconds",
        "batch_activated",
        "hospitals",
    }
    assert spec_keys <= body.keys()
    assert {"row", "hospital_id", "name", "status"} <= body["hospitals"][0].keys()


def test_elapsed_includes_live_run() -> None:
    from datetime import timedelta

    job = job_with(RowStatus.PENDING)
    job.processing_seconds = 2.0
    assert job.elapsed_seconds() == 2.0
    job.status = JobStatus.PROCESSING
    job.run_started_at = job.created_at
    assert job.elapsed_seconds(now=job.created_at + timedelta(seconds=3)) == 5.0


# --- repository ----------------------------------------------------------------------------------


async def test_get_returns_isolated_snapshots() -> None:
    repo = InMemoryBatchRepository()
    job = job_with(RowStatus.PENDING)
    await repo.add(job)
    snapshot = await repo.get(job.batch_id)
    assert snapshot is not None
    snapshot.rows[0].status = RowStatus.FAILED
    fresh = await repo.require(job.batch_id)
    assert fresh.rows[0].status is RowStatus.PENDING


async def test_add_rejects_duplicates_and_unknown_ids_raise() -> None:
    repo = InMemoryBatchRepository()
    job = job_with(RowStatus.PENDING)
    await repo.add(job)
    with pytest.raises(ValueError, match="already exists"):
        await repo.add(job)
    with pytest.raises(BatchNotFoundError):
        await repo.require(uuid4())
    with pytest.raises(BatchNotFoundError):
        await repo.update_row(uuid4(), 1, status=RowStatus.FAILED)


async def test_transition_is_compare_and_set() -> None:
    repo = InMemoryBatchRepository()
    job = job_with(RowStatus.FAILED)
    job.status = JobStatus.PARTIAL_FAILURE
    await repo.add(job)

    async def resume() -> bool:
        try:
            await repo.transition(job.batch_id, JobStatus.QUEUED)
        except InvalidStateError:
            return False
        return True

    outcomes = await asyncio.gather(*(resume() for _ in range(10)))
    assert outcomes.count(True) == 1  # exactly one concurrent resume wins


async def test_transition_applies_fields_and_row_updates_atomically() -> None:
    repo = InMemoryBatchRepository()
    job = job_with(RowStatus.CREATED, RowStatus.CREATED)
    job.status = JobStatus.PROCESSING
    await repo.add(job)
    updated, previous = await repo.transition(
        job.batch_id,
        JobStatus.COMPLETED,
        batch_activated=True,
        row_updates={1: {"status": RowStatus.CREATED_AND_ACTIVATED}},
    )
    assert previous is JobStatus.PROCESSING
    assert updated.batch_activated
    assert [r.status for r in updated.rows] == [RowStatus.CREATED_AND_ACTIVATED, RowStatus.CREATED]


async def test_eviction_keeps_active_jobs() -> None:
    repo = InMemoryBatchRepository(max_batches=2)
    active = job_with(RowStatus.PENDING)  # QUEUED: never evicted
    await repo.add(active)
    finished = []
    for _ in range(3):
        j = job_with(RowStatus.CREATED_AND_ACTIVATED)
        j.status = JobStatus.COMPLETED
        finished.append(j)
        await repo.add(j)
    assert await repo.get(active.batch_id) is not None
    assert await repo.get(finished[0].batch_id) is None
    assert await repo.get(finished[-1].batch_id) is not None
