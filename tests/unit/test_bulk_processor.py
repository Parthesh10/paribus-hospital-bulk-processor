"""BulkProcessor against the stateful FakeUpstream: success, failures, reconciliation, resume."""

import asyncio

import httpx
import pytest

from app.errors import InvalidStateError, UpstreamUnavailableError
from app.models.domain import BatchJob, JobStatus, RowStatus
from tests.conftest import Stack, csv_bytes, make_rows
from tests.fake_upstream import COMMIT_THEN_TIMEOUT, PASS


async def new_job(stack: Stack, n: int = 3, *rows: str) -> BatchJob:
    content = csv_bytes(*(rows or make_rows(n)))
    return await stack.processor.create_batch(stack.parse(content))


def statuses(job: BatchJob) -> list[RowStatus]:
    return [r.status for r in job.rows]


async def test_full_success_creates_concurrently_and_activates(stack: Stack) -> None:
    job = await new_job(stack, 5)
    result = await stack.processor.run(job.batch_id)

    assert result.status is JobStatus.COMPLETED
    assert result.batch_activated
    assert statuses(result) == [RowStatus.CREATED_AND_ACTIVATED] * 5
    assert all(r.hospital_id for r in result.rows)
    upstream = stack.fake.batch(job.batch_id)
    assert len(upstream) == 5
    assert all(h["active"] for h in upstream)
    assert stack.fake.count("PATCH", "/hospitals/batch") == 1
    assert result.runs == 1
    assert result.processing_seconds >= 0


async def test_partial_failure_never_activates(stack: Stack) -> None:
    stack.fake.fail_always("create:Hospital 2", 500)
    job = await new_job(stack, 3)
    result = await stack.processor.run(job.batch_id)

    assert result.status is JobStatus.PARTIAL_FAILURE
    assert not result.batch_activated
    assert statuses(result) == [RowStatus.CREATED, RowStatus.FAILED, RowStatus.CREATED]
    failed = result.rows[1]
    assert failed.retryable
    assert "500" in (failed.error or "")
    assert stack.fake.count("PATCH", "/hospitals/batch") == 0
    assert not any(h["active"] for h in stack.fake.batch(job.batch_id))
    assert result.counts().failed == 1


async def test_upstream_validation_error_is_not_retried(stack: Stack) -> None:
    stack.fake.fail("create:Hospital 1", 422)
    job = await new_job(stack, 1)
    result = await stack.processor.run(job.batch_id)
    assert result.rows[0].status is RowStatus.FAILED
    assert result.rows[0].retryable is False
    assert stack.fake.count("POST", "/hospitals/") == 1


async def test_transient_errors_are_retried_transparently(stack: Stack) -> None:
    stack.fake.fail("create:Hospital 1", 503, httpx.ConnectError("reset"))
    stack.fake.fail("create:Hospital 2", httpx.ReadTimeout("slow"))
    job = await new_job(stack, 2)
    result = await stack.processor.run(job.batch_id)
    assert result.status is JobStatus.COMPLETED
    assert stack.fake.count("POST", "/hospitals/") == 5  # 3 + 2 attempts


async def test_lost_create_response_is_deduplicated(stack: Stack) -> None:
    # Attempt 1 is committed upstream but the response is lost; attempt 2 creates a second copy.
    stack.fake.fail("create:Hospital 1", COMMIT_THEN_TIMEOUT)
    job = await new_job(stack, 2)
    result = await stack.processor.run(job.batch_id)

    assert result.status is JobStatus.COMPLETED
    upstream = stack.fake.batch(job.batch_id)
    assert len(upstream) == 2  # the orphaned duplicate was deleted before activation
    assert {h["id"] for h in upstream} == {r.hospital_id for r in result.rows}
    assert stack.fake.count("DELETE", "/hospitals/") == 1


async def test_row_that_failed_ambiguously_adopts_its_upstream_record(stack: Stack) -> None:
    # Every attempt commits but loses its response: 3 upstream copies, row marked failed.
    stack.fake.fail("create:Hospital 1", *[COMMIT_THEN_TIMEOUT] * 3)
    job = await new_job(stack, 2)
    result = await stack.processor.run(job.batch_id)

    assert result.status is JobStatus.COMPLETED  # adopted one copy, deleted two
    assert len(stack.fake.batch(job.batch_id)) == 2
    assert result.rows[0].status is RowStatus.CREATED_AND_ACTIVATED
    assert result.rows[0].hospital_id is not None


async def test_activation_failure_then_resume_activates(stack: Stack) -> None:
    stack.fake.fail("activate", 500, 500, 500)
    job = await new_job(stack, 2)
    result = await stack.processor.run(job.batch_id)
    assert result.status is JobStatus.ACTIVATION_FAILED
    assert result.activation_error
    assert "500" in result.activation_error
    assert statuses(result) == [RowStatus.CREATED] * 2

    resumed, task = await stack.processor.resume(job.batch_id)
    assert resumed.status is JobStatus.QUEUED
    assert task is not None
    final = await task
    assert final.status is JobStatus.COMPLETED
    assert final.activation_error is None
    assert stack.fake.count("POST", "/hospitals/") == 2  # nothing re-created


async def test_activation_applied_but_response_lost_is_detected(stack: Stack) -> None:
    # PATCH commits then times out; the retry gets 400 "already active". The processor must
    # recognise the batch is in fact active instead of reporting failure.
    stack.fake.fail("activate", COMMIT_THEN_TIMEOUT)
    job = await new_job(stack, 2)
    result = await stack.processor.run(job.batch_id)
    assert result.status is JobStatus.COMPLETED
    assert result.batch_activated


async def test_activation_failure_with_unreachable_verification(stack: Stack) -> None:
    stack.fake.fail_always("activate", 500)
    # Call 1 is the pre-activation reconciliation; the verification calls after it fail too.
    stack.fake.fail("get_batch", PASS, 503, 503, 503)
    job = await new_job(stack, 1)
    result = await stack.processor.run(job.batch_id)
    assert result.status is JobStatus.ACTIVATION_FAILED
    assert result.status.is_resumable


async def test_resume_retries_only_failed_rows_without_duplicates(stack: Stack) -> None:
    stack.fake.fail_always("create:Hospital 2", 503)
    job = await new_job(stack, 3)
    first = await stack.processor.run(job.batch_id)
    assert first.status is JobStatus.PARTIAL_FAILURE
    posts_after_first = stack.fake.count("POST", "/hospitals/")

    stack.fake.heal()
    _, task = await stack.processor.resume(job.batch_id)
    assert task is not None
    final = await task

    assert final.status is JobStatus.COMPLETED
    assert final.runs == 2
    assert stack.fake.count("POST", "/hospitals/") == posts_after_first + 1
    assert len(stack.fake.batch(job.batch_id)) == 3
    assert [r.attempts for r in final.rows] == [1, 2, 1]
    # The same batch id was used throughout.
    assert {h["creation_batch_id"] for h in stack.fake.batch(job.batch_id)} == {str(job.batch_id)}


async def test_resume_adopts_rows_whose_create_actually_landed(stack: Stack) -> None:
    stack.fake.fail("create:Hospital 1", *[COMMIT_THEN_TIMEOUT] * 3)
    stack.fake.fail("get_batch", 503, 503, 503)  # end-of-run reconciliation can't run
    job = await new_job(stack, 1)
    first = await stack.processor.run(job.batch_id)
    assert first.status is JobStatus.PARTIAL_FAILURE
    assert len(stack.fake.batch(job.batch_id)) == 3

    _, task = await stack.processor.resume(job.batch_id)
    assert task is not None
    final = await task
    assert final.status is JobStatus.COMPLETED
    assert len(stack.fake.batch(job.batch_id)) == 1  # adopted one, deleted two duplicates
    assert stack.fake.count("POST", "/hospitals/") == 3  # resume created nothing new


async def test_resume_recreates_rows_lost_by_an_upstream_restart(stack: Stack) -> None:
    stack.fake.fail_always("create:Hospital 3", 500)
    job = await new_job(stack, 3)
    await stack.processor.run(job.batch_id)
    stack.fake.wipe()  # free-tier upstream restarted: in-memory data gone
    stack.fake.heal()

    _, task = await stack.processor.resume(job.batch_id)
    assert task is not None
    final = await task
    assert final.status is JobStatus.COMPLETED
    assert len(stack.fake.batch(job.batch_id)) == 3
    assert {r.hospital_id for r in final.rows} == {h["id"] for h in stack.fake.batch(job.batch_id)}


async def test_resume_is_idempotent_and_guarded(stack: Stack) -> None:
    job = await new_job(stack, 1)
    completed = await stack.processor.run(job.batch_id)
    same, task = await stack.processor.resume(job.batch_id)
    assert task is None
    assert same.status is JobStatus.COMPLETED == completed.status

    await stack.processor.rollback(job.batch_id)
    with pytest.raises(InvalidStateError):
        await stack.processor.resume(job.batch_id)


async def test_concurrent_resumes_start_exactly_one_run(stack: Stack) -> None:
    stack.fake.fail("create:Hospital 1", 500, 500, 500)
    job = await new_job(stack, 1)
    await stack.processor.run(job.batch_id)
    outcomes = await asyncio.gather(
        *(stack.processor.resume(job.batch_id) for _ in range(5)), return_exceptions=True
    )
    started = [o for o in outcomes if isinstance(o, tuple)]
    rejected = [o for o in outcomes if isinstance(o, InvalidStateError)]
    assert (len(started), len(rejected)) == (1, 4)
    assert started[0][1] is not None
    await started[0][1]


async def test_rollback_deletes_upstream_and_is_idempotent(stack: Stack) -> None:
    stack.fake.fail_always("create:Hospital 2", 500)
    job = await new_job(stack, 2)
    await stack.processor.run(job.batch_id)

    rolled = await stack.processor.rollback(job.batch_id)
    assert rolled.status is JobStatus.ROLLED_BACK
    assert statuses(rolled) == [RowStatus.ROLLED_BACK, RowStatus.FAILED]
    assert stack.fake.batch(job.batch_id) == []
    again = await stack.processor.rollback(job.batch_id)
    assert again.status is JobStatus.ROLLED_BACK
    assert stack.fake.count("DELETE", "/hospitals/batch") == 1


async def test_failed_rollback_restores_previous_status(stack: Stack) -> None:
    job = await new_job(stack, 1)
    await stack.processor.run(job.batch_id)
    stack.fake.fail_always("delete_batch", 503)
    with pytest.raises(UpstreamUnavailableError):
        await stack.processor.rollback(job.batch_id)
    restored = await stack.repo.require(job.batch_id)
    assert restored.status is JobStatus.COMPLETED


async def test_rollback_rejected_while_processing(stack: Stack) -> None:
    job = await new_job(stack, 1)
    await stack.repo.transition(job.batch_id, JobStatus.PROCESSING)
    with pytest.raises(InvalidStateError):
        await stack.processor.rollback(job.batch_id)


async def test_skipped_invalid_rows_are_not_sent(stack: Stack) -> None:
    content = csv_bytes("Good,1 Main St,555-0100", ",2 Main St,", "Also Good,3 Main St,")
    job = await stack.processor.create_batch(stack.parse(content))
    result = await stack.processor.run(job.batch_id)
    assert statuses(result) == [
        RowStatus.CREATED_AND_ACTIVATED,
        RowStatus.SKIPPED_INVALID,
        RowStatus.CREATED_AND_ACTIVATED,
    ]
    assert "required" in (result.rows[1].error or "")
    assert result.status is JobStatus.COMPLETED
    assert stack.fake.count("POST", "/hospitals/") == 2


async def test_duplicate_deletion_failure_is_tolerated(stack: Stack) -> None:
    stack.fake.fail("create:Hospital 1", COMMIT_THEN_TIMEOUT)
    stack.fake.fail_always("delete_hospital", 500)
    job = await new_job(stack, 1)
    result = await stack.processor.run(job.batch_id)
    assert result.status is JobStatus.COMPLETED  # logged, not fatal


async def test_cancellation_leaves_batch_resumable(stack: Stack) -> None:
    import threading

    stack.fake.create_gate = threading.Event()  # creates block until released
    job = await new_job(stack, 2)
    task = stack.processor.start(job.batch_id)
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    interrupted = await stack.repo.require(job.batch_id)
    assert interrupted.status is JobStatus.PARTIAL_FAILURE
    assert all(r.status is RowStatus.FAILED and r.retryable for r in interrupted.rows)
    assert "Interrupted" in (interrupted.rows[0].error or "")


async def test_unexpected_error_in_a_row_fails_only_that_row(
    stack: Stack, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_create = stack.client.create_hospital

    async def flaky(payload):  # type: ignore[no-untyped-def]
        if payload.name == "Hospital 1":
            raise RuntimeError("bug")
        return await real_create(payload)

    monkeypatch.setattr(stack.client, "create_hospital", flaky)
    job = await new_job(stack, 2)
    result = await stack.processor.run(job.batch_id)
    assert statuses(result) == [RowStatus.FAILED, RowStatus.CREATED]
    assert result.status is JobStatus.PARTIAL_FAILURE


async def test_crash_outside_rows_marks_batch_resumable(
    stack: Stack, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def boom(_batch_id):  # type: ignore[no-untyped-def]
        raise RuntimeError("bug in finish")

    monkeypatch.setattr(stack.processor, "_finish", boom)
    job = await new_job(stack, 1)
    result = await stack.processor.run(job.batch_id)
    assert result.status is JobStatus.PARTIAL_FAILURE


async def test_progress_events_are_published(stack: Stack) -> None:
    job = await new_job(stack, 2)
    queue = stack.broker.subscribe(job.batch_id)
    await stack.processor.run(job.batch_id)
    events = []
    while not queue.empty():
        events.append(queue.get_nowait())
    types = [e["type"] for e in events]
    assert types[0] == "job"
    assert events[0]["status"] == "processing"
    assert types.count("row") == 4  # in_progress + created, per row
    assert events[-1]["type"] == "job"
    assert events[-1]["result"]["status"] == "completed"
    stack.broker.unsubscribe(job.batch_id, queue)
    assert stack.broker.subscriber_count(job.batch_id) == 0
