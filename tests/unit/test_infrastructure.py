"""Job runner shutdown, progress broker back-pressure, structured logging, app lifespan."""

import asyncio
import json
import logging
from uuid import uuid4

import httpx
from fastapi.testclient import TestClient

from app.config import Settings
from app.logging_config import ContextFilter, JsonFormatter, batch_id_var, configure_logging
from app.main import create_app
from app.services.job_runner import JobRunner
from app.services.progress import ProgressBroker
from tests.fake_upstream import FakeUpstream


async def test_runner_waits_for_quick_jobs_and_cancels_slow_ones() -> None:
    runner = JobRunner()
    quick = runner.submit(asyncio.sleep(0.01, result="done"))
    slow = runner.submit(asyncio.sleep(30))
    assert runner.active == 2
    await runner.shutdown(grace_seconds=0.1)
    assert quick.result() == "done"
    assert slow.cancelled()
    assert runner.active == 0
    await runner.shutdown(grace_seconds=0)  # no-op when idle


def test_broker_drops_events_for_a_stuck_subscriber() -> None:
    broker = ProgressBroker(queue_size=2)
    batch_id = uuid4()
    queue = broker.subscribe(batch_id)
    for i in range(5):
        broker.publish(batch_id, {"n": i})  # must never block or raise
    assert queue.qsize() == 2
    broker.unsubscribe(batch_id, queue)
    broker.unsubscribe(batch_id, queue)  # idempotent
    broker.publish(batch_id, {"n": 99})  # no subscribers: no-op


def test_json_log_lines_carry_batch_id_and_extras() -> None:
    record = logging.LogRecord("app.test", logging.INFO, __file__, 1, "row failed", (), None)
    record.row = 3
    token = batch_id_var.set("b-123")
    try:
        ContextFilter().filter(record)
    finally:
        batch_id_var.reset(token)
    payload = json.loads(JsonFormatter().format(record))
    assert payload["msg"] == "row failed"
    assert payload["batch_id"] == "b-123"
    assert payload["row"] == 3
    assert payload["level"] == "INFO"


def test_json_log_includes_exception() -> None:
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = logging.LogRecord("x", logging.ERROR, __file__, 1, "err", (), sys.exc_info())
    ContextFilter().filter(record)
    assert "ValueError: boom" in json.loads(JsonFormatter().format(record))["exc_info"]


def test_configure_logging_text_mode() -> None:
    configure_logging("DEBUG", "text")
    root = logging.getLogger()
    assert root.level == logging.DEBUG
    assert len(root.handlers) == 1
    configure_logging("WARNING", "text")


def test_startup_warms_upstream_in_background(settings: Settings) -> None:
    fake = FakeUpstream()
    app = create_app(
        settings.model_copy(update={"upstream_warm_on_startup": True}),
        upstream_transport=httpx.MockTransport(fake.handler),
    )
    with TestClient(app) as client:
        from tests.conftest import wait_for

        wait_for(lambda: ("GET", "/") in fake.calls)
        assert client.get("/health").json()["upstream"]["warm"] is True


def test_unhandled_errors_return_consistent_json(settings: Settings) -> None:
    app = create_app(settings, upstream_transport=httpx.MockTransport(FakeUpstream().handler))

    @app.get("/boom")
    async def boom() -> None:
        raise RuntimeError("kaboom")

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/boom")
    assert response.status_code == 500
    assert response.json() == {
        "error": {
            "code": "internal_error",
            "message": "An unexpected error occurred.",
            "details": None,
        }
    }
