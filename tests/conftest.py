import time
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from app.clients.hospital_directory import HospitalDirectoryClient
from app.clients.retry import RetryPolicy
from app.config import Settings
from app.main import create_app
from app.repositories.batch_repository import InMemoryBatchRepository
from app.services.bulk_processor import BulkProcessor
from app.services.csv_validator import CsvValidator, ParsedRow
from app.services.job_runner import JobRunner
from app.services.progress import ProgressBroker
from tests.fake_upstream import FakeUpstream

UPSTREAM = "http://upstream.test"


def csv_bytes(*rows: str, header: str = "name,address,phone") -> bytes:
    return ("\n".join([header, *rows]) + "\n").encode()


def make_rows(n: int, prefix: str = "Hospital") -> list[str]:
    return [f"{prefix} {i},{i} Main St,555-{i:04d}" for i in range(1, n + 1)]


@pytest.fixture
def settings() -> Settings:
    return Settings(
        upstream_base_url=UPSTREAM,
        upstream_max_attempts=3,
        upstream_backoff_base=0,
        upstream_backoff_max=0,
        upstream_warm_on_startup=False,
        shutdown_grace_seconds=0,
        log_format="text",
        log_level="WARNING",
    )


@pytest.fixture
def fake() -> FakeUpstream:
    return FakeUpstream()


# --- service-level fixtures (no HTTP layer) ------------------------------------------------------


class Stack:
    def __init__(self, fake: FakeUpstream, http: httpx.AsyncClient) -> None:
        self.fake = fake
        self.client = HospitalDirectoryClient(
            http,
            retry_policy=RetryPolicy(max_attempts=3, base_delay=0, max_delay=0),
            max_concurrency=5,
            cold_start_timeout=5,
            warm_ttl_seconds=600,
        )
        self.repo = InMemoryBatchRepository()
        self.broker = ProgressBroker()
        self.runner = JobRunner()
        self.processor = BulkProcessor(self.client, self.repo, self.broker, self.runner)
        self.validator = CsvValidator(max_rows=20, max_bytes=256 * 1024)

    def parse(self, content: bytes) -> list[ParsedRow]:
        return self.validator.validate(content).rows


@pytest.fixture
async def stack(fake: FakeUpstream) -> AsyncIterator[Stack]:
    async with httpx.AsyncClient(
        base_url=UPSTREAM, transport=httpx.MockTransport(fake.handler)
    ) as http:
        yield Stack(fake, http)


# --- API fixtures --------------------------------------------------------------------------------


@pytest.fixture
def api(settings: Settings, fake: FakeUpstream) -> Iterator[TestClient]:
    app = create_app(settings, upstream_transport=httpx.MockTransport(fake.handler))
    with TestClient(app) as client:
        yield client


def upload(content: bytes, name: str = "hospitals.csv", ctype: str = "text/csv") -> dict[str, Any]:
    return {"file": (name, content, ctype)}


def wait_for(predicate: Callable[[], Any], timeout: float = 5.0) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result := predicate():
            return result
        time.sleep(0.01)
    raise AssertionError("condition not met in time")
