"""End-to-end against the REAL Hospital Directory API. Opt-in: `pytest -m integration`.

Every batch created here is deleted again (rollback), even when an assertion fails.
"""

from collections.abc import Iterator
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app

pytestmark = pytest.mark.integration

REAL_UPSTREAM = "https://hospital-directory.onrender.com"


@pytest.fixture(scope="module")
def live_api() -> Iterator[TestClient]:
    settings = Settings(upstream_base_url=REAL_UPSTREAM, log_format="text")
    with TestClient(create_app(settings)) as client:
        yield client


@pytest.fixture
def cleanup(live_api: TestClient) -> Iterator[list[str]]:
    batch_ids: list[str] = []
    yield batch_ids
    for batch_id in batch_ids:
        live_api.delete(f"/hospitals/bulk/{batch_id}")
        # Belt and braces: make sure nothing is left upstream.
        httpx.delete(f"{REAL_UPSTREAM}/hospitals/batch/{batch_id}", timeout=30)


def test_bulk_create_activate_and_rollback_against_real_upstream(
    live_api: TestClient, cleanup: list[str]
) -> None:
    tag = uuid4().hex[:8]
    csv = (
        "name,address,phone\n"
        f"Integration Test Hospital A {tag},1 Test St,555-0100\n"
        f"Integration Test Hospital B {tag},2 Test St,\n"
        f"Integration Test Hospital C {tag},3 Test St,(555) 010-0102\n"
    ).encode()

    response = live_api.post("/hospitals/bulk", files={"file": ("it.csv", csv, "text/csv")})
    assert response.status_code == 200, response.text
    body = response.json()
    cleanup.append(body["batch_id"])

    assert body["status"] == "completed", body
    assert body["batch_activated"] is True
    assert body["processed_hospitals"] == 3
    upstream = httpx.get(f"{REAL_UPSTREAM}/hospitals/batch/{body['batch_id']}", timeout=30).json()
    assert {h["id"] for h in upstream} == {h["hospital_id"] for h in body["hospitals"]}
    assert all(h["active"] for h in upstream)

    rolled = live_api.delete(f"/hospitals/bulk/{body['batch_id']}").json()
    assert rolled["status"] == "rolled_back"
    gone = httpx.get(f"{REAL_UPSTREAM}/hospitals/batch/{body['batch_id']}", timeout=30)
    assert gone.status_code == 404
