"""HospitalDirectoryClient against respx-mocked routes: retries, error mapping, warm-up."""

import asyncio
from collections.abc import AsyncIterator, Iterator
from uuid import uuid4

import httpx
import pytest
import respx

from app.clients.hospital_directory import HospitalDirectoryClient, UpstreamError
from app.clients.retry import RetryPolicy
from app.models.upstream import HospitalCreate

BASE = "http://upstream.test"
HOSPITAL = {
    "id": 7,
    "name": "General",
    "address": "1 Main St",
    "phone": None,
    "creation_batch_id": "3f1c2d4e-0000-4000-8000-000000000000",
    "active": False,
    "created_at": "2026-09-27T09:50:50.824234",
}


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    with respx.mock(base_url=BASE, assert_all_called=False) as mock:
        yield mock


@pytest.fixture
async def client() -> AsyncIterator[HospitalDirectoryClient]:
    async with httpx.AsyncClient(base_url=BASE) as http:
        yield HospitalDirectoryClient(
            http,
            retry_policy=RetryPolicy(max_attempts=3, base_delay=0, max_delay=0),
            max_concurrency=2,
            cold_start_timeout=5,
            warm_ttl_seconds=600,
        )


def payload() -> HospitalCreate:
    return HospitalCreate(name="General", address="1 Main St", creation_batch_id=uuid4())


async def test_create_sends_batch_id_in_body_and_omits_null_phone(
    router: respx.MockRouter, client: HospitalDirectoryClient
) -> None:
    route = router.post("/hospitals/").respond(200, json=HOSPITAL)
    body = payload()
    hospital = await client.create_hospital(body)
    assert hospital.id == 7
    sent = route.calls.last.request
    assert httpx.Response(200, content=sent.content).json() == {
        "name": "General",
        "address": "1 Main St",
        "creation_batch_id": str(body.creation_batch_id),
    }


async def test_retries_5xx_then_succeeds(
    router: respx.MockRouter, client: HospitalDirectoryClient
) -> None:
    route = router.post("/hospitals/").mock(
        side_effect=[httpx.Response(503), httpx.Response(502), httpx.Response(200, json=HOSPITAL)]
    )
    assert (await client.create_hospital(payload())).id == 7
    assert route.call_count == 3


async def test_retries_timeouts_then_gives_up(
    router: respx.MockRouter, client: HospitalDirectoryClient
) -> None:
    route = router.post("/hospitals/").mock(side_effect=httpx.ReadTimeout("slow"))
    with pytest.raises(UpstreamError) as info:
        await client.create_hospital(payload())
    assert route.call_count == 3
    assert info.value.retryable
    assert info.value.attempts == 3
    assert "ReadTimeout" in str(info.value)


async def test_does_not_retry_validation_errors(
    router: respx.MockRouter, client: HospitalDirectoryClient
) -> None:
    route = router.post("/hospitals/").respond(
        422,
        json={"detail": [{"loc": ["body", "name"], "msg": "String should have at least 1"}]},
    )
    with pytest.raises(UpstreamError) as info:
        await client.create_hospital(payload())
    assert route.call_count == 1
    assert not info.value.retryable
    assert info.value.status_code == 422
    assert "name: String should have at least 1" in str(info.value)


async def test_honours_retry_after_on_429(
    router: respx.MockRouter, client: HospitalDirectoryClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    slept: list[float] = []
    real_sleep = asyncio.sleep

    async def fake_sleep(delay: float) -> None:
        slept.append(delay)
        await real_sleep(0)

    monkeypatch.setattr("app.clients.hospital_directory.asyncio.sleep", fake_sleep)
    client._retry = RetryPolicy(max_attempts=2, base_delay=0, max_delay=10)
    router.post("/hospitals/").mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "2"}),
            httpx.Response(200, json=HOSPITAL),
        ]
    )
    await client.create_hospital(payload())
    assert slept == [2.0]


async def test_non_retryable_transport_error_fails_fast(
    router: respx.MockRouter, client: HospitalDirectoryClient
) -> None:
    route = router.get("/").mock(side_effect=httpx.UnsupportedProtocol("ftp"))
    with pytest.raises(UpstreamError) as info:
        await client._request("GET", "/")
    assert route.call_count == 1
    assert not info.value.retryable


async def test_error_body_that_is_not_json_is_summarised(
    router: respx.MockRouter, client: HospitalDirectoryClient
) -> None:
    router.patch(url__regex=r"/activate$").respond(400, text="nope")
    with pytest.raises(UpstreamError, match="400: nope"):
        await client.activate_batch(uuid4())


async def test_batch_404_means_empty(
    router: respx.MockRouter, client: HospitalDirectoryClient
) -> None:
    router.get(url__regex=r"/hospitals/batch/").respond(404, json={"detail": "No hospitals"})
    router.delete(url__regex=r"/hospitals/batch/").respond(404, json={"detail": "No hospitals"})
    assert await client.get_batch(uuid4()) == []
    assert await client.delete_batch(uuid4()) == 0


async def test_batch_operations_parse_counts(
    router: respx.MockRouter, client: HospitalDirectoryClient
) -> None:
    batch_id = uuid4()
    router.get(f"/hospitals/batch/{batch_id}").respond(200, json=[HOSPITAL])
    router.patch(f"/hospitals/batch/{batch_id}/activate").respond(200, json={"activated_count": 1})
    router.delete(f"/hospitals/batch/{batch_id}").respond(200, json={"deleted_count": 1})
    router.delete("/hospitals/7").respond(204)
    assert [h.id for h in await client.get_batch(batch_id)] == [7]
    assert await client.activate_batch(batch_id) == 1
    assert await client.delete_batch(batch_id) == 1
    await client.delete_hospital(7)


async def test_warm_up_is_coalesced_and_cached(
    router: respx.MockRouter, client: HospitalDirectoryClient
) -> None:
    async def slow_health(_: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.05)
        return httpx.Response(200, json={"status": "OK"})

    route = router.get("/").mock(side_effect=slow_health)
    assert not client.is_warm
    results = await asyncio.gather(*(client.warm_up() for _ in range(5)))
    assert results == [True] * 5
    assert route.call_count == 1  # five concurrent batches share one cold-start ping
    assert client.is_warm
    assert client.last_success_at is not None
    await client.warm_up()
    assert route.call_count == 1  # cached within the TTL


async def test_warm_up_failure_is_swallowed(
    router: respx.MockRouter, client: HospitalDirectoryClient
) -> None:
    router.get("/").mock(side_effect=httpx.ConnectError("down"))
    assert await client.warm_up() is False
    assert not client.is_warm


async def test_ping(router: respx.MockRouter, client: HospitalDirectoryClient) -> None:
    router.get("/").mock(side_effect=[httpx.Response(200, json={}), httpx.Response(503)])
    assert await client.ping() is True
    assert await client.ping() is False  # ping never retries


async def test_semaphore_caps_in_flight_requests(
    router: respx.MockRouter, client: HospitalDirectoryClient
) -> None:
    in_flight = peak = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        return httpx.Response(200, json=HOSPITAL)

    router.post("/hospitals/").mock(side_effect=handler)
    await asyncio.gather(*(client.create_hospital(payload()) for _ in range(8)))
    assert peak == 2  # max_concurrency=2
