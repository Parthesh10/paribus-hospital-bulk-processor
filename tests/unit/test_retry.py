from datetime import UTC, datetime

import httpx
import pytest

from app.clients.retry import (
    RetryPolicy,
    is_retryable_exception,
    is_retryable_status,
    parse_retry_after,
)


def upper_bound(_low: float, high: float) -> float:
    return high


def test_backoff_grows_exponentially_and_is_capped() -> None:
    policy = RetryPolicy(max_attempts=10, base_delay=0.5, max_delay=4.0, rand=upper_bound)
    assert [policy.delay_for(n) for n in range(1, 7)] == [0.5, 1.0, 2.0, 4.0, 4.0, 4.0]


def test_full_jitter_samples_between_zero_and_ceiling() -> None:
    seen: list[tuple[float, float]] = []

    def record(low: float, high: float) -> float:
        seen.append((low, high))
        return (low + high) / 2

    policy = RetryPolicy(base_delay=1.0, max_delay=10.0, rand=record)
    assert policy.delay_for(3) == 2.0
    assert seen == [(0.0, 4.0)]


def test_retry_after_overrides_backoff_but_is_capped() -> None:
    policy = RetryPolicy(base_delay=1.0, max_delay=5.0, rand=upper_bound)
    assert policy.delay_for(1, retry_after=3.0) == 3.0
    assert policy.delay_for(1, retry_after=120.0) == 5.0


@pytest.mark.parametrize("code", [408, 425, 429, 500, 502, 503, 504])
def test_transient_statuses_are_retryable(code: int) -> None:
    assert is_retryable_status(code)


@pytest.mark.parametrize("code", [400, 401, 403, 404, 409, 422])
def test_client_errors_are_not_retryable(code: int) -> None:
    assert not is_retryable_status(code)


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (httpx.ReadTimeout("t"), True),
        (httpx.ConnectTimeout("t"), True),
        (httpx.ConnectError("c"), True),
        (httpx.RemoteProtocolError("r"), True),
        (httpx.UnsupportedProtocol("u"), False),
        (ValueError("v"), False),
    ],
)
def test_exception_classification(exc: BaseException, expected: bool) -> None:
    assert is_retryable_exception(exc) is expected


def test_parse_retry_after_seconds_and_http_date() -> None:
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    assert parse_retry_after("7") == 7.0
    assert parse_retry_after("Thu, 01 Jan 2026 12:00:30 GMT", now=now) == 30.0
    assert parse_retry_after("Thu, 01 Jan 2026 11:00:00 GMT", now=now) == 0.0
    assert parse_retry_after("soon") is None
    assert parse_retry_after(None) is None
