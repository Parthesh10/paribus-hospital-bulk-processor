"""Retry policy: which failures are transient, and how long to wait between attempts."""

import random
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx

RETRYABLE_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})


def is_retryable_status(status_code: int) -> bool:
    """Transient server-side conditions. Other 4xx mean *our request* is wrong; never retry."""
    return status_code in RETRYABLE_STATUS_CODES


def is_retryable_exception(exc: BaseException) -> bool:
    """Timeouts, connection failures and dropped connections are transient.

    `UnsupportedProtocol`/`InvalidURL`-style errors are configuration bugs and are not retried.
    """
    return isinstance(exc, httpx.TimeoutException | httpx.NetworkError | httpx.RemoteProtocolError)


def parse_retry_after(value: str | None, *, now: datetime | None = None) -> float | None:
    """`Retry-After` is either delta-seconds or an HTTP date (RFC 9110 §10.2.3)."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - (now or datetime.now(UTC))).total_seconds())


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff with full jitter.

    Full jitter (`uniform(0, min(cap, base * 2**n))`) de-synchronises the 20 concurrent row tasks
    of a batch that all hit the same upstream hiccup at once, instead of having them retry in
    lock-step waves (see the AWS Architecture Blog, "Exponential Backoff and Jitter").
    """

    max_attempts: int = 4
    base_delay: float = 0.5
    max_delay: float = 8.0
    rand: Callable[[float, float], float] = field(default=random.uniform, compare=False)

    def delay_for(self, attempt: int, retry_after: float | None = None) -> float:
        """Delay before the next attempt; `attempt` is the 1-based number of the one that failed."""
        if retry_after is not None:
            # The server told us when to come back; honour it (bounded by our cap).
            return min(retry_after, self.max_delay)
        ceiling = min(self.max_delay, self.base_delay * (2 ** (attempt - 1)))
        return self.rand(0.0, ceiling)
