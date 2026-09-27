"""Client for the Hospital Directory API. All upstream I/O goes through here.

Responsibilities:
* one pooled `httpx.AsyncClient` shared by every batch (keep-alive; TLS handshakes are paid once);
* a process-wide semaphore capping in-flight upstream requests, so N concurrent uploads cannot
  turn into N x 20 connections against a shared free-tier service;
* retries with exponential backoff + jitter on transient failures only;
* cold-start handling: a single, coalesced warm-up ping with a long timeout before work starts.
"""

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import httpx

from app.clients.retry import (
    RetryPolicy,
    is_retryable_exception,
    is_retryable_status,
    parse_retry_after,
)
from app.config import Settings
from app.models.upstream import Hospital, HospitalCreate

logger = logging.getLogger(__name__)


class UpstreamError(Exception):
    """An upstream call failed for good (non-retryable response, or retries exhausted)."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool,
        status_code: int | None = None,
        attempts: int = 1,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.status_code = status_code
        self.attempts = attempts


def _describe_response(response: httpx.Response) -> str:
    """Compact, human-readable summary of an error response body."""
    try:
        body = response.json()
    except ValueError:
        text = response.text.strip()
        return text[:200] if text else response.reason_phrase
    detail = body.get("detail", body) if isinstance(body, dict) else body
    if isinstance(detail, list):  # FastAPI 422 shape: [{"loc": [...], "msg": "..."}]
        parts = []
        for item in detail:
            if isinstance(item, dict):
                loc = ".".join(str(p) for p in item.get("loc", ()) if p != "body")
                parts.append(f"{loc}: {item.get('msg')}" if loc else str(item.get("msg")))
        return "; ".join(parts) or str(detail)
    return str(detail)


class HospitalDirectoryClient:
    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        retry_policy: RetryPolicy,
        max_concurrency: int,
        cold_start_timeout: float,
        warm_ttl_seconds: float,
    ) -> None:
        self._http = http
        self._retry = retry_policy
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._cold_start_timeout = cold_start_timeout
        self._warm_ttl = warm_ttl_seconds
        self._warm_lock = asyncio.Lock()
        self._last_success_monotonic: float | None = None
        self.last_success_at: datetime | None = None

    @classmethod
    def build_http_client(
        cls, settings: Settings, transport: httpx.AsyncBaseTransport | None = None
    ) -> httpx.AsyncClient:
        pool = settings.upstream_max_concurrency
        return httpx.AsyncClient(
            base_url=str(settings.upstream_base_url),
            timeout=httpx.Timeout(
                settings.upstream_read_timeout, connect=settings.upstream_connect_timeout
            ),
            limits=httpx.Limits(max_connections=pool, max_keepalive_connections=pool),
            headers={"User-Agent": "hospital-bulk-processor/1.0"},
            transport=transport,
        )

    @classmethod
    def from_settings(
        cls, settings: Settings, http: httpx.AsyncClient
    ) -> "HospitalDirectoryClient":
        return cls(
            http,
            retry_policy=RetryPolicy(
                max_attempts=settings.upstream_max_attempts,
                base_delay=settings.upstream_backoff_base,
                max_delay=settings.upstream_backoff_max,
            ),
            max_concurrency=settings.upstream_max_concurrency,
            cold_start_timeout=settings.upstream_cold_start_timeout,
            warm_ttl_seconds=settings.upstream_warm_ttl_seconds,
        )

    # --- public API ----------------------------------------------------------------------------

    @property
    def is_warm(self) -> bool:
        return (
            self._last_success_monotonic is not None
            and time.monotonic() - self._last_success_monotonic < self._warm_ttl
        )

    async def warm_up(self) -> bool:
        """Wake a sleeping upstream before fanning out work.

        Without this, the first wave of 20 concurrent creates would all sit on a 30-60s cold start
        and trip their (short) read timeouts together. Concurrent callers share one ping.
        Never raises: if upstream is down, the real calls will fail and be reported per row.
        """
        if self.is_warm:
            return True
        async with self._warm_lock:
            if self.is_warm:  # another batch warmed it while we waited for the lock
                return True
            started = time.monotonic()
            try:
                # One long attempt absorbs a cold start; a second covers a blip. More would
                # only delay batches when upstream is genuinely down.
                await self._request(
                    "GET", "/", request_timeout=self._cold_start_timeout, max_attempts=2
                )
            except UpstreamError as exc:
                logger.warning("upstream warm-up failed", extra={"error": str(exc)})
                return False
            logger.info(
                "upstream warm-up complete",
                extra={"duration_s": round(time.monotonic() - started, 3)},
            )
            return True

    async def ping(self) -> bool:
        try:
            await self._request("GET", "/", max_attempts=1)
        except UpstreamError:
            return False
        return True

    async def create_hospital(self, payload: HospitalCreate) -> Hospital:
        response = await self._request(
            "POST", "/hospitals/", json=payload.model_dump(mode="json", exclude_none=True)
        )
        return Hospital.model_validate(response.json())

    async def get_batch(self, batch_id: UUID) -> list[Hospital]:
        """Hospitals in a batch. Upstream answers 404 for an empty/unknown batch; we return []."""
        response = await self._request("GET", f"/hospitals/batch/{batch_id}", ok_404=True)
        if response.status_code == 404:
            return []
        return [Hospital.model_validate(item) for item in response.json()]

    async def activate_batch(self, batch_id: UUID) -> int:
        response = await self._request("PATCH", f"/hospitals/batch/{batch_id}/activate")
        return int(response.json().get("activated_count", 0))

    async def delete_batch(self, batch_id: UUID) -> int:
        """Delete every hospital in a batch. Idempotent: an already-empty batch deletes 0."""
        response = await self._request("DELETE", f"/hospitals/batch/{batch_id}", ok_404=True)
        if response.status_code == 404:
            return 0
        return int(response.json().get("deleted_count", 0))

    async def delete_hospital(self, hospital_id: int) -> None:
        await self._request("DELETE", f"/hospitals/{hospital_id}", ok_404=True)

    # --- transport -----------------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        request_timeout: float | None = None,
        ok_404: bool = False,
        max_attempts: int | None = None,
    ) -> httpx.Response:
        """Send with retries. Returns 2xx (or 404 when `ok_404`) responses; raises otherwise.

        The semaphore is held per *attempt*, not across backoff sleeps, so a request that is
        waiting to retry does not block a slot other rows could be using.
        """
        attempts_allowed = max_attempts or self._retry.max_attempts
        timeout: Any = request_timeout if request_timeout is not None else httpx.USE_CLIENT_DEFAULT
        attempt = 0
        while True:
            attempt += 1
            started = time.monotonic()
            retry_after: float | None = None
            try:
                async with self._semaphore:
                    response = await self._http.request(method, path, json=json, timeout=timeout)
            except httpx.HTTPError as exc:
                if not is_retryable_exception(exc):
                    raise UpstreamError(
                        f"{method} {path} failed: {type(exc).__name__}: {exc}",
                        retryable=False,
                        attempts=attempt,
                    ) from exc
                error = UpstreamError(
                    f"{method} {path} failed after {attempt} attempt(s): "
                    f"{type(exc).__name__}{': ' + str(exc) if str(exc) else ''}",
                    retryable=True,
                    attempts=attempt,
                )
            else:
                code = response.status_code
                if code < 400 or (ok_404 and code == 404):
                    self._mark_success()
                    logger.debug(
                        "upstream call ok",
                        extra={
                            "method": method,
                            "path": path,
                            "status": code,
                            "attempt": attempt,
                            "duration_s": round(time.monotonic() - started, 3),
                        },
                    )
                    return response
                retryable = is_retryable_status(code)
                error = UpstreamError(
                    f"upstream {method} {path} returned {code}: {_describe_response(response)}",
                    retryable=retryable,
                    status_code=code,
                    attempts=attempt,
                )
                if not retryable:
                    raise error
                retry_after = parse_retry_after(response.headers.get("Retry-After"))

            if attempt >= attempts_allowed:
                raise error
            delay = self._retry.delay_for(attempt, retry_after)
            logger.warning(
                "upstream call failed; retrying",
                extra={
                    "method": method,
                    "path": path,
                    "attempt": attempt,
                    "retry_in_s": round(delay, 3),
                    "error": str(error),
                },
            )
            await asyncio.sleep(delay)

    def _mark_success(self) -> None:
        self._last_success_monotonic = time.monotonic()
        self.last_success_at = datetime.now(UTC)
