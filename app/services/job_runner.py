"""Owns background batch tasks so they are not garbage-collected and can be drained on shutdown."""

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

logger = logging.getLogger(__name__)


class JobRunner:
    """A minimal in-process task supervisor.

    It is the seam where a real queue (arq/RQ/SQS workers) would plug in: `submit` would enqueue
    instead of `create_task`, and progress would flow back through the broker.
    """

    def __init__(self) -> None:
        self._tasks: set[asyncio.Task[Any]] = set()

    @property
    def active(self) -> int:
        return len(self._tasks)

    def submit[T](
        self, coro: Coroutine[Any, Any, T], *, name: str | None = None
    ) -> asyncio.Task[T]:
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def shutdown(self, grace_seconds: float) -> None:
        """Let running batches finish within the grace period, then cancel the rest.

        Cancelled runs mark their unfinished rows as failed (retryable), which leaves the batch
        resumable, not stuck in `processing`.
        """
        if not self._tasks:
            return
        logger.info("waiting for running batches", extra={"count": len(self._tasks)})
        _, pending = await asyncio.wait(set(self._tasks), timeout=grace_seconds)
        for task in pending:
            task.cancel()
        if pending:
            logger.warning("cancelled unfinished batches", extra={"count": len(pending)})
            await asyncio.gather(*pending, return_exceptions=True)
