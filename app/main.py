"""Application factory and lifespan."""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import FileResponse

from app import __version__
from app.api import bulk, health, ws
from app.api.deps import Services
from app.clients.hospital_directory import HospitalDirectoryClient
from app.config import Settings, get_settings
from app.errors import register_exception_handlers
from app.logging_config import configure_logging
from app.middleware import BodySizeLimitMiddleware, RequestIdMiddleware
from app.repositories.batch_repository import InMemoryBatchRepository
from app.services.bulk_processor import BulkProcessor
from app.services.csv_validator import CsvValidator
from app.services.job_runner import JobRunner
from app.services.progress import ProgressBroker

STATIC_DIR = Path(__file__).parent / "static"
MULTIPART_OVERHEAD_BYTES = 64 * 1024  # boundaries + part headers around the CSV itself

DESCRIPTION = """
Bulk-import hospitals from a CSV into the
[Hospital Directory API](https://hospital-directory.onrender.com/docs).

**Workflow:** validate CSV -> new batch id (UUID4) -> create every row upstream concurrently
(bounded, with retries) -> reconcile against upstream -> activate the batch only if *every* row
exists.

**Row statuses:** `pending`, `in_progress`, `created` (exists but batch not activated),
`created_and_activated`, `failed` (see `error`; retried by resume), `skipped_invalid`,
`rolled_back`.

**Batch statuses:** `queued` -> `processing` -> `completed` | `partial_failure` |
`activation_failed`; then optionally `rolling_back` -> `rolled_back`.

**Progress:** poll `GET /hospitals/bulk/{batch_id}/status` or connect a WebSocket to
`/ws/bulk/{batch_id}`.
"""


def build_services(settings: Settings, http: httpx.AsyncClient) -> Services:
    client = HospitalDirectoryClient.from_settings(settings, http)
    repository = InMemoryBatchRepository(max_batches=settings.max_stored_batches)
    broker = ProgressBroker()
    runner = JobRunner()
    return Services(
        settings=settings,
        client=client,
        repository=repository,
        broker=broker,
        runner=runner,
        processor=BulkProcessor(client, repository, broker, runner),
        validator=CsvValidator(max_rows=settings.max_csv_rows, max_bytes=settings.max_upload_bytes),
    )


def create_app(
    settings: Settings | None = None,
    *,
    upstream_transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    """Build the app. `upstream_transport` lets tests swap the network for a mock."""
    settings = settings or get_settings()
    configure_logging(settings.log_level, settings.log_format)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # One pooled client for the process lifetime: connection reuse across all batches.
        http = HospitalDirectoryClient.build_http_client(settings, upstream_transport)
        services = build_services(settings, http)
        app.state.services = services
        warm_up: asyncio.Task[bool] | None = None
        if settings.upstream_warm_on_startup:
            # Don't block startup (and the platform health check) on a sleeping upstream.
            warm_up = asyncio.create_task(services.client.warm_up())
        try:
            yield
        finally:
            if warm_up is not None:
                warm_up.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await warm_up
            await services.runner.shutdown(settings.shutdown_grace_seconds)
            await http.aclose()

    app = FastAPI(
        title="Hospital Bulk Processor",
        version=__version__,
        description=DESCRIPTION,
        lifespan=lifespan,
    )
    register_exception_handlers(app)
    # Last added = outermost: request ids are assigned first, so even a 413 carries one.
    app.add_middleware(
        BodySizeLimitMiddleware,
        max_body_bytes=settings.max_upload_bytes + MULTIPART_OVERHEAD_BYTES,
    )
    app.add_middleware(RequestIdMiddleware)
    app.include_router(bulk.router)
    app.include_router(ws.router)
    app.include_router(health.router)

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    return app


app = create_app()
