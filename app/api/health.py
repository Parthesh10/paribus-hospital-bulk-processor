"""Liveness/readiness endpoint."""

from typing import Annotated

from fastapi import APIRouter, Query, Response

from app import __version__
from app.api.deps import ServicesDep
from app.models.schemas import HealthResponse, UpstreamHealth

router = APIRouter(tags=["Health"])


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Service health",
    description="Cheap by default: it does **not** call upstream, so platform health checks "
    "never fail (or wake the upstream) because of a sleeping dependency. `?deep=true` also "
    "pings upstream.",
)
async def health(
    services: ServicesDep,
    deep: Annotated[bool, Query(description="Also ping the upstream API.")] = False,
) -> HealthResponse:
    client = services.client
    reachable = await client.ping() if deep else None
    return HealthResponse(
        status="ok",
        version=__version__,
        upstream=UpstreamHealth(
            base_url=str(services.settings.upstream_base_url),
            warm=client.is_warm,
            last_success_at=client.last_success_at,
            reachable=reachable,
        ),
    )


@router.head("/health", include_in_schema=False)
async def health_head() -> Response:
    """Many uptime monitors probe with HEAD. Kept out of the schema to avoid a duplicate op id."""
    return Response()
