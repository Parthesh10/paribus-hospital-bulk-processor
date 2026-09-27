"""Application exceptions and the handlers that render them as consistent JSON error bodies."""

import logging
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger(__name__)


class AppError(Exception):
    """Base for errors that map to a specific HTTP response."""

    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR
    code: str = "internal_error"

    def __init__(self, message: str, *, details: Any = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details


class BatchNotFoundError(AppError):
    status_code = status.HTTP_404_NOT_FOUND
    code = "batch_not_found"


class InvalidStateError(AppError):
    """Operation not allowed in the batch's current state (e.g. resume while processing)."""

    status_code = status.HTTP_409_CONFLICT
    code = "invalid_batch_state"


class CsvValidationError(AppError):
    status_code = status.HTTP_422_UNPROCESSABLE_CONTENT
    code = "csv_validation_failed"


class PayloadTooLargeError(AppError):
    status_code = status.HTTP_413_CONTENT_TOO_LARGE
    code = "payload_too_large"


class UpstreamUnavailableError(AppError):
    status_code = status.HTTP_502_BAD_GATEWAY
    code = "upstream_error"


def _body(code: str, message: str, details: Any = None) -> dict[str, Any]:
    return {"error": {"code": code, "message": message, "details": details}}


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def _app_error(_: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code, content=_body(exc.code, exc.message, exc.details)
        )

    @app.exception_handler(RequestValidationError)
    async def _request_validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        details = [
            {"loc": list(e.get("loc", ())), "msg": e.get("msg"), "type": e.get("type")}
            for e in exc.errors()
        ]
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content=_body("request_validation_failed", "Request is invalid.", details),
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = {404: "not_found", 405: "method_not_allowed"}.get(exc.status_code, "http_error")
        return JSONResponse(
            status_code=exc.status_code,
            content=_body(code, str(exc.detail)),
            headers=getattr(exc, "headers", None),
        )

    @app.exception_handler(Exception)
    async def _unhandled(_: Request, exc: Exception) -> JSONResponse:
        logger.error("unhandled error", exc_info=exc)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=_body("internal_error", "An unexpected error occurred."),
        )
