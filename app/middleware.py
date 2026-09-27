"""Pure ASGI middleware (no BaseHTTPMiddleware: covers WebSockets, no streaming caveats)."""

import json
import re
from uuid import uuid4

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.logging_config import request_id_var

_SAFE_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


class BodySizeLimitMiddleware:
    """Reject oversized request bodies *before* they are parsed.

    Starlette's multipart parser spools the entire upload to a temp file before the endpoint
    runs, so the endpoint's own size check alone would still let a client make us write
    gigabytes to disk. Rejecting on `Content-Length` avoids that. (Chunked uploads without a
    length are still bounded by the endpoint check; a reverse proxy limit is the real fix.)
    """

    def __init__(self, app: ASGIApp, *, max_body_bytes: int) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            length = dict(scope.get("headers") or []).get(b"content-length", b"")
            if length.isdigit() and int(length) > self.max_body_bytes:
                body = json.dumps(
                    {
                        "error": {
                            "code": "payload_too_large",
                            "message": f"Request body exceeds {self.max_body_bytes} bytes.",
                            "details": None,
                        }
                    }
                ).encode()
                await send(
                    {
                        "type": "http.response.start",
                        "status": 413,
                        "headers": [
                            (b"content-type", b"application/json"),
                            (b"content-length", str(len(body)).encode()),
                            (b"connection", b"close"),
                        ],
                    }
                )
                await send({"type": "http.response.body", "body": body})
                return
        await self.app(scope, receive, send)


class RequestIdMiddleware:
    """Propagates `X-Request-ID` (or mints one) into logs and the response headers.

    Background batch tasks inherit the context, so a batch's log lines also carry the id of the
    request that started it.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        incoming = dict(scope.get("headers") or []).get(b"x-request-id", b"").decode("latin-1")
        request_id = incoming if _SAFE_ID.match(incoming) else uuid4().hex[:16]
        token = request_id_var.set(request_id)

        async def send_with_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message).append("X-Request-ID", request_id)
            await send(message)

        try:
            await self.app(scope, receive, send_with_id)
        finally:
            request_id_var.reset(token)
