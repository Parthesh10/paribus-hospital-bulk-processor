"""Request-id middleware (pure ASGI, so it also covers WebSockets and has no streaming caveats)."""

import re
from uuid import uuid4

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.logging_config import request_id_var

_SAFE_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


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
