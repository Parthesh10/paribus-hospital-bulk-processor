"""A stateful in-memory emulation of the Hospital Directory API, with fault injection.

It reproduces the behaviours observed against the real service (see DECISIONS.md):
creates return 200; batch GET/PATCH/DELETE return 404 for an empty batch; activation returns 400
if anything in the batch is already active. Plugged in via `httpx.MockTransport(fake.handler)`.
"""

import asyncio
import re
import threading
from collections import defaultdict, deque
from datetime import UTC, datetime
from typing import Any

import httpx

# A fault is: an HTTP status code to return, an exception to raise, or COMMIT_THEN_TIMEOUT
# (perform the operation, then lose the response, i.e. the ambiguous-failure case).
COMMIT_THEN_TIMEOUT = "commit_then_timeout"
PASS = "pass"  # behave normally (useful to fail only the Nth call)
Fault = int | Exception | str

_BATCH = re.compile(r"^/hospitals/batch/(?P<batch>[^/]+)(?P<activate>/activate)?$")
_HOSPITAL = re.compile(r"^/hospitals/(?P<id>\d+)$")


class FakeUpstream:
    def __init__(self) -> None:
        self.hospitals: dict[int, dict[str, Any]] = {}
        self.next_id = 1
        self.calls: list[tuple[str, str]] = []
        self._faults: defaultdict[str, deque[Fault]] = defaultdict(deque)
        self._sticky: dict[str, Fault] = {}
        # When set (and cleared), creates block until the test releases them.
        self.create_gate: threading.Event | None = None

    # --- fault injection -----------------------------------------------------------------------

    def fail(self, op: str, *outcomes: Fault) -> None:
        """Queue one-shot outcomes for `op`: `create`, `create:<name>`, `activate`, `get_batch`,
        `delete_batch`, `delete_hospital`, `health`."""
        self._faults[op].extend(outcomes)

    def fail_always(self, op: str, outcome: Fault) -> None:
        self._sticky[op] = outcome

    def heal(self) -> None:
        self._faults.clear()
        self._sticky.clear()

    def wipe(self) -> None:
        """Simulate the free-tier upstream restarting and losing its in-memory data."""
        self.hospitals.clear()

    def count(self, method: str, path_prefix: str) -> int:
        return sum(1 for m, p in self.calls if m == method and p.startswith(path_prefix))

    def batch(self, batch_id: Any) -> list[dict[str, Any]]:
        return [h for h in self.hospitals.values() if h["creation_batch_id"] == str(batch_id)]

    def _next_fault(self, *ops: str) -> Fault | None:
        for op in ops:
            if self._faults[op]:
                return self._faults[op].popleft()
            if op in self._sticky:
                return self._sticky[op]
        return None

    # --- transport handler ---------------------------------------------------------------------

    async def handler(self, request: httpx.Request) -> httpx.Response:
        method, path = request.method, request.url.path
        self.calls.append((method, path))

        if method == "GET" and path == "/":
            return self._apply(self._next_fault("health"), request, lambda: _json({"status": "OK"}))

        if method == "POST" and path == "/hospitals/":
            return await self._create(request)

        if m := _BATCH.match(path):
            batch_id = m["batch"]
            if method == "PATCH" and m["activate"]:
                return self._apply(
                    self._next_fault("activate"), request, lambda: self._activate(batch_id)
                )
            if method == "GET":
                return self._apply(
                    self._next_fault("get_batch"), request, lambda: self._get_batch(batch_id)
                )
            if method == "DELETE":
                return self._apply(
                    self._next_fault("delete_batch"), request, lambda: self._delete_batch(batch_id)
                )

        if (m := _HOSPITAL.match(path)) and method == "DELETE":
            hospital_id = int(m["id"])
            return self._apply(
                self._next_fault("delete_hospital"),
                request,
                lambda: (
                    _empty(204)
                    if self.hospitals.pop(hospital_id, None)
                    else _json({"detail": "Hospital not found"}, 404)
                ),
            )

        return _json({"detail": "Not Found"}, 404)

    def _apply(self, fault: Fault | None, request: httpx.Request, action: Any) -> httpx.Response:
        if fault is None or fault == PASS:
            response: httpx.Response = action()
            return response
        if isinstance(fault, Exception):
            raise fault
        if fault == COMMIT_THEN_TIMEOUT:
            action()
            raise httpx.ReadTimeout("response lost", request=request)
        assert isinstance(fault, int)
        return _json({"detail": f"injected {fault}"}, fault)

    async def _create(self, request: httpx.Request) -> httpx.Response:
        payload = httpx.Response(200, content=request.content).json()
        if self.create_gate is not None:
            while not self.create_gate.is_set():
                await asyncio.sleep(0.005)
        fault = self._next_fault(f"create:{payload.get('name')}", "create")

        def commit() -> httpx.Response:
            if not payload.get("name") or not payload.get("address"):
                return _json(
                    {
                        "detail": [
                            {
                                "type": "string_too_short",
                                "loc": ["body", "name"],
                                "msg": "String should have at least 1 character",
                            }
                        ]
                    },
                    422,
                )
            hospital = {
                "id": self.next_id,
                "name": payload["name"],
                "address": payload["address"],
                "phone": payload.get("phone"),
                "creation_batch_id": payload.get("creation_batch_id"),
                "active": payload.get("creation_batch_id") is None,
                "created_at": datetime.now(UTC).replace(tzinfo=None).isoformat(),
            }
            self.hospitals[self.next_id] = hospital
            self.next_id += 1
            return _json(hospital)

        return self._apply(fault, request, commit)

    def _get_batch(self, batch_id: str) -> httpx.Response:
        found = self.batch(batch_id)
        if not found:
            return _json({"detail": "No hospitals found with the specified batch ID"}, 404)
        return _json(found)

    def _activate(self, batch_id: str) -> httpx.Response:
        found = self.batch(batch_id)
        if not found:
            return _json({"detail": "No hospitals found with the specified batch ID"}, 404)
        if any(h["active"] for h in found):
            return _json(
                {
                    "detail": "Cannot activate batch: one or more hospitals in the batch "
                    "are already active"
                },
                400,
            )
        for h in found:
            h["active"] = True
        return _json(
            {
                "activated_count": len(found),
                "message": f"Activated {len(found)} hospital(s) with batch ID {batch_id}",
            }
        )

    def _delete_batch(self, batch_id: str) -> httpx.Response:
        found = self.batch(batch_id)
        if not found:
            return _json({"detail": "No hospitals found with the specified batch ID"}, 404)
        for h in found:
            del self.hospitals[h["id"]]
        return _json({"deleted_count": len(found), "message": f"Deleted {len(found)}"})


def _json(body: Any, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=body)


def _empty(status: int) -> httpx.Response:
    return httpx.Response(status)
