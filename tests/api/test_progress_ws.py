"""WebSocket progress stream."""

import threading
from typing import Any
from uuid import UUID

from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from tests.conftest import csv_bytes, make_rows, upload, wait_for
from tests.fake_upstream import FakeUpstream


def drain(ws: Any) -> list[dict[str, Any]]:
    events = []
    while True:
        try:
            events.append(ws.receive_json())
        except WebSocketDisconnect:
            return events


def test_websocket_streams_live_progress_until_terminal(
    api: TestClient, fake: FakeUpstream
) -> None:
    fake.create_gate = threading.Event()  # hold creates so we observe the run live
    accepted = api.post("/hospitals/bulk?async=true", files=upload(csv_bytes(*make_rows(3))))
    batch_id = accepted.json()["batch_id"]

    with api.websocket_connect(f"/ws/bulk/{batch_id}") as ws:
        snapshot = ws.receive_json()
        assert snapshot["type"] == "snapshot"
        assert snapshot["result"]["batch_id"] == batch_id
        assert snapshot["status"] in {"queued", "processing"}

        fake.create_gate.set()
        events = drain(ws)

    row_events = [e for e in events if e["type"] == "row"]
    assert {e["row"]["status"] for e in row_events} >= {"created"}
    assert all("counts" in e for e in row_events)
    final = events[-1]
    assert final["type"] == "job"
    assert final["status"] == "completed"
    assert final["result"]["processed_hospitals"] == 3
    assert final["result"]["batch_activated"] is True


def test_websocket_on_finished_batch_sends_snapshot_and_closes(api: TestClient) -> None:
    batch_id = api.post("/hospitals/bulk", files=upload(csv_bytes(*make_rows(1)))).json()[
        "batch_id"
    ]
    with api.websocket_connect(f"/ws/bulk/{batch_id}") as ws:
        events = drain(ws)
    assert [e["type"] for e in events] == ["snapshot"]
    assert events[0]["result"]["status"] == "completed"


def test_websocket_unknown_batch(api: TestClient) -> None:
    with api.websocket_connect("/ws/bulk/00000000-0000-4000-8000-000000000000") as ws:
        events = drain(ws)
    assert events == [
        {
            "type": "error",
            "code": "batch_not_found",
            "message": "Batch 00000000-0000-4000-8000-000000000000",
        }
    ]


def test_websocket_client_disconnect_unsubscribes(api: TestClient, fake: FakeUpstream) -> None:
    fake.create_gate = threading.Event()
    batch_id = api.post(
        "/hospitals/bulk?async=true", files=upload(csv_bytes(*make_rows(1)))
    ).json()["batch_id"]
    with api.websocket_connect(f"/ws/bulk/{batch_id}") as ws:
        ws.receive_json()
    # Leaving the context closes the socket; processing continues unaffected.
    fake.create_gate.set()
    broker = api.app.state.services.broker  # type: ignore[attr-defined]
    wait_for(lambda: broker.subscriber_count(UUID(batch_id)) == 0)
    wait_for(lambda: api.get(f"/hospitals/bulk/{batch_id}/status").json()["status"] == "completed")
