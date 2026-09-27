"""HTTP-level tests: the full app with the upstream replaced by FakeUpstream."""

import threading

from fastapi.testclient import TestClient

from tests.conftest import csv_bytes, make_rows, upload, wait_for
from tests.fake_upstream import FakeUpstream

SPEC_KEYS = {
    "batch_id",
    "total_hospitals",
    "processed_hospitals",
    "failed_hospitals",
    "processing_time_seconds",
    "batch_activated",
    "hospitals",
}


def test_bulk_create_happy_path_returns_spec_shape(api: TestClient, fake: FakeUpstream) -> None:
    response = api.post("/hospitals/bulk", files=upload(csv_bytes(*make_rows(20))))
    assert response.status_code == 200
    body = response.json()
    assert body.keys() >= SPEC_KEYS
    assert body["total_hospitals"] == body["processed_hospitals"] == 20
    assert body["failed_hospitals"] == 0
    assert body["batch_activated"] is True
    assert body["status"] == "completed"
    assert [h["row"] for h in body["hospitals"]] == list(range(1, 21))
    assert {h["status"] for h in body["hospitals"]} == {"created_and_activated"}
    assert all(isinstance(h["hospital_id"], int) for h in body["hospitals"])
    assert len(fake.batch(body["batch_id"])) == 20
    assert response.headers["X-Request-ID"]


def test_partial_failure_reports_rows_and_skips_activation(
    api: TestClient, fake: FakeUpstream
) -> None:
    fake.fail_always("create:Hospital 2", 500)
    body = api.post("/hospitals/bulk", files=upload(csv_bytes(*make_rows(3)))).json()
    assert body["status"] == "partial_failure"
    assert body["batch_activated"] is False
    assert (body["processed_hospitals"], body["failed_hospitals"]) == (2, 1)
    assert body["resumable"] is True
    by_row = {h["row"]: h for h in body["hospitals"]}
    assert by_row[1]["status"] == "created"
    assert by_row[2]["status"] == "failed"
    assert "500" in by_row[2]["error"]
    assert fake.count("PATCH", "/hospitals/batch") == 0


def test_upstream_timeouts_are_retried(api: TestClient, fake: FakeUpstream) -> None:
    import httpx

    fake.fail("create", httpx.ReadTimeout("slow"), 503)
    body = api.post("/hospitals/bulk", files=upload(csv_bytes(*make_rows(2)))).json()
    assert body["status"] == "completed"


def test_resume_after_failure_then_rollback(api: TestClient, fake: FakeUpstream) -> None:
    fake.fail_always("create:Hospital 1", 503)
    first = api.post("/hospitals/bulk", files=upload(csv_bytes(*make_rows(2)))).json()
    batch_id = first["batch_id"]
    assert first["status"] == "partial_failure"

    fake.heal()
    resumed = api.post(f"/hospitals/bulk/{batch_id}/resume")
    assert resumed.status_code == 200
    body = resumed.json()
    assert body["batch_id"] == batch_id
    assert body["status"] == "completed"
    assert body["batch_activated"] is True
    assert body["runs"] == 2
    assert len(fake.batch(batch_id)) == 2

    again = api.post(f"/hospitals/bulk/{batch_id}/resume").json()  # idempotent
    assert again["runs"] == 2

    rolled = api.delete(f"/hospitals/bulk/{batch_id}")
    assert rolled.status_code == 200
    assert rolled.json()["status"] == "rolled_back"
    assert fake.batch(batch_id) == []

    conflict = api.post(f"/hospitals/bulk/{batch_id}/resume")
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "invalid_batch_state"


def test_activation_failure_is_reported(api: TestClient, fake: FakeUpstream) -> None:
    fake.fail_always("activate", 503)
    body = api.post("/hospitals/bulk", files=upload(csv_bytes(*make_rows(2)))).json()
    assert body["status"] == "activation_failed"
    assert body["batch_activated"] is False
    assert "503" in body["activation_error"]
    assert {h["status"] for h in body["hospitals"]} == {"created"}


def test_rollback_upstream_failure_returns_502(api: TestClient, fake: FakeUpstream) -> None:
    body = api.post("/hospitals/bulk", files=upload(csv_bytes(*make_rows(1)))).json()
    fake.fail_always("delete_batch", 503)
    response = api.delete(f"/hospitals/bulk/{body['batch_id']}")
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "upstream_error"
    status = api.get(f"/hospitals/bulk/{body['batch_id']}/status").json()
    assert status["status"] == "completed"


def test_invalid_csv_is_rejected_with_report(api: TestClient, fake: FakeUpstream) -> None:
    response = api.post("/hospitals/bulk", files=upload(csv_bytes(*make_rows(21))))
    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "csv_validation_failed"
    assert error["details"]["errors"][0]["code"] == "too_many_rows"
    assert fake.calls == []  # nothing reached upstream


def test_invalid_rows_reject_file_unless_skip_invalid(api: TestClient, fake: FakeUpstream) -> None:
    content = csv_bytes("Good,1 Main St,555-0100", ",2 Main St,")
    rejected = api.post("/hospitals/bulk", files=upload(content))
    assert rejected.status_code == 422
    assert "skip_invalid" in rejected.json()["error"]["message"]

    body = api.post("/hospitals/bulk?skip_invalid=true", files=upload(content)).json()
    assert body["status"] == "completed"
    assert body["skipped_hospitals"] == 1
    assert [h["status"] for h in body["hospitals"]] == ["created_and_activated", "skipped_invalid"]


def test_all_rows_invalid_is_rejected_even_with_skip_invalid(api: TestClient) -> None:
    response = api.post("/hospitals/bulk?skip_invalid=true", files=upload(csv_bytes(",1 St,")))
    assert response.status_code == 422
    assert response.json()["error"]["message"] == "The CSV file has no valid rows."


def test_oversized_upload_is_413(api: TestClient) -> None:
    big = csv_bytes(*make_rows(10)) + b"#" * (300 * 1024)
    response = api.post("/hospitals/bulk", files=upload(big))
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "payload_too_large"


def test_wrong_file_type_is_rejected(api: TestClient) -> None:
    response = api.post(
        "/hospitals/bulk", files=upload(b"%PDF-1.7", name="h.pdf", ctype="application/pdf")
    )
    assert response.status_code == 422
    codes = {e["code"] for e in response.json()["error"]["details"]["errors"]}
    assert {"invalid_file_extension", "invalid_content_type"} <= codes


def test_missing_file_field_uses_consistent_error_shape(api: TestClient) -> None:
    response = api.post("/hospitals/bulk")
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "request_validation_failed"


def test_async_mode_returns_202_and_can_be_polled(api: TestClient, fake: FakeUpstream) -> None:
    response = api.post("/hospitals/bulk?async=true", files=upload(csv_bytes(*make_rows(3))))
    assert response.status_code == 202
    accepted = response.json()
    batch_id = accepted["batch_id"]
    assert accepted["links"] == {
        "status": f"/hospitals/bulk/{batch_id}/status",
        "websocket": f"/ws/bulk/{batch_id}",
        "resume": f"/hospitals/bulk/{batch_id}/resume",
        "rollback": f"/hospitals/bulk/{batch_id}",
    }
    final = wait_for(
        lambda: (b := api.get(accepted["links"]["status"]).json())["status"] == "completed" and b
    )
    assert final["processed_hospitals"] == 3


def test_live_status_and_conflicting_operations_while_processing(
    api: TestClient, fake: FakeUpstream
) -> None:
    fake.create_gate = threading.Event()
    batch_id = api.post(
        "/hospitals/bulk?async=true", files=upload(csv_bytes(*make_rows(2)))
    ).json()["batch_id"]

    live = wait_for(
        lambda: (
            (b := api.get(f"/hospitals/bulk/{batch_id}/status").json())["status"] == "processing"
            and b
        )
    )
    assert live["pending_hospitals"] == 2
    assert api.post(f"/hospitals/bulk/{batch_id}/resume").status_code == 409
    assert api.delete(f"/hospitals/bulk/{batch_id}").status_code == 409

    fake.create_gate.set()
    wait_for(lambda: api.get(f"/hospitals/bulk/{batch_id}/status").json()["status"] == "completed")


def test_async_resume_returns_202(api: TestClient, fake: FakeUpstream) -> None:
    fake.fail_always("create", 500)
    batch_id = api.post("/hospitals/bulk", files=upload(csv_bytes(*make_rows(1)))).json()[
        "batch_id"
    ]
    fake.heal()
    response = api.post(f"/hospitals/bulk/{batch_id}/resume?async=true")
    assert response.status_code == 202
    wait_for(lambda: api.get(f"/hospitals/bulk/{batch_id}/status").json()["status"] == "completed")


def test_unknown_and_malformed_batch_ids(api: TestClient) -> None:
    missing = api.get("/hospitals/bulk/00000000-0000-4000-8000-000000000000/status")
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "batch_not_found"
    malformed = api.get("/hospitals/bulk/not-a-uuid/status")
    assert malformed.status_code == 422
    assert malformed.json()["error"]["code"] == "request_validation_failed"
    assert api.get("/nope").json()["error"]["code"] == "not_found"


def test_validate_endpoint_never_calls_upstream(api: TestClient, fake: FakeUpstream) -> None:
    ok = api.post("/hospitals/bulk/validate", files=upload(csv_bytes(*make_rows(2))))
    assert ok.status_code == 200
    assert ok.json()["valid"] is True
    assert ok.json()["total_rows"] == 2

    bad = api.post(
        "/hospitals/bulk/validate", files=upload(b"name,address\n,1 St\nA,\n", name="x.csv")
    ).json()
    assert bad["valid"] is False
    assert bad["invalid_rows"] == 2
    assert [e["code"] for e in bad["errors"]] == ["missing_required_field"] * 2

    huge = api.post("/hospitals/bulk/validate", files=upload(b"x" * (300 * 1024))).json()
    assert huge["errors"][0]["code"] == "file_too_large"
    assert fake.calls == []


def test_health_and_index(api: TestClient, fake: FakeUpstream) -> None:
    health = api.get("/health").json()
    assert health["status"] == "ok"
    assert health["upstream"]["reachable"] is None
    assert fake.calls == []  # shallow health never touches upstream

    deep = api.get("/health?deep=true").json()
    assert deep["upstream"]["reachable"] is True

    index = api.get("/")
    assert index.status_code == 200
    assert "text/html" in index.headers["content-type"]


def test_request_id_is_propagated(api: TestClient) -> None:
    response = api.get("/health", headers={"X-Request-ID": "abc-123"})
    assert response.headers["X-Request-ID"] == "abc-123"
    unsafe = api.get("/health", headers={"X-Request-ID": "bad id with spaces"})
    assert unsafe.headers["X-Request-ID"] != "bad id with spaces"


def test_openapi_documents_all_endpoints(api: TestClient) -> None:
    paths = api.get("/openapi.json").json()["paths"]
    assert {
        "/hospitals/bulk",
        "/hospitals/bulk/validate",
        "/hospitals/bulk/{batch_id}/status",
        "/hospitals/bulk/{batch_id}/resume",
        "/hospitals/bulk/{batch_id}",
        "/health",
    } <= paths.keys()
    bulk = paths["/hospitals/bulk"]["post"]
    assert {"200", "202", "413", "422"} <= bulk["responses"].keys()
