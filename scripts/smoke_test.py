"""Post-deploy smoke test: exercise every endpoint of a running instance, then clean up.

    python -m scripts.smoke_test http://localhost:8000
    python -m scripts.smoke_test https://<your-service>.onrender.com

Prints a Markdown report. The batch it creates is rolled back (deleted upstream) at the end,
and that is verified directly against the upstream API.
"""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import httpx
from websockets.sync.client import connect

ROOT = Path(__file__).resolve().parent.parent
UPSTREAM = "https://hospital-directory.onrender.com"

report: list[str] = []


def step(name: str, ok: bool, detail: str) -> None:
    report.append(f"| {'PASS' if ok else 'FAIL'} | {name} | {detail} |")
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}", flush=True)
    if not ok:
        raise SystemExit(finish(1))


def finish(code: int) -> int:
    print("\n| Result | Step | Detail |\n|---|---|---|")
    print("\n".join(report))
    return code


def upload(path: Path) -> dict[str, Any]:
    return {"file": (path.name, path.read_bytes(), "text/csv")}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("base_url")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")
    ws_base = base.replace("https://", "wss://").replace("http://", "ws://")

    # Free-tier services (this one *and* upstream) may be asleep: be patient on the first call.
    with httpx.Client(base_url=base, timeout=httpx.Timeout(120.0)) as http:
        started = time.perf_counter()
        health = http.get("/health")
        step(
            "GET /health",
            health.status_code == 200 and health.json()["status"] == "ok",
            f"{health.status_code} in {time.perf_counter() - started:.1f}s (includes cold start)",
        )

        report_body = http.post(
            "/hospitals/bulk/validate", files=upload(ROOT / "samples" / "hospitals_invalid.csv")
        ).json()
        step(
            "POST /hospitals/bulk/validate (invalid sample)",
            report_body["valid"] is False and report_body["invalid_rows"] == 5,
            f"valid={report_body['valid']}, errors={len(report_body['errors'])}, "
            f"warnings={len(report_body['warnings'])}",
        )

        too_many = http.post(
            "/hospitals/bulk", files=upload(ROOT / "samples" / "hospitals_21_rows.csv")
        )
        step(
            "POST /hospitals/bulk (21 rows) is rejected",
            too_many.status_code == 422,
            f"{too_many.status_code} {too_many.json()['error']['code']}",
        )

        started = time.perf_counter()
        accepted = http.post(
            "/hospitals/bulk?async=true",
            files=upload(ROOT / "samples" / "hospitals_missing_phone.csv"),
        )
        body = accepted.json()
        batch_id = body["batch_id"]
        step("POST /hospitals/bulk?async=true", accepted.status_code == 202, f"batch {batch_id}")

        try:
            events: list[dict[str, Any]] = []
            with connect(f"{ws_base}{body['links']['websocket']}", open_timeout=60) as ws:
                for message in ws:
                    events.append(json.loads(message))
                    if events[-1]["type"] == "job" and events[-1].get("result"):
                        break
            final = events[-1].get("result") or events[-1]
            step(
                "WS /ws/bulk/{id} streams progress",
                final.get("status") == "completed",
                f"{len(events)} events ({sum(e['type'] == 'row' for e in events)} row updates), "
                f"final status {final.get('status')} after {time.perf_counter() - started:.1f}s",
            )

            status = http.get(body["links"]["status"]).json()
            step(
                "GET /hospitals/bulk/{id}/status",
                status["status"] == "completed" and status["batch_activated"] is True,
                f"processed={status['processed_hospitals']}/{status['total_hospitals']}, "
                f"activated={status['batch_activated']}, "
                f"processing_time={status['processing_time_seconds']}s",
            )

            resumed = http.post(body["links"]["resume"]).json()
            step(
                "POST /hospitals/bulk/{id}/resume (idempotent no-op)",
                resumed["runs"] == 1 and resumed["status"] == "completed",
                f"runs={resumed['runs']}",
            )
        finally:
            rolled = http.delete(body["links"]["rollback"])
            step(
                "DELETE /hospitals/bulk/{id} (rollback)",
                rolled.status_code == 200 and rolled.json()["status"] == "rolled_back",
                f"{rolled.status_code} {rolled.json().get('status')}",
            )

    leftover = httpx.get(f"{UPSTREAM}/hospitals/batch/{batch_id}", timeout=60)
    step(
        "Upstream batch deleted",
        leftover.status_code == 404,
        f"GET upstream /hospitals/batch/{{id}} -> {leftover.status_code}",
    )
    return finish(0)


if __name__ == "__main__":
    sys.exit(main())
