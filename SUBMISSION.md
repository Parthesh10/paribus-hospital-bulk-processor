# Submission: Hospital Bulk Processing System

| Item | Value |
|---|---|
| **GitHub repository** | <https://github.com/Parthesh10/paribus-hospital-bulk-processor> |
| **Live URL** | <https://hospital-bulk-processor-jvl7.onrender.com> |
| UI | <https://hospital-bulk-processor-jvl7.onrender.com/> |
| API docs | <https://hospital-bulk-processor-jvl7.onrender.com/docs> |

> Render free tier: the first request after ~15 minutes idle takes ~30-60 s while the service
> wakes, and the upstream Hospital Directory API may be asleep too. Later requests are fast.

## Definition of Done

| | Item | Evidence |
|---|---|---|
| ✅ | Upstream API explored, findings documented | [DECISIONS.md §0](DECISIONS.md#0-upstream-api-findings) |
| ✅ | `POST /hospitals/bulk` works end-to-end against the real upstream, spec response shape | Live: 20 rows created + activated, `200` in 6.4 s (below) |
| ✅ | Bonus 1: performance | 106.5 s sequential → 6.8 s concurrent (15.8x) |
| ✅ | Bonus 2: progress tracking (polling + WebSocket, `?async=true` → 202) | Live WebSocket stream verified through Render's proxy (`wss://`) |
| ✅ | Bonus 3: resume (same batch id, failed rows only, idempotent, no duplicates) + explicit rollback | Unit/API tests; live idempotent resume + rollback |
| ✅ | Bonus 4: CSV validation endpoint, same validator inside `/bulk` | Live: invalid sample → 5 errors, 2 warnings; 21 rows → `422` |
| ✅ | Bonus 5: tests (145 passing, 99% coverage, opt-in real-upstream integration test) | CI + local |
| ✅ | Bonus 6: Dockerfile + docker-compose | Built and health-probed in GitHub Actions; the Render service runs this Dockerfile |
| ✅ | `ruff`, `ruff format`, `mypy --strict` clean | CI |
| ✅ | Runs with one command (`uvicorn app.main:app` / `docker compose up`) | |
| ✅ | README, DECISIONS, SUBMISSION | |
| ✅ | Clean git history, public GitHub repo | |
| ✅ | Deployed on Render and smoke-tested | Results below |
| ✅ | Every test batch created upstream deleted | Each run rolls back and verifies `404` upstream |

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/` | Web UI (upload, validate, live progress, resume/rollback) |
| `GET` | `/docs`, `/redoc` | Interactive API docs |
| `POST` | `/hospitals/bulk` | Bulk create from CSV (`?async=true`, `?skip_invalid=true`) |
| `POST` | `/hospitals/bulk/validate` | Validate a CSV without calling upstream |
| `GET` | `/hospitals/bulk/{batch_id}/status` | Batch status and per-row progress |
| `WS` | `/ws/bulk/{batch_id}` | Progress event stream |
| `POST` | `/hospitals/bulk/{batch_id}/resume` | Retry failed rows / activation |
| `DELETE` | `/hospitals/bulk/{batch_id}` | Roll back (delete the batch upstream) |
| `GET` | `/health` | Health (`?deep=true` pings upstream) |

Try it:

```bash
L=https://hospital-bulk-processor-jvl7.onrender.com
curl -F "file=@samples/hospitals_valid_20.csv" $L/hospitals/bulk              # create + activate
curl -F "file=@samples/hospitals_invalid.csv" $L/hospitals/bulk/validate      # validation report
curl -X DELETE $L/hospitals/bulk/<batch_id>                                   # clean up afterwards
```

## Post-deploy smoke test (live URL)

`python -m scripts.smoke_test https://hospital-bulk-processor-jvl7.onrender.com`, 2026-09-27, against the real upstream:

| Result | Step | Detail |
|---|---|---|
| PASS | GET /health | 200 in 0.8s |
| PASS | POST /hospitals/bulk/validate (invalid sample) | valid=False, errors=5, warnings=2 |
| PASS | POST /hospitals/bulk (21 rows) is rejected | 422 csv_validation_failed |
| PASS | POST /hospitals/bulk?async=true | batch 24a116a5-b08a-4eb1-9135-9f2311863c47 |
| PASS | WS /ws/bulk/{id} streams progress | 12 events (10 row updates), final status completed after 8.4s |
| PASS | GET /hospitals/bulk/{id}/status | processed=5/5, activated=True, processing_time=7.901s |
| PASS | POST /hospitals/bulk/{id}/resume (idempotent no-op) | runs=1 |
| PASS | DELETE /hospitals/bulk/{id} (rollback) | 200 rolled_back |
| PASS | Upstream batch deleted | GET upstream /hospitals/batch/{id} -> 404 |

**Synchronous 20-row import on the live URL** (`curl -F file=@samples/hospitals_valid_20.csv $L/hospitals/bulk`):
`200` in 6.44 s; `total_hospitals=20`, `processed_hospitals=20`, `failed_hospitals=0`,
`processing_time_seconds=5.921`, `batch_activated=true`, every row `created_and_activated`.
Upstream confirmed 20 active records. Rolled back afterwards; upstream then returned `404` for
the batch.

**UI on the live URL** (headless Edge via Playwright): validation report rendered (7 rows,
5 errors, 2 warnings), 5-row import streamed live over WebSocket to `completed`, rollback
worked, no console errors.

## Other verification

**Benchmark** (`python -m scripts.benchmark`, 20 rows, real upstream):

| Concurrency | Wall time (s) | Speed-up | Rows/s |
|---:|---:|---:|---:|
| 1 | 106.45 | 1.0x | 0.19 |
| 5 | 22.71 | 4.7x | 0.88 |
| 10 | 11.53 | 9.2x | 1.73 |
| 20 | 6.76 | 15.8x | 2.96 |

**Tests and static checks** (locally and in [GitHub Actions](https://github.com/Parthesh10/paribus-hospital-bulk-processor/actions)):

```
pytest                 → 145 passed, 1 deselected (integration), ~6 s
pytest --cov           → 99% line coverage (branch coverage on)
pytest -m integration  → 1 passed (real upstream, cleans up after itself)
ruff check / format    → clean
mypy (strict)          → no issues in 26 source files
docker build + /health → passing (CI "docker" job)
```

## Deployment details

- Render web service `hospital-bulk-processor` (Docker runtime, free plan, Oregon: same region
  as the upstream API), auto-deploys on push to `main`. Created through the Render API with the
  same settings as [`render.yaml`](render.yaml), except the health-check path, which that API
  can't set. Render therefore checks the open port; setting *Settings → Health Check Path* to
  `/health` in the dashboard (or recreating from the Blueprint) makes it probe the endpoint.
- Batch state is in memory (allowed by the brief), so a restart or free-tier sleep forgets batch
  *records*, not the hospitals upstream. The scaling path is in the README and DECISIONS D15-D18.
