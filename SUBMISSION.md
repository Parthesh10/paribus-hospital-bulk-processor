# Submission: Hospital Bulk Processing System

| Item | Value |
|---|---|
| GitHub repository | **Not yet published.** Blocked: `gh` is installed but not authenticated on the build machine. See [Step 1](#step-1-publish-to-github-2-minutes). |
| Live URL | **Not yet deployed.** Blocked: Render deploys from a Git repository, so this waits on Step 1. See [Step 2](#step-2-deploy-on-render-5-minutes). |
| Everything else | Done and verified locally against the **real** upstream API (results below). |

## Definition of Done

| | Item | Status |
|---|---|---|
| ✅ | Upstream API explored, findings documented | [DECISIONS.md §0](DECISIONS.md#0-upstream-api-findings) |
| ✅ | `POST /hospitals/bulk` works end-to-end against the real upstream, spec response shape | 20 rows created + activated in 6.45 s; see smoke test below |
| ✅ | Bonus 1: performance (bounded concurrency, pooled client, retries + jitter, cold-start warm-up, env-configurable, benchmark) | 106.5 s → 6.8 s (15.8x) |
| ✅ | Bonus 2: progress tracking (polling endpoint + WebSocket, `?async=true` → 202) | |
| ✅ | Bonus 3: resume (same batch id, failed rows only, idempotent, no duplicates) + explicit rollback | |
| ✅ | Bonus 4: CSV validation endpoint (all listed edge cases), same validator inside `/bulk` | |
| ✅ | Bonus 5: tests: 145 passing, 99% coverage; opt-in real-upstream integration test passing | |
| ⚠️ | Bonus 6: Dockerfile + docker-compose.yml written; **not built locally** (Docker isn't installed on the build machine). The Render deploy (Docker runtime) and the CI workflow's `docker build` + `/health` probe both verify it. | See [Step 3](#step-3-optional-verify-docker-locally) |
| ✅ | `ruff check`, `ruff format --check`, `mypy --strict`: all clean | |
| ✅ | Runs locally with one command (`uvicorn app.main:app`) | |
| ✅ | README, DECISIONS, SUBMISSION | |
| ✅ | Clean git history (logical commits) | |
| ⏳ | Pushed to public GitHub | Step 1 |
| ⏳ | Deployed on Render and smoke-tested | Steps 2 and 4 |
| ✅ | Every test batch created upstream deleted | Checked: `GET /hospitals/` upstream returned 0 records at the end of the session |

## Steps to finish

Run these from the project folder.

### Step 1: Publish to GitHub (~2 minutes)

```bash
gh auth login                      # GitHub.com → HTTPS → login with a web browser
gh repo create paribus-hospital-bulk-processor --public --source . --remote origin --push \
  --description "Bulk CSV import service for the Hospital Directory API (FastAPI, async, WebSocket progress)"
gh repo view --web                 # confirm; CI (lint, types, tests, docker build) starts automatically
```

The repo URL will be `https://github.com/<your-username>/paribus-hospital-bulk-processor`.

### Step 2: Deploy on Render (~5 minutes)

The repo contains a Blueprint ([`render.yaml`](render.yaml)): a Docker web service on the free
plan, health check `/health`, env vars preset.

1. Open <https://dashboard.render.com/blueprints> and click **New Blueprint Instance**.
2. Connect GitHub if asked, and select `paribus-hospital-bulk-processor`.
3. Render reads `render.yaml` and shows one web service, `hospital-bulk-processor`. Click **Apply** (or **Deploy Blueprint**).
4. Wait for the build (~3-5 minutes for the Docker image) until the service shows **Live**.
5. Copy the URL shown at the top of the service page, e.g. `https://hospital-bulk-processor.onrender.com`. Render appends a suffix if the name is taken.

Shortcut: open `https://render.com/deploy?repo=https://github.com/<your-username>/paribus-hospital-bulk-processor`.

### Step 3 (optional): Verify Docker locally

```bash
docker compose up --build          # then open http://localhost:8000
curl localhost:8000/health
```

### Step 4: Post-deploy smoke test

```bash
python -m venv .venv && .venv/Scripts/activate      # macOS/Linux: source .venv/bin/activate
pip install -r requirements-dev.txt
python -m scripts.smoke_test https://<your-service>.onrender.com
```

It checks, in order: health, validation of the invalid sample, rejection of the 21-row file,
async bulk create, the WebSocket stream to completion, status, idempotent resume, rollback, and
finally that the batch is gone upstream. It prints a Markdown table you can paste below.
The first request can take ~1 minute: both free-tier services may be asleep.

Then fill in the two URLs at the top of this file and in the README's "Live" line.

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

## Verification results (local instance → real upstream API)

**Smoke test** (`python -m scripts.smoke_test http://localhost:8766`, 2026-09-27):

| Result | Step | Detail |
|---|---|---|
| PASS | GET /health | 200 in 2.0s |
| PASS | POST /hospitals/bulk/validate (invalid sample) | valid=False, errors=5, warnings=2 |
| PASS | POST /hospitals/bulk (21 rows) is rejected | 422 csv_validation_failed |
| PASS | POST /hospitals/bulk?async=true | batch 9c092b2a-dbe6-4f72-ab06-3908e3e9dabb |
| PASS | WS /ws/bulk/{id} streams progress | 7 events (5 row updates), final status completed after 5.8s |
| PASS | GET /hospitals/bulk/{id}/status | processed=5/5, activated=True, processing_time=5.843s |
| PASS | POST /hospitals/bulk/{id}/resume (idempotent no-op) | runs=1 |
| PASS | DELETE /hospitals/bulk/{id} (rollback) | 200 rolled_back |
| PASS | Upstream batch deleted | GET upstream /hospitals/batch/{id} -> 404 |

**Synchronous 20-row import** (`curl -F file=@samples/hospitals_valid_20.csv .../hospitals/bulk`):
`200` in 6.67 s; `processed_hospitals=20`, `failed_hospitals=0`, `batch_activated=true`, every
row `created_and_activated`. Upstream confirmed 20 active records. Then rolled back, and upstream
returned `404` for the batch.

**UI** (headless Edge via Playwright): validation report rendered (7 rows, 5 errors, 2 warnings),
5-row import streamed live over WebSocket to `completed`, rollback worked, no console errors.

**Benchmark** (`python -m scripts.benchmark`, 20 rows, real upstream):

| Concurrency | Wall time (s) | Speed-up | Rows/s |
|---:|---:|---:|---:|
| 1 | 106.45 | 1.0x | 0.19 |
| 5 | 22.71 | 4.7x | 0.88 |
| 10 | 11.53 | 9.2x | 1.73 |
| 20 | 6.76 | 15.8x | 2.96 |

**Tests and static checks:**

```
pytest                 → 145 passed, 1 deselected (integration), ~6 s
pytest --cov           → 99% line coverage (branch coverage on)
pytest -m integration  → 1 passed (real upstream, cleans up after itself)
ruff check / format    → clean
mypy (strict)          → Success: no issues found in 26 source files
```

## Post-deploy smoke test (deployed URL)

_To be filled in after Step 4._

## Notes for the reviewer

- On Render's free tier this service sleeps too: the first request after ~15 minutes idle takes
  ~30-60 s, and upstream may be asleep as well. The service warms upstream in the background at
  boot and before each batch.
- Batch state is in memory (allowed by the brief), so a restart forgets batch *records*, not the
  hospitals upstream. The scaling path is in the README and in DECISIONS D15-D18.
