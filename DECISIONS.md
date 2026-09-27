# Design decisions

Every non-trivial choice in this project, in the form *Decision / Alternatives considered / Why*.
Start with **§0**: the measured behaviour of the upstream API drives most of what follows.

## Contents

- [0. Upstream API findings](#0-upstream-api-findings)
- **Shape of the service**: [D1 Framework](#d1-fastapi--httpx-async) · [D2 Sync by default, async opt-in](#d2-synchronous-by-default-async-opt-in) · [D3 Layering](#d3-layered-structure-with-a-service-container)
- **Performance**: [D4 Concurrency](#d4-bounded-concurrency-one-process-wide-semaphore-default-20) · [D5 Retries](#d5-retry-policy) · [D6 Cold starts](#d6-cold-starts-a-coalesced-warm-up-ping)
- **Correctness**: [D7 Exactly-once via reconciliation](#d7-non-idempotent-creates-at-least-once--reconciliation) · [D8 Failure policy](#d8-failure-policy-never-activate-partially-resume-or-explicit-rollback) · [D9 Resume](#d9-resume-semantics) · [D10 Activation verification](#d10-activation-is-verified-not-trusted) · [D11 Statuses](#d11-row-statuses-and-the-job-state-machine)
- **Input**: [D12 Invalid rows](#d12-invalid-rows-reject-the-file-by-default-skip_invalid-to-opt-in) · [D13 Validation rules](#d13-validation-rules) · [D14 Upload limits](#d14-upload-size-enforced-twice)
- **State and progress**: [D15 Repository](#d15-repository-with-atomic-operations-in-memory-implementation) · [D16 Progress](#d16-progress-in-process-broker-state-carrying-events-websocket--polling) · [D17 Background work](#d17-background-work-jobrunner-shield-graceful-drain) · [D18 Single worker](#d18-single-worker-by-design)
- **API and operations**: [D19 Errors and status codes](#d19-error-format-and-status-codes) · [D20 Logging](#d20-structured-logging-with-context-variables) · [D21 Health](#d21-health-check-is-shallow-by-default) · [D22 Testing](#d22-testing-strategy) · [D23 Deployment](#d23-packaging-and-deployment) · [D24 Frontend](#d24-frontend-one-static-page) · [D25 Not built](#d25-deliberately-not-built)

---

## 0. Upstream API findings

Explored on 2026-09-27 via `openapi.json` plus live calls. Every probe batch was deleted afterwards.

**Contract**

| Topic | Finding |
|---|---|
| Batch id | Sent in the **JSON body** as `creation_batch_id` (UUID, validated upstream). Not a query param or header. |
| Create | `POST /hospitals/` → **`200`** (not 201) with the full hospital. `name`, `address` required (min length 1); `phone` optional/nullable. No max length enforced (a 5,000-char name was accepted). |
| Validation errors | `422` in FastAPI's shape: `{"detail": [{"type", "loc": ["body","name"], "msg", "input", "ctx"}]}`. |
| Get batch | `GET /hospitals/batch/{id}` → list, or **`404` if the batch has no hospitals** (not `[]`). |
| Activate | `PATCH .../activate` → `{"activated_count": n}`. **Not idempotent**: calling it again returns **`400` "one or more hospitals in the batch are already active"**. `404` for an empty/unknown batch. |
| Delete batch | `DELETE /hospitals/batch/{id}` → `{"deleted_count": n}`; `404` if empty. |
| Extras not in the brief | `GET/PUT/DELETE /hospitals/{id}` exist. `DELETE /hospitals/{id}` → `204` (used for duplicate clean-up). `GET /` is a health check `{"status":"OK"}`. |
| Timestamps | `created_at` has **no timezone** (`2026-09-27T09:50:50.824234`). |

**Behaviour and performance**

| Topic | Finding |
|---|---|
| Cold start | First request after idle: **~23 s** (Render free tier; up to ~60 s documented). |
| Create latency | **~5.3 s per call**, consistently (probably an artificial delay). `422`s return in ~0.3 s, so the delay sits after validation. |
| Other latency | GET/PATCH/DELETE ≈ **0.3 s**. |
| Parallelism | 5 concurrent creates: 6.4 s wall; **20 concurrent: 6.6 s wall**. Upstream handles the delay concurrently, so fan-out is almost free for it. |
| Storage | **In-memory**: ids restarted at 1 after the cold start and `GET /hospitals/` was `[]`. **Data is lost whenever upstream restarts or sleeps.** |
| Shared | The service is shared with other candidates, so we cap concurrency and always clean up. |

These facts account for most of the design:

- latency-bound creates → concurrency ([D4](#d4-bounded-concurrency-one-process-wide-semaphore-default-20));
- cold starts → warm-up ([D6](#d6-cold-starts-a-coalesced-warm-up-ping));
- non-idempotent create/activate → reconciliation and verification ([D7](#d7-non-idempotent-creates-at-least-once--reconciliation), [D10](#d10-activation-is-verified-not-trusted));
- in-memory upstream → resume checks what still exists ([D9](#d9-resume-semantics));
- 404-for-empty → the client maps it to `[]`/`0`.

---

## D1. FastAPI + httpx (async)

- **Decision:** FastAPI on uvicorn, `httpx.AsyncClient` for upstream calls, Pydantic v2 models and settings.
- **Alternatives:** Flask + `requests` + a thread pool (the brief says Flask is preferred for minimalism); aiohttp.
- **Why:** The workload is 20 concurrent, 5-second, I/O-bound calls plus WebSockets. asyncio does that on one thread with no pool sizing. Flask would need threads for the fan-out and an extension for WebSockets. FastAPI also generates the OpenAPI docs the brief asks for, and the upstream is itself FastAPI, so its error shapes are familiar.

## D2. Synchronous by default, async opt-in

- **Decision:** By default `POST /hospitals/bulk` waits for the run and returns the spec's full result (`200`). `?async=true` returns `202` with `batch_id` and `links.status`/`links.websocket`. Both modes run the **same** background task; sync mode awaits it with `asyncio.shield`.
- **Alternatives:** Always `202` + polling (the "correct" shape for long jobs, but it breaks the spec's example response); always synchronous (no progress tracking possible).
- **Why:** It keeps the exact contract in the brief while enabling the progress/WebSocket bonus. Worst-case sync latency is a cold start plus about 7 s, which is fine for an HTTP request. Because the work is a task and not the request coroutine, a client that disconnects mid-run doesn't cancel the batch: it keeps going and stays visible via status.

## D3. Layered structure with a service container

- **Decision:** `api/` (HTTP only) → `services/` (orchestration, validation, progress) → `clients/` (all upstream I/O) and `repositories/` (state). A frozen `Services` dataclass is built in the lifespan and injected with `Depends`. `create_app(settings, upstream_transport=...)` is a factory.
- **Alternatives:** Module-level singletons; a DI framework.
- **Why:** Each layer can be tested alone. The processor never sees HTTP objects, and the client never sees domain rows. The `upstream_transport` seam lets tests swap the network for a mock without monkeypatching. A DI framework would be overkill for seven objects.

## D4. Bounded concurrency: one process-wide semaphore, default 20

- **Decision:** Rows are created with `asyncio.gather`. Every upstream request acquires a semaphore owned by the client (so it's **shared across all batches**), sized by `UPSTREAM_MAX_CONCURRENCY=20`. The httpx pool has the same size. The semaphore is held **per attempt**, not across backoff sleeps.
- **Alternatives:** A per-batch semaphore (N uploads → N×20 requests against a shared free-tier service); unbounded gather; a worker pool per batch.
- **Why:** 20 = max rows per CSV, so a lone upload finishes in one wave (~6.8 s against ~106 s sequential; see README). Upstream handled 20 concurrent calls with no slowdown (§0), and 20 is still modest for a shared service. A global cap is what protects upstream: extra concurrent uploads queue instead of multiplying load. Releasing the slot during backoff keeps throughput up when a few rows are retrying.

## D5. Retry policy

- **Decision:** At most 4 attempts. Retry **only** timeouts, network errors, dropped connections, and `408/425/429/500/502/503/504`. Never other 4xx. Exponential backoff with **full jitter** (`uniform(0, min(8, 0.5·2ⁿ))`), and `Retry-After` is honoured (capped). Everything is configurable.
- **Alternatives:** Fixed delays; equal jitter; retry everything; `tenacity`.
- **Why:** 4xx means our request is wrong and a retry can't fix it (a `422` fails the row immediately with `retryable=false`). Full jitter matters because 20 sibling rows tend to fail together (same upstream hiccup). Without it they'd retry in lock-step waves. The logic is ~40 lines and fully unit-tested, so a dependency wasn't justified. Retrying non-idempotent POSTs is safe here only because of [D7](#d7-non-idempotent-creates-at-least-once--reconciliation).

## D6. Cold starts: a coalesced warm-up ping

- **Decision:** Before fanning out, `client.warm_up()` sends `GET /` with a long timeout (90 s, at most 2 attempts), but only if upstream hasn't answered successfully in the last 10 minutes. An `asyncio.Lock` with a double check means concurrent batches share **one** ping. The app also fires a warm-up in the background at startup (not blocking readiness). Normal calls keep a 30 s read timeout.
- **Alternatives:** Just raise every timeout to 90 s (a genuinely hung call then blocks a slot for 90 s); let the first creates absorb the cold start (20 requests queue behind a sleeping dyno and may all time out together, burning retries).
- **Why:** It separates the one expected slow call from the steady state. Warm-up never raises: if upstream is really down, the per-row calls fail, retry and are reported, which keeps a single failure path.

## D7. Non-idempotent creates: at-least-once + reconciliation

- **Decision:** Treat `POST /hospitals/` as at-least-once, and make results effectively-once by **reconciling** against `GET /hospitals/batch/{id}` (the source of truth) before activation and at the start of every resume:
  1. Upstream records whose id no row claims (**orphans**) are matched to `failed` rows by content (`name`, `address`, `phone`) and **adopted**. This handles a create that succeeded but whose response was lost.
  2. Remaining orphans are true duplicates (created by a retry after a lost response) and are **deleted** via `DELETE /hospitals/{id}` before activation.
  3. Rows we believe are `created` but upstream no longer has (upstream restarted; §0) go back to `failed`, so resume re-creates them.
- **Alternatives:** Don't retry POSTs at all (a lost response then becomes a permanent failure *and* a stray record); an idempotency key (upstream doesn't support one); adopt/check before every single retry (racy with concurrent siblings that have identical content).
- **Why:** It's the only correct option with this upstream. Doing it once, after all row tasks settle, avoids races between siblings. It costs one ~0.3 s GET per run. It's covered by tests that commit a create upstream and then drop the response.
- **Limitation:** content matching can't tell apart two *identical* rows in the same CSV. The validator warns about duplicate rows for this reason.

## D8. Failure policy: never activate partially, resume or explicit rollback

- **Decision:** Activation happens only if **every** workable row exists upstream. Otherwise the batch ends `partial_failure` and stays **inactive**; created rows show `created`. The client then chooses `POST .../resume` or `DELETE /hospitals/bulk/{id}` (rollback → upstream `DELETE /hospitals/batch/{id}`). Nothing is rolled back automatically.
- **Alternatives:** Activate whatever succeeded (violates the brief's "once **all** hospitals are created"); auto-rollback on any failure (throws away 19 good rows because of one transient 503, and a later retry starts from zero).
- **Why:** The batch/activation mechanism upstream exists precisely so a partial import stays invisible. Keeping it inactive is safe *and* cheap to finish. Rollback is explicit because deleting data should be a deliberate act. A failed rollback restores the previous status and returns `502`, so the batch is never stuck in `rolling_back`.

## D9. Resume semantics

- **Decision:** `POST /hospitals/bulk/{id}/resume`:
  - reuses the **same** batch id;
  - reconciles first ([D7](#d7-non-idempotent-creates-at-least-once--reconciliation));
  - re-creates only `failed` rows, then activates.

  For `activation_failed` it only re-runs activation. It's idempotent: `completed` returns the current result without doing anything. It's guarded: the state change to `queued` is an atomic compare-and-set, so of N concurrent resumes exactly one starts and the rest get `409`. Resume after rollback is `409`.
- **Alternatives:** Resume with a new batch id (orphans the first batch's rows); retry blindly without reconciliation (duplicates whenever a "failed" create had actually landed); allow resume while processing.
- **Why:** "Never create duplicates for rows that already succeeded" needs both the reconcile-first step and the CAS guard. Reconciling also covers the upstream-lost-its-data case (§0), which a pure "retry failed rows" loop would miss.

## D10. Activation is verified, not trusted

- **Decision:** If `PATCH .../activate` errors (after retries), `GET` the batch. If every hospital is `active`, treat the batch as activated.
- **Alternatives:** Trust the error; don't retry PATCH.
- **Why:** Upstream returns `400` if anything is already active (§0). So a PATCH that was applied but whose response timed out, retried, then gets a 400 would be reported as a failure though the batch is live. Covered by a test that commits the PATCH and then drops the response.

## D11. Row statuses and the job state machine

- **Decision:** Row statuses: `pending`, `in_progress`, `created`, `created_and_activated`, `failed` (+ `error`, `retryable`), `skipped_invalid`, `rolled_back`. Job statuses: `queued → processing → completed | partial_failure | activation_failed`, then `rolling_back → rolled_back`. Allowed transitions live in one table (`ALLOWED_TRANSITIONS`) and are enforced by the repository.
- **Alternatives:** Booleans (`created`, `activated`, …); free-form strings.
- **Why:** Each status answers "what exists upstream and is it visible?" without looking at other fields. A single transition table makes illegal operations (rollback mid-run, resume after rollback) impossible by construction and easy to test exhaustively. The response is additive: the spec's fields keep their names and meaning, and extra fields (`status`, `skipped_hospitals`, `pending_hospitals`, `resumable`, `runs`, `links`, …) come alongside them. `processed_hospitals` = rows that exist upstream; `failed_hospitals` = upstream failures; skipped rows are counted separately so the spec's two numbers stay truthful.

## D12. Invalid rows: reject the file by default, `skip_invalid` to opt in

- **Decision:** Any row error → `422` with the full report and nothing sent upstream. `?skip_invalid=true` processes valid rows, keeps invalid ones as `skipped_invalid`, and they don't block activation. A file with zero valid rows is always `422`.
- **Alternatives:** Always skip invalid rows silently; always reject.
- **Why:** For a hospital directory, a silent partial import is the worse failure, so strict is the right default. The opt-in covers "import what you can", and it's what makes `skipped_invalid` meaningful.

## D13. Validation rules

- **Decision:** One pure `CsvValidator` backs both `/validate` and `/bulk`. It separates file errors, row errors (with `row` = data row number and `line` = physical line) and warnings.

  | Rule | Choice | Reason |
  |---|---|---|
  | Headers | Case-insensitive, trimmed, stray BOM removed. Required `name`, `address`; optional `phone`. | Real spreadsheets vary in case and spacing. |
  | Unknown / duplicate columns | Error. A hint is shown if the header contains `;` or a tab. | A typo like `phone_number` must not silently drop data. |
  | Empty trailing header cells (`a,b,c,`) | Warning; the column is ignored. | Excel adds these. |
  | Encoding | UTF-8 with the BOM stripped; NUL bytes mean the file is binary. | |
  | Values | Trimmed, and internal whitespace (including newlines in quoted cells) collapsed. | |
  | Length limits | name 200, address 500, phone 40. | Upstream has no limits (§0); we set sane ones. |
  | Phone | Lenient pattern (`+`, digits, spaces, `()-./`, extension `x`/`ext`/`#`) with at least 3 digits. | |
  | Blank lines | Skipped with one aggregated warning. Row numbers stay dense (row N = Nth hospital). | |
  | Duplicate rows | Warning only (case-insensitive). | They might be intentional; see the D7 limitation. |
  | Content type | Browsers/curl send `application/vnd.ms-excel` or `application/octet-stream` for CSVs, so those are accepted and clearly wrong types (PDF, images, JSON) rejected. | The `.csv` extension is the primary signal. |

- **Why:** One implementation means `/validate` can never disagree with `/bulk`. Stable machine-readable `code`s make the report usable by UIs (ours renders it).

## D14. Upload size enforced twice

- **Decision:** (1) ASGI middleware rejects requests whose `Content-Length` exceeds the limit + 64 KiB multipart overhead with `413`, **before parsing**. (2) The endpoint reads at most `limit + 1` bytes and the validator flags `file_too_large` (`413` from `/bulk`, a report entry from `/validate`).
- **Why:** Starlette's multipart parser spools the entire file to a temp file before the endpoint runs, so (2) alone would still let a client make us write gigabytes to disk. (1) closes that for all normal clients. A reverse-proxy limit is the production-grade answer for chunked uploads.

## D15. Repository with atomic operations, in-memory implementation

- **Decision:** `BatchRepository` exposes `add`, `get` (returns a **deep-copied snapshot**), `transition` (**compare-and-set** on status, which can also set fields and apply row updates in the *same* atomic step) and `update_rows`. `InMemoryBatchRepository` guards a dict with a `threading.Lock`, and evicts the oldest *finished* jobs beyond `MAX_STORED_BATCHES`.
- **Alternatives:** A get-mutate-save API; `asyncio.Lock`; Redis/Postgres now.
- **Why:**
  - 20 row tasks update one job concurrently, and two requests can race to resume. Get-mutate-save loses updates the moment the store is out of process. Each operation here maps onto one atomic primitive later: `UPDATE … WHERE status IN (…)`, or a Lua script / `HSET`.
  - Snapshots stop callers from mutating stored state by accident, just as a real store would.
  - Atomic transition + row updates means no observer ever sees a `completed` job whose rows still say `created`.
  - `threading.Lock` is correct for both threads and coroutines because critical sections never `await`. An `asyncio.Lock` isn't thread-safe.
  - In-memory storage is allowed by the brief. Eviction bounds memory.

## D16. Progress: in-process broker, state-carrying events, WebSocket + polling

- **Decision:** The processor publishes `row` events (the row's **full** state + counts) and `job` events (status; the final one includes the whole result) to a `ProgressBroker` of bounded per-subscriber queues. The WebSocket handler **subscribes before reading the snapshot**, sends `snapshot`, forwards events, and closes after the terminal event. Polling uses the same `BatchResult` shape. The UI uses the WebSocket and falls back to polling.
- **Alternatives:** Server-Sent Events; delta events; polling only.
- **Why:**
  - Subscribing first means no event can fall between the snapshot and the stream. Because events carry state rather than deltas, any overlap is harmless.
  - A full queue drops events instead of blocking processing, so a stuck client can't slow a batch.
  - The handler runs its sender and disconnect watcher in an **anyio task group**, so neither can outlive the connection. An earlier `asyncio.wait` version orphaned tasks on cancellation, and a flaky test caught it.
  - With several instances, the broker becomes Redis pub/sub on `batch:{id}` behind the same interface.

## D17. Background work: `JobRunner`, `shield`, graceful drain

- **Decision:** Runs are `asyncio` tasks held by a `JobRunner` (strong references, so the event loop can't garbage-collect them). On shutdown the lifespan waits up to `SHUTDOWN_GRACE_SECONDS`, then cancels. A cancelled run marks its unfinished rows `failed` (retryable, "interrupted") and ends `partial_failure`, so it stays resumable instead of stuck in `processing`. The container `exec`s uvicorn so SIGTERM reaches it.
- **Alternatives:** FastAPI `BackgroundTasks` (tied to the response lifecycle, with no drain or cancellation handling); Celery/RQ (excluded by the brief, and overkill here).
- **Why:** Small, explicit, testable, and it's the seam where a real queue plugs in later.

## D18. Single worker by design

- **Decision:** One uvicorn process (no `--workers N`).
- **Why:** State is in memory. With several workers, a status request or WebSocket could land on a process that doesn't know the batch. One async process handles the load comfortably, since the bottleneck is upstream latency, not our CPU. Scaling out requires [D15](#d15-repository-with-atomic-operations-in-memory-implementation)/[D16](#d16-progress-in-process-broker-state-carrying-events-websocket--polling) to move to Redis/Postgres. That's documented in the README instead of pretending otherwise.

## D19. Error format and status codes

- **Decision:** Every error is `{"error": {"code", "message", "details"}}`, including FastAPI's request-validation errors and 404s, which are re-shaped. Codes:

  | Status | When |
  |---|---|
  | `422` | Invalid CSV (`details` = the validation report) or an invalid request |
  | `413` | Upload too large |
  | `404` | Unknown batch |
  | `409` | Illegal state change |
  | `502` | Upstream failure during rollback |
  | `200` | A batch with failed rows (`status: partial_failure`) |

- **Alternatives:** `207 Multi-Status` for partial failures; FastAPI's default `{"detail": ...}`.
- **Why:** The request itself succeeded and the body describes per-row outcomes. `207` is WebDAV-specific and poorly supported by clients. One error shape means clients write one error handler.

## D20. Structured logging with context variables

- **Decision:** JSON logs to stdout. `batch_id` and `request_id` come from `contextvars` and are injected by a logging filter. Structured fields go through `extra=`. The request id is taken from `X-Request-ID` (validated) or generated, and echoed in the response.
- **Why:** asyncio tasks copy the context when created, so setting `batch_id` once per run tags every line emitted by the 20 row tasks and the client's retry logs, with no parameter threading. A batch's logs also carry the id of the request that started it. JSON is what log platforms (Render, Datadog, …) index; `LOG_FORMAT=text` exists for humans.

## D21. Health check is shallow by default

- **Decision:** `GET /health` doesn't call upstream. `?deep=true` pings it.
- **Why:** The platform health check must reflect *our* process. If it depended on upstream, a sleeping upstream would get our service restarted, and every probe would wake upstream for nothing.

## D22. Testing strategy

- **Decision:** A **stateful fake upstream** (`tests/fake_upstream.py`) reproduces the observed contract (200 on create, 404 for an empty batch, 400 on re-activation), plugged in through `httpx.MockTransport`. It injects faults per operation or per row: HTTP codes, exceptions, and `COMMIT_THEN_TIMEOUT` (apply, then lose the response). **respx** tests the client's retry/backoff/error mapping at the route level. There's one opt-in **integration test** against the real API that cleans up after itself, plus a smoke-test script and a benchmark script that also clean up.
- **Why:** Most bugs worth catching live in multi-step interactions (retry → duplicate → reconcile → activate), which route-level stubs express badly and a stateful fake expresses naturally. Result: 145 tests, 99% coverage, ~6 s runtime, deterministic. Live-progress tests use a gate (`create_gate`) instead of sleeps.

## D23. Packaging and deployment

- **Decision:**
  - Multi-stage `python:3.13-slim` image with a venv copied into the runtime stage, a non-root user, a stdlib `HEALTHCHECK`, and `--proxy-headers`.
  - Dependencies locked **universally** with `uv pip compile --universal`, so the lock is valid on Linux and Windows (`uvloop` gets a platform marker).
  - Render Blueprint with the **Docker runtime** on the free plan in `oregon` (same region as upstream), health check `/health`.
  - CI runs lint, types, tests with a coverage gate, and a Docker build with a health probe.
- **Alternatives:** Render's native Python runtime (faster builds); `pip freeze` for the lock.
- **Why:**
  - Deploying the Dockerfile means the deployed artifact *is* the tested container; Docker wasn't available on the dev machine, and Render/CI builds cover that gap.
  - `pip freeze` on Windows would have produced a Windows-only lock.
  - Requires Python 3.12+ (PEP 695 generics); the image uses 3.13.
  - Render's free tier sleeps too, so this service has its own cold start (documented in SUBMISSION).

## D24. Frontend: one static page

- **Decision:** `app/static/index.html`, served at `/`, in vanilla JS with no build step. It covers upload with drag-and-drop, the validation report and preview, live progress (WebSocket, polling fallback), the result table, and resume/rollback buttons. All CSV-derived text is inserted with `textContent`, never `innerHTML`. Light and dark themes follow the OS.
- **Why:** It's a fullstack role, so a reviewer can exercise the whole system without curl. Zero toolchain means nothing to build or break. Verified end-to-end in headless Edge (validation, live WebSocket run, rollback, no console errors).

## D25. Deliberately not built

| Item | Reason |
|---|---|
| Kafka/Celery/Redis/Postgres | Excluded by the brief. The seams exist (repository, broker, runner). |
| Auth, rate limiting | Out of scope for the exercise. Listed under "at scale" in the README. |
| Idempotency-Key on `POST /bulk` | Needs durable storage to be meaningful across restarts. Listed as the first follow-up. |
| Adaptive (AIMD) upstream limiter | A fixed cap is enough: upstream showed no degradation at 20. |
