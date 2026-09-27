"""Benchmark: sequential vs concurrent bulk processing against the REAL upstream.

Runs the production code path (validator -> BulkProcessor -> HospitalDirectoryClient) for the
same CSV at several concurrency limits, then rolls every batch back so nothing is left upstream.

    python -m scripts.benchmark                       # 20-row sample, concurrency 1,5,10,20
    python -m scripts.benchmark --concurrency 1 20 --repeat 2
"""

import argparse
import asyncio
import statistics
import time
from collections.abc import Sequence
from pathlib import Path

import httpx

from app.clients.hospital_directory import HospitalDirectoryClient
from app.config import Settings
from app.models.domain import JobStatus
from app.repositories.batch_repository import InMemoryBatchRepository
from app.services.bulk_processor import BulkProcessor
from app.services.csv_validator import CsvValidator, ParsedRow
from app.services.job_runner import JobRunner
from app.services.progress import ProgressBroker

ROOT = Path(__file__).resolve().parent.parent


async def run_once(
    settings: Settings, rows: Sequence[ParsedRow], concurrency: int
) -> tuple[float, JobStatus]:
    tuned = settings.model_copy(update={"upstream_max_concurrency": concurrency})
    async with HospitalDirectoryClient.build_http_client(tuned) as http:
        client = HospitalDirectoryClient.from_settings(tuned, http)
        await client.warm_up()  # measure steady state, not Render's cold start
        processor = BulkProcessor(client, InMemoryBatchRepository(), ProgressBroker(), JobRunner())
        job = await processor.create_batch(rows)
        started = time.perf_counter()
        try:
            result = await processor.run(job.batch_id)
            return time.perf_counter() - started, result.status
        finally:
            await processor.rollback(job.batch_id)  # always clean up upstream


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--csv", type=Path, default=ROOT / "samples" / "hospitals_valid_20.csv")
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 5, 10, 20])
    parser.add_argument("--repeat", type=int, default=1)
    args = parser.parse_args()

    settings = Settings(log_level="WARNING", log_format="text")
    validator = CsvValidator(max_rows=settings.max_csv_rows, max_bytes=settings.max_upload_bytes)
    parsed = validator.validate(args.csv.read_bytes()).rows
    print(f"Upstream: {settings.upstream_base_url}  CSV: {args.csv.name} ({len(parsed)} rows)")

    results: dict[int, list[float]] = {}
    for concurrency in args.concurrency:
        for attempt in range(1, args.repeat + 1):
            seconds, status = await run_once(settings, parsed, concurrency)
            print(f"  concurrency={concurrency:<3} run={attempt}  {seconds:7.2f}s  {status}")
            if status is JobStatus.COMPLETED:
                results.setdefault(concurrency, []).append(seconds)

    baseline = statistics.median(results[min(results)]) if results else 0.0
    print("\n| Concurrency | Median wall time (s) | Speed-up vs sequential | Rows/s |")
    print("|---:|---:|---:|---:|")
    for concurrency, samples in results.items():
        median = statistics.median(samples)
        print(
            f"| {concurrency} | {median:.2f} | {baseline / median:.1f}x | "
            f"{len(parsed) / median:.2f} |"
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except httpx.HTTPError as exc:  # pragma: no cover - operator feedback
        raise SystemExit(f"upstream error: {exc}") from exc
