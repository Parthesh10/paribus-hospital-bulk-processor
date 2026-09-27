"""Service container and FastAPI dependency providers."""

from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends
from starlette.requests import HTTPConnection

from app.clients.hospital_directory import HospitalDirectoryClient
from app.config import Settings
from app.repositories.batch_repository import BatchRepository
from app.services.bulk_processor import BulkProcessor
from app.services.csv_validator import CsvValidator
from app.services.job_runner import JobRunner
from app.services.progress import ProgressBroker


@dataclass(frozen=True)
class Services:
    """Everything with process lifetime, built once in the app lifespan."""

    settings: Settings
    client: HospitalDirectoryClient
    repository: BatchRepository
    broker: ProgressBroker
    runner: JobRunner
    processor: BulkProcessor
    validator: CsvValidator


def get_services(conn: HTTPConnection) -> Services:
    # HTTPConnection works for both HTTP requests and WebSockets.
    services: Services = conn.app.state.services
    return services


ServicesDep = Annotated[Services, Depends(get_services)]
