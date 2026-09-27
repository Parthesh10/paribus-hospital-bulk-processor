"""Wire models for the Hospital Directory API (mirrors its OpenAPI schema)."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class HospitalCreate(BaseModel):
    name: str
    address: str
    phone: str | None = None
    creation_batch_id: UUID | None = None


class Hospital(BaseModel):
    id: int
    name: str
    address: str
    phone: str | None = None
    creation_batch_id: UUID | None = None
    active: bool = True
    created_at: datetime | None = None
