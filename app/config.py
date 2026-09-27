"""Application settings, loaded from environment variables (and an optional `.env`)."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All tunables live here so behaviour can be changed per environment without code edits."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- Upstream Hospital Directory API -------------------------------------------------------
    upstream_base_url: HttpUrl = Field(
        default=HttpUrl("https://hospital-directory.onrender.com"),
        description="Base URL of the Hospital Directory API.",
    )
    upstream_max_concurrency: int = Field(
        default=20,
        ge=1,
        le=100,
        description="Process-wide cap on in-flight upstream requests (shared by all batches).",
    )
    upstream_connect_timeout: float = Field(default=10.0, gt=0)
    upstream_read_timeout: float = Field(
        default=30.0,
        gt=0,
        description="Per-request read timeout. Upstream creates take ~5.3s when warm.",
    )
    upstream_cold_start_timeout: float = Field(
        default=90.0,
        gt=0,
        description="Timeout for the warm-up ping; Render free-tier cold starts take 25-60s.",
    )
    upstream_warm_ttl_seconds: float = Field(
        default=600.0,
        ge=0,
        description="Skip the warm-up ping if upstream answered successfully within this window.",
    )
    upstream_warm_on_startup: bool = Field(
        default=True, description="Fire a background warm-up ping when the app starts."
    )
    upstream_max_attempts: int = Field(
        default=4, ge=1, le=10, description="Total attempts per request (1 = no retries)."
    )
    upstream_backoff_base: float = Field(default=0.5, ge=0, description="Backoff base in seconds.")
    upstream_backoff_max: float = Field(default=8.0, ge=0, description="Backoff cap in seconds.")

    # --- CSV limits ----------------------------------------------------------------------------
    max_csv_rows: int = Field(default=20, ge=1, description="Maximum hospitals per CSV.")
    max_upload_bytes: int = Field(default=256 * 1024, ge=1024, description="Upload size limit.")

    # --- Storage -------------------------------------------------------------------------------
    max_stored_batches: int = Field(
        default=1000,
        ge=10,
        description="In-memory retention: oldest finished batches are evicted beyond this count.",
    )

    # --- Operations ----------------------------------------------------------------------------
    log_level: str = Field(default="INFO")
    log_format: Literal["json", "text"] = Field(default="json")
    shutdown_grace_seconds: float = Field(
        default=20.0, ge=0, description="How long shutdown waits for running batches."
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
