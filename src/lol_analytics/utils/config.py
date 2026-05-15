"""Application configuration loaded from environment variables.

Uses pydantic-settings so that config is type-checked and validated at startup,
not at the moment a missing variable is read deep in some pipeline run.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings.

    All values can be overridden via environment variables or a .env file
    in the project root. See .env.example for the full list.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ---- Riot API ----
    riot_api_key: str = Field(..., description="Riot Games API key (RGAPI-...)")

    # ---- Routing ----
    # Platforms are per-region shards used by summoner-v4, league-v4, etc.
    # Regions are super-regions used by match-v5 (americas/europe/asia/sea).
    lol_platforms: list[str] = Field(default=["BR1", "KR"])
    lol_regions: list[str] = Field(default=["AMERICAS", "ASIA"])

    # ---- Rate limiting (development key defaults) ----
    # Riot enforces TWO concurrent windows: per-second AND per-2min.
    # We track both with a token bucket per window.
    riot_rate_limit_per_second: int = 20
    riot_rate_limit_per_2min: int = 100

    # ---- Storage ----
    bronze_path: str = "/tmp/lol/bronze"
    silver_path: str = "/tmp/lol/silver"
    gold_path: str = "/tmp/lol/gold"

    # ---- Observability ----
    log_level: str = "INFO"


def get_settings() -> Settings:
    """Cached settings accessor.

    We don't use functools.lru_cache because tests need to override env vars
    and re-instantiate. Pipeline jobs only call this once at startup anyway.
    """
    return Settings()  # type: ignore[call-arg]
