"""Application configuration loaded from environment variables.

Uses pydantic-settings so that config is type-checked and validated at startup,
not at the moment a missing variable is read deep in some pipeline run.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


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
    lol_platforms: Annotated[list[str], NoDecode] = Field(default=["BR1", "KR"])
    lol_regions: Annotated[list[str], NoDecode] = Field(default=["AMERICAS", "ASIA"])

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

    @field_validator("lol_platforms", "lol_regions", mode="before")
    @classmethod
    def _split_csv(cls, value: object) -> object:
        """Accept comma-separated env values (e.g. `LOL_PLATFORMS=BR1,KR`).

        Pydantic-settings tries to decode list fields as JSON, which
        breaks the conventional CSV-in-dotenv style. We intercept
        strings and split them; lists pass through untouched so
        Python/JSON callers still work. Strips inline `#` comments
        for tolerance to `.env` files that document values inline.
        """
        if isinstance(value, str):
            if "#" in value:
                value = value.split("#", 1)[0]
            return [item.strip() for item in value.split(",") if item.strip()]
        return value


def get_settings() -> Settings:
    """Cached settings accessor.

    We don't use functools.lru_cache because tests need to override env vars
    and re-instantiate. Pipeline jobs only call this once at startup anyway.
    """
    return Settings()  # type: ignore[call-arg]
