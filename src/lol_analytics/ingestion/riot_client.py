"""Riot Games API client.

Wraps the subset of endpoints we need for Phase 1:
  - league-v4: top-tier player IDs (Master / Grandmaster / Challenger)
  - summoner-v4: PUUID lookup
  - match-v5: match list and full match data + timeline

Two routing layers (a Riot quirk worth understanding):
  - Platform routing (BR1, KR, NA1, ...) for player-centric endpoints
  - Regional routing (americas, europe, asia, sea) for match endpoints

We map platform → region internally so callers don't have to.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, cast

import httpx
import structlog
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from lol_analytics.ingestion.rate_limiter import RiotRateLimiter

log = structlog.get_logger(__name__)

# Max attempts for the retry loop. Exposed as a module constant so the
# dead-letter record can report it without re-deriving the tenacity config.
MAX_ATTEMPTS = 5

# Map of platform shard → regional super-region for match-v5 endpoints
PLATFORM_TO_REGION: dict[str, str] = {
    "BR1": "americas",
    "LA1": "americas",
    "LA2": "americas",
    "NA1": "americas",
    "EUW1": "europe",
    "EUN1": "europe",
    "TR1": "europe",
    "RU": "europe",
    "KR": "asia",
    "JP1": "asia",
    "OC1": "sea",
    "PH2": "sea",
    "SG2": "sea",
    "TH2": "sea",
    "TW2": "sea",
    "VN2": "sea",
}


class RiotApiError(Exception):
    """Base class for Riot API errors that should NOT be retried."""


class RiotRateLimitError(Exception):
    """429 from Riot. Should be retried after Retry-After seconds."""

    def __init__(self, retry_after: float):
        super().__init__(f"Rate limited; retry after {retry_after}s")
        self.retry_after = retry_after


def _status_from_message(error: RiotApiError) -> int | None:
    """Best-effort extraction of the HTTP status from a RiotApiError.

    `RiotApiError` messages are formatted `"{status} on {url}: {body}"`.
    Returns the leading integer if present, else `None`.
    """
    head = str(error).split(" ", 1)[0]
    return int(head) if head.isdigit() else None


class RiotApiClient:
    """Async Riot API client.

    One client instance per process is fine; httpx handles connection pooling.
    Use as an async context manager so the underlying httpx client is closed.
    """

    def __init__(
        self,
        api_key: str,
        rate_limiter: RiotRateLimiter,
        timeout: float = 10.0,
    ):
        """Construct the client.

        Args:
            api_key: Riot API key (`RGAPI-...`).
            rate_limiter: Shared multi-window rate limiter.
            timeout: Per-request timeout in seconds.
        """
        self.api_key = api_key
        self.rate_limiter = rate_limiter
        # Terminal failures (4xx other than 429, or transient errors that
        # exhaust all retries) are appended here as plain dicts. The
        # ingestion notebook reads this list after a run and writes the
        # rows to `bronze.ingestion_dead_letter`. The exception still
        # propagates — recording it here does not swallow it.
        self.dead_letters: list[dict[str, Any]] = []
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers={"X-Riot-Token": api_key},
        )

    async def __aenter__(self) -> RiotApiClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self._client.aclose()

    # ---------- Routing helpers ----------

    @staticmethod
    def _platform_url(platform: str, path: str) -> str:
        return f"https://{platform.lower()}.api.riotgames.com{path}"

    @staticmethod
    def _region_url(region: str, path: str) -> str:
        return f"https://{region.lower()}.api.riotgames.com{path}"

    @classmethod
    def region_for_platform(cls, platform: str) -> str:
        try:
            return PLATFORM_TO_REGION[platform.upper()]
        except KeyError as e:
            raise ValueError(f"Unknown platform: {platform}") from e

    # ---------- Core request method ----------

    def _record_dead_letter(
        self,
        endpoint: str,
        url: str,
        error: BaseException,
        http_status: int | None,
    ) -> None:
        """Append a dead-letter record for a terminally failed request.

        The record is a plain dict whose keys match the
        `bronze.ingestion_dead_letter` table columns, so the ingestion
        notebook can write the accumulated list straight to Delta.
        Recording here never raises and never swallows — the original
        exception still propagates to the caller.
        """
        self.dead_letters.append(
            {
                "request_id": str(uuid.uuid4()),
                "endpoint": endpoint,
                "url": url,
                "http_status": http_status,
                "error_class": type(error).__name__,
                "error_message": str(error)[:1000],
                "request_payload": None,
                "attempt_count": MAX_ATTEMPTS,
                "failed_at": datetime.now(tz=UTC),
            }
        )

    async def _get(self, url: str, *, endpoint: str) -> Any:
        """Issue a rate-limited GET with retries on transient errors.

        Retries:
          - httpx.TransportError (connection issues, DNS, etc.)
          - 429 (rate limit) — respects Retry-After
          - 5xx — exponential backoff
        Does NOT retry on 4xx (other than 429): bad request, not found, forbidden.

        On terminal failure (4xx other than 429, or transient errors that
        exhaust all retries) a dead-letter dict is appended to
        `self.dead_letters`, then the exception propagates unchanged.

        Args:
            url: Full request URL.
            endpoint: Logical endpoint name (e.g. `get_match`) for the
                dead-letter record. Keyword-only so callers can't confuse
                it with the URL.
        """
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(MAX_ATTEMPTS),
                wait=wait_exponential(multiplier=1, min=1, max=30),
                retry=retry_if_exception_type(
                    (httpx.TransportError, httpx.HTTPStatusError, RiotRateLimitError)
                ),
                reraise=True,
            ):
                with attempt:
                    await self.rate_limiter.acquire()
                    response = await self._client.get(url)

                    if response.status_code == 429:
                        retry_after = float(response.headers.get("Retry-After", "1"))
                        log.warning("rate_limited", url=url, retry_after=retry_after)
                        raise RiotRateLimitError(retry_after)

                    if response.status_code >= 500:
                        log.warning(
                            "server_error",
                            url=url,
                            status=response.status_code,
                        )
                        response.raise_for_status()  # triggers retry via tenacity

                    if response.status_code >= 400:
                        # 4xx other than 429 — do NOT retry, this is a client bug
                        log.error(
                            "client_error",
                            url=url,
                            status=response.status_code,
                            body=response.text[:500],
                        )
                        raise RiotApiError(
                            f"{response.status_code} on {url}: {response.text[:200]}"
                        )

                    return response.json()
        except RiotApiError as e:
            # Non-retryable 4xx — extract the status from the message prefix.
            self._record_dead_letter(endpoint, url, e, _status_from_message(e))
            raise
        except RetryError as e:
            # All retries exhausted; tenacity wraps the last exception.
            self._record_dead_letter(endpoint, url, e.last_attempt.exception() or e, None)
            raise
        except (httpx.HTTPStatusError, httpx.TransportError, RiotRateLimitError) as e:
            # reraise=True surfaces the last exception directly (not a
            # RetryError) when retries are exhausted.
            status = e.response.status_code if isinstance(e, httpx.HTTPStatusError) else None
            self._record_dead_letter(endpoint, url, e, status)
            raise

        # Unreachable but satisfies type checker
        raise RuntimeError("Retry loop exited without returning")

    # ---------- Endpoints ----------

    async def get_challenger_league(
        self, platform: str, queue: str = "RANKED_SOLO_5x5"
    ) -> dict[str, Any]:
        """Top ~300 players in Challenger tier on a platform."""
        url = self._platform_url(platform, f"/lol/league/v4/challengerleagues/by-queue/{queue}")
        return cast("dict[str, Any]", await self._get(url, endpoint="get_challenger_league"))

    async def get_grandmaster_league(
        self, platform: str, queue: str = "RANKED_SOLO_5x5"
    ) -> dict[str, Any]:
        """Top ~700 players in Grandmaster tier on a platform."""
        url = self._platform_url(platform, f"/lol/league/v4/grandmasterleagues/by-queue/{queue}")
        return cast("dict[str, Any]", await self._get(url, endpoint="get_grandmaster_league"))

    async def get_master_league(
        self, platform: str, queue: str = "RANKED_SOLO_5x5"
    ) -> dict[str, Any]:
        """All players in Master tier on a platform (variable size, often thousands)."""
        url = self._platform_url(platform, f"/lol/league/v4/masterleagues/by-queue/{queue}")
        return cast("dict[str, Any]", await self._get(url, endpoint="get_master_league"))

    async def get_summoner_by_puuid(self, platform: str, puuid: str) -> dict[str, Any]:
        url = self._platform_url(platform, f"/lol/summoner/v4/summoners/by-puuid/{puuid}")
        return cast("dict[str, Any]", await self._get(url, endpoint="get_summoner_by_puuid"))

    async def get_match_ids_by_puuid(
        self,
        region: str,
        puuid: str,
        *,
        queue: int | None = 420,  # 420 = ranked solo
        count: int = 20,
        start: int = 0,
    ) -> list[str]:
        """Recent match IDs for a player. Default: last 20 ranked solo matches."""
        url = self._region_url(region, f"/lol/match/v5/matches/by-puuid/{puuid}/ids")
        params = []
        if queue is not None:
            params.append(f"queue={queue}")
        params.append(f"count={count}")
        params.append(f"start={start}")
        full_url = f"{url}?{'&'.join(params)}"
        result = await self._get(full_url, endpoint="get_match_ids_by_puuid")
        # API returns a JSON array directly, not an object
        return result if isinstance(result, list) else []

    async def get_match(self, region: str, match_id: str) -> dict[str, Any]:
        url = self._region_url(region, f"/lol/match/v5/matches/{match_id}")
        return cast("dict[str, Any]", await self._get(url, endpoint="get_match"))

    async def get_match_timeline(self, region: str, match_id: str) -> dict[str, Any]:
        url = self._region_url(region, f"/lol/match/v5/matches/{match_id}/timeline")
        return cast("dict[str, Any]", await self._get(url, endpoint="get_match_timeline"))
