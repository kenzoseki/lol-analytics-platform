"""End-to-end smoke test for the Riot API client.

Run with:
    uv run python -m lol_analytics.ingestion.smoke_test

Requires a valid RIOT_API_KEY in .env. This script:
  1. Pulls top 5 Challenger players on BR1
  2. For one player, lists their last 5 ranked match IDs
  3. Fetches one full match payload

If all three steps succeed, your API key + rate limiter + routing logic work.
"""

from __future__ import annotations

import asyncio

from lol_analytics.ingestion.rate_limiter import RiotRateLimiter
from lol_analytics.ingestion.riot_client import RiotApiClient
from lol_analytics.utils.config import get_settings
from lol_analytics.utils.logging import configure_logging, get_logger


async def run_smoke_test() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    log = get_logger("smoke_test")

    rate_limiter = RiotRateLimiter(
        windows=[
            (settings.riot_rate_limit_per_second, 1.0),
            (settings.riot_rate_limit_per_2min, 120.0),
        ]
    )

    async with RiotApiClient(settings.riot_api_key, rate_limiter) as client:
        platform = "BR1"
        region = client.region_for_platform(platform)

        log.info("step_1_challenger_league", platform=platform)
        league = await client.get_challenger_league(platform)
        entries = league.get("entries", [])
        log.info("challenger_loaded", count=len(entries))

        if not entries:
            log.error("no_challenger_entries")
            return

        # Top player by leaguePoints
        top = max(entries, key=lambda e: e.get("leaguePoints", 0))
        puuid = top["puuid"]
        log.info(
            "top_player",
            league_points=top["leaguePoints"],
            wins=top["wins"],
            losses=top["losses"],
        )

        log.info("step_2_match_ids", puuid_prefix=puuid[:8])
        match_ids = await client.get_match_ids_by_puuid(region, puuid, count=5)
        log.info("match_ids_loaded", count=len(match_ids))

        if not match_ids:
            log.warning("no_recent_ranked_matches")
            return

        log.info("step_3_match_detail", match_id=match_ids[0])
        match = await client.get_match(region, match_ids[0])
        info = match.get("info", {})
        log.info(
            "match_loaded",
            game_version=info.get("gameVersion"),
            game_duration_s=info.get("gameDuration"),
            participants=len(info.get("participants", [])),
        )

        log.info("smoke_test_passed")


if __name__ == "__main__":
    asyncio.run(run_smoke_test())
