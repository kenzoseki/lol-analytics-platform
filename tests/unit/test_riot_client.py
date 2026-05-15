"""Unit tests for the Riot API client using respx to mock httpx."""

from __future__ import annotations

import httpx
import pytest
import respx

from lol_analytics.ingestion.rate_limiter import RiotRateLimiter
from lol_analytics.ingestion.riot_client import (
    RiotApiClient,
    RiotApiError,
)


@pytest.fixture
def limiter() -> RiotRateLimiter:
    # Generous limits — these tests are about the client, not throttling
    return RiotRateLimiter(windows=[(1000, 1.0)])


class TestRouting:
    def test_americas_for_br1(self) -> None:
        assert RiotApiClient.region_for_platform("BR1") == "americas"

    def test_asia_for_kr(self) -> None:
        assert RiotApiClient.region_for_platform("KR") == "asia"

    def test_europe_for_euw1(self) -> None:
        assert RiotApiClient.region_for_platform("EUW1") == "europe"

    def test_case_insensitive(self) -> None:
        assert RiotApiClient.region_for_platform("br1") == "americas"

    def test_unknown_platform_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown platform"):
            RiotApiClient.region_for_platform("ATLANTIS")


class TestRiotApiClient:
    @pytest.mark.asyncio
    @respx.mock
    async def test_get_challenger_league_succeeds(self, limiter: RiotRateLimiter) -> None:
        mock_payload = {
            "tier": "CHALLENGER",
            "entries": [{"puuid": "abc", "leaguePoints": 1000, "wins": 50, "losses": 30}],
        }
        respx.get(
            "https://br1.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"
        ).mock(return_value=httpx.Response(200, json=mock_payload))

        async with RiotApiClient("RGAPI-test", limiter) as client:
            result = await client.get_challenger_league("BR1")

        assert result == mock_payload
        assert result["entries"][0]["puuid"] == "abc"

    @pytest.mark.asyncio
    @respx.mock
    async def test_404_raises_without_retry(self, limiter: RiotRateLimiter) -> None:
        route = respx.get(
            "https://br1.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/missing"
        ).mock(return_value=httpx.Response(404, json={"status": {"message": "not found"}}))

        async with RiotApiClient("RGAPI-test", limiter) as client:
            with pytest.raises(RiotApiError, match="404"):
                await client.get_summoner_by_puuid("BR1", "missing")

        # 4xx is NOT retried — exactly one call
        assert route.call_count == 1

    @pytest.mark.asyncio
    @respx.mock
    async def test_500_is_retried(self, limiter: RiotRateLimiter) -> None:
        # First two calls fail, third succeeds
        route = respx.get(
            "https://br1.api.riotgames.com/lol/league/v4/masterleagues/by-queue/RANKED_SOLO_5x5"
        ).mock(
            side_effect=[
                httpx.Response(503),
                httpx.Response(503),
                httpx.Response(200, json={"tier": "MASTER", "entries": []}),
            ]
        )

        async with RiotApiClient("RGAPI-test", limiter) as client:
            result = await client.get_master_league("BR1")

        assert result["tier"] == "MASTER"
        assert route.call_count == 3

    @pytest.mark.asyncio
    @respx.mock
    async def test_match_ids_returns_list(self, limiter: RiotRateLimiter) -> None:
        # match-v5 returns a JSON array directly, not an object
        respx.get(
            url__regex=r"https://americas\.api\.riotgames\.com/lol/match/v5/matches/by-puuid/.*"
        ).mock(return_value=httpx.Response(200, json=["BR1_111", "BR1_222"]))

        async with RiotApiClient("RGAPI-test", limiter) as client:
            ids = await client.get_match_ids_by_puuid("americas", "puuid-xyz")

        assert ids == ["BR1_111", "BR1_222"]

    @pytest.mark.asyncio
    @respx.mock
    async def test_api_key_header_is_sent(self, limiter: RiotRateLimiter) -> None:
        route = respx.get(
            "https://br1.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"
        ).mock(return_value=httpx.Response(200, json={"entries": []}))

        async with RiotApiClient("RGAPI-secret-token", limiter) as client:
            await client.get_challenger_league("BR1")

        sent_request = route.calls[0].request
        assert sent_request.headers["X-Riot-Token"] == "RGAPI-secret-token"
