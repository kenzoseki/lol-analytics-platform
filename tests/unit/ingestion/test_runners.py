"""Tests for the ingestion runners — orchestration only, no Spark, no HTTP.

The runners are tested with hand-rolled fakes:
- `FakeClient` — stands in for `RiotApiClient`; async methods return
  canned payloads or raise on demand.
- `FakeWriter` — stands in for `BronzeWriter`; records the records it
  was given and returns a configurable `MergeResult`.

This isolates the orchestration logic (fetch loop, error skipping,
ingestion-log events) from the network and from Spark.
"""

from __future__ import annotations

from typing import Any

from lol_analytics.bronze.ingestion_log import InMemoryIngestionLogSink
from lol_analytics.bronze.records import BronzeLeagueEntryRecord, BronzeMatchRecord
from lol_analytics.bronze.writer import (
    TABLE_RAW_LEAGUE_ENTRIES,
    TABLE_RAW_MATCH_TIMELINE,
    TABLE_RAW_MATCHES,
    MergeResult,
)
from lol_analytics.ingestion.runners import (
    LeagueEntriesIngestionRunner,
    MatchIngestionRunner,
    TimelineIngestionRunner,
)


class FakeClient:
    """Async stand-in for RiotApiClient. Canned responses, optional failures."""

    def __init__(
        self,
        *,
        match_fails: set[str] | None = None,
        tier_fails: set[str] | None = None,
    ) -> None:
        self.match_fails = match_fails or set()
        self.tier_fails = tier_fails or set()
        self.match_calls: list[str] = []
        self.timeline_calls: list[str] = []

    async def get_match(self, region: str, match_id: str) -> dict[str, Any]:
        self.match_calls.append(match_id)
        if match_id in self.match_fails:
            raise RuntimeError(f"simulated failure on {match_id}")
        return {"metadata": {"matchId": match_id}, "info": {"gameDuration": 1800}}

    async def get_match_timeline(self, region: str, match_id: str) -> dict[str, Any]:
        self.timeline_calls.append(match_id)
        if match_id in self.match_fails:
            raise RuntimeError(f"simulated timeline failure on {match_id}")
        return {"metadata": {"matchId": match_id}, "info": {"frames": []}}

    async def _league(self, tier: str) -> dict[str, Any]:
        if tier in self.tier_fails:
            raise RuntimeError(f"simulated failure on {tier}")
        return {
            "tier": tier,
            "queue": "RANKED_SOLO_5x5",
            "entries": [
                {"puuid": f"{tier}-p1", "leaguePoints": 100, "wins": 10, "losses": 5},
                {"puuid": f"{tier}-p2", "leaguePoints": 200, "wins": 20, "losses": 8},
            ],
        }

    async def get_challenger_league(
        self, platform: str, queue: str = "RANKED_SOLO_5x5"
    ) -> dict[str, Any]:
        return await self._league("CHALLENGER")

    async def get_grandmaster_league(
        self, platform: str, queue: str = "RANKED_SOLO_5x5"
    ) -> dict[str, Any]:
        return await self._league("GRANDMASTER")

    async def get_master_league(
        self, platform: str, queue: str = "RANKED_SOLO_5x5"
    ) -> dict[str, Any]:
        return await self._league("MASTER")


class FakeWriter:
    """Sync stand-in for BronzeWriter. Captures records, returns canned result."""

    TABLE_RAW_MATCHES = TABLE_RAW_MATCHES
    TABLE_RAW_MATCH_TIMELINE = TABLE_RAW_MATCH_TIMELINE
    TABLE_RAW_LEAGUE_ENTRIES = TABLE_RAW_LEAGUE_ENTRIES

    def __init__(self, *, inserted: int | None = None, skipped: int = 0) -> None:
        # If `inserted` is None, default to "all rows inserted".
        self._inserted = inserted
        self._skipped = skipped
        self.matches: list[BronzeMatchRecord] = []
        self.timelines: list[BronzeMatchRecord] = []
        self.league_entries: list[BronzeLeagueEntryRecord] = []

    def _result(self, n: int) -> MergeResult:
        inserted = n if self._inserted is None else self._inserted
        return MergeResult(inserted=inserted, skipped_duplicate=self._skipped)

    def upsert_matches(self, records: list[BronzeMatchRecord]) -> MergeResult:
        self.matches = records
        return self._result(len(records))

    def upsert_timelines(self, records: list[BronzeMatchRecord]) -> MergeResult:
        self.timelines = records
        return self._result(len(records))

    def upsert_league_entries(self, records: list[BronzeLeagueEntryRecord]) -> MergeResult:
        self.league_entries = records
        return self._result(len(records))


class TestMatchIngestionRunner:
    async def test_happy_path_builds_and_upserts_all(self) -> None:
        client = FakeClient()
        writer = FakeWriter()
        runner = MatchIngestionRunner(client, writer)  # type: ignore[arg-type]

        inserted = await runner.run(
            region="americas",
            platform="BR1",
            match_ids=["BR1_1", "BR1_2", "BR1_3"],
        )

        assert inserted == 3
        assert len(writer.matches) == 3
        assert {r.match_id for r in writer.matches} == {"BR1_1", "BR1_2", "BR1_3"}
        # Every record carries the platform and a computed hash.
        assert all(r.platform == "BR1" and r.payload_hash for r in writer.matches)

    async def test_failed_match_is_skipped_not_fatal(self) -> None:
        # BR1_2 fails — the other two must still be ingested.
        client = FakeClient(match_fails={"BR1_2"})
        writer = FakeWriter()
        runner = MatchIngestionRunner(client, writer)  # type: ignore[arg-type]

        inserted = await runner.run(
            region="americas",
            platform="BR1",
            match_ids=["BR1_1", "BR1_2", "BR1_3"],
        )

        assert inserted == 2
        assert {r.match_id for r in writer.matches} == {"BR1_1", "BR1_3"}

    async def test_empty_match_list(self) -> None:
        client = FakeClient()
        writer = FakeWriter()
        runner = MatchIngestionRunner(client, writer)  # type: ignore[arg-type]

        inserted = await runner.run(region="americas", platform="BR1", match_ids=[])
        assert inserted == 0
        assert writer.matches == []

    async def test_emits_started_and_completed_events(self) -> None:
        client = FakeClient()
        writer = FakeWriter()
        log_sink = InMemoryIngestionLogSink()
        runner = MatchIngestionRunner(client, writer, log_sink=log_sink)  # type: ignore[arg-type]

        await runner.run(region="americas", platform="BR1", match_ids=["BR1_1"])

        actions = [e.action for e in log_sink.events]
        assert actions[0] == "started"
        assert "inserted" in actions
        assert actions[-1] == "completed"
        # All events share one run_id.
        assert len({e.run_id for e in log_sink.events}) == 1

    async def test_emits_skipped_duplicate_event(self) -> None:
        client = FakeClient()
        writer = FakeWriter(inserted=1, skipped=2)
        log_sink = InMemoryIngestionLogSink()
        runner = MatchIngestionRunner(client, writer, log_sink=log_sink)  # type: ignore[arg-type]

        await runner.run(region="americas", platform="BR1", match_ids=["BR1_1", "BR1_2", "BR1_3"])

        skipped = [e for e in log_sink.events if e.action == "skipped_duplicate"]
        assert len(skipped) == 1
        assert skipped[0].rows_affected == 2

    async def test_completed_event_has_duration(self) -> None:
        client = FakeClient()
        writer = FakeWriter()
        log_sink = InMemoryIngestionLogSink()
        runner = MatchIngestionRunner(client, writer, log_sink=log_sink)  # type: ignore[arg-type]

        await runner.run(region="americas", platform="BR1", match_ids=["BR1_1"])
        completed = next(e for e in log_sink.events if e.action == "completed")
        assert completed.duration_ms is not None
        assert completed.duration_ms >= 0

    async def test_api_key_hash_propagates_to_records(self) -> None:
        client = FakeClient()
        writer = FakeWriter()
        runner = MatchIngestionRunner(
            client,
            writer,
            api_key_hash="ab12",  # type: ignore[arg-type]
        )
        await runner.run(region="americas", platform="BR1", match_ids=["BR1_1"])
        assert writer.matches[0].api_key_hash == "ab12"

    async def test_writer_failure_emits_failed_event_and_reraises(self) -> None:
        class BrokenWriter(FakeWriter):
            def upsert_matches(self, records: list[BronzeMatchRecord]) -> MergeResult:
                raise RuntimeError("delta is down")

        client = FakeClient()
        writer = BrokenWriter()
        log_sink = InMemoryIngestionLogSink()
        runner = MatchIngestionRunner(client, writer, log_sink=log_sink)  # type: ignore[arg-type]

        import pytest

        with pytest.raises(RuntimeError, match="delta is down"):
            await runner.run(region="americas", platform="BR1", match_ids=["BR1_1"])

        assert log_sink.events[-1].action == "failed"
        assert log_sink.events[-1].error_class == "RuntimeError"


class TestTimelineIngestionRunner:
    async def test_happy_path(self) -> None:
        client = FakeClient()
        writer = FakeWriter()
        runner = TimelineIngestionRunner(client, writer)  # type: ignore[arg-type]

        inserted = await runner.run(region="americas", platform="BR1", match_ids=["BR1_1", "BR1_2"])
        assert inserted == 2
        assert len(writer.timelines) == 2
        # Source endpoint must point at the /timeline path.
        assert all("/timeline" in r.source_endpoint for r in writer.timelines)

    async def test_failed_timeline_is_skipped(self) -> None:
        client = FakeClient(match_fails={"BR1_1"})
        writer = FakeWriter()
        runner = TimelineIngestionRunner(client, writer)  # type: ignore[arg-type]

        inserted = await runner.run(region="americas", platform="BR1", match_ids=["BR1_1", "BR1_2"])
        assert inserted == 1
        assert writer.timelines[0].match_id == "BR1_2"


class TestLeagueEntriesIngestionRunner:
    async def test_pulls_all_three_apex_tiers(self) -> None:
        client = FakeClient()
        writer = FakeWriter()
        runner = LeagueEntriesIngestionRunner(client, writer)  # type: ignore[arg-type]

        inserted = await runner.run(platform="BR1")

        # 3 tiers x 2 entries each = 6 rows.
        assert inserted == 6
        tiers = {r.tier for r in writer.league_entries}
        assert tiers == {"CHALLENGER", "GRANDMASTER", "MASTER"}

    async def test_one_failed_tier_does_not_abort_others(self) -> None:
        client = FakeClient(tier_fails={"GRANDMASTER"})
        writer = FakeWriter()
        runner = LeagueEntriesIngestionRunner(client, writer)  # type: ignore[arg-type]

        inserted = await runner.run(platform="BR1")

        # GRANDMASTER skipped → only CHALLENGER + MASTER = 4 rows.
        assert inserted == 4
        tiers = {r.tier for r in writer.league_entries}
        assert tiers == {"CHALLENGER", "MASTER"}

    async def test_emits_lifecycle_events(self) -> None:
        client = FakeClient()
        writer = FakeWriter()
        log_sink = InMemoryIngestionLogSink()
        runner = LeagueEntriesIngestionRunner(client, writer, log_sink=log_sink)  # type: ignore[arg-type]

        await runner.run(platform="BR1")

        actions = [e.action for e in log_sink.events]
        assert actions[0] == "started"
        assert actions[-1] == "completed"
        assert all(e.runner_name == "league_entries_ingestion" for e in log_sink.events)

    async def test_records_carry_platform(self) -> None:
        client = FakeClient()
        writer = FakeWriter()
        runner = LeagueEntriesIngestionRunner(client, writer)  # type: ignore[arg-type]

        await runner.run(platform="KR")
        assert all(r.platform == "KR" for r in writer.league_entries)
