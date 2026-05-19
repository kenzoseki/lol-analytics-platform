"""Ingestion runners — orchestrate fetch → build → upsert for Bronze.

A runner is the join point between the two halves of the pipeline:

- The **async** Riot API layer (`RiotApiClient`) — network-bound, runs
  many requests concurrently under the rate limiter.
- The **sync** Spark layer (`BronzeWriter`) — writes the collected
  records to Delta via `MERGE INTO`.

CLAUDE.md says "don't mix async and PySpark". The runner respects that
by phase separation: it `await`s *all* the API calls first, collecting
plain records in memory, and only then hands the full batch to the
(synchronous) writer. There is no `await` interleaved with Spark calls.

Every runner:
- Generates one `run_id` and emits `started` / `completed` / `failed`
  ingestion-log events around its work.
- Records per-table `inserted` / `skipped_duplicate` counts.
- Does NOT retry — `RiotApiClient` already retries transient errors and
  writes terminal failures to the dead-letter queue. A runner surfaces
  the failure in its `failed` event and re-raises.

All collaborators (`client`, `writer`, `log_sink`) are injected, so the
orchestration logic is unit-testable with mocks and needs no Spark.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import structlog

from lol_analytics.bronze.ingestion_log import (
    IngestionEvent,
    IngestionLogSink,
    new_run_id,
)
from lol_analytics.bronze.records import (
    build_league_entry_records,
    build_match_record,
)
from lol_analytics.ingestion.riot_client import RiotApiClient

if TYPE_CHECKING:
    from lol_analytics.bronze.writer import BronzeWriter

log = structlog.get_logger(__name__)

# Apex-tier league-v4 endpoints, paired with the tier label they carry.
_APEX_TIERS = ("CHALLENGER", "GRANDMASTER", "MASTER")


def _now_ms() -> int:
    return int(time.monotonic() * 1000)


class _RunContext:
    """Tracks a single runner invocation: its run_id and elapsed time."""

    def __init__(self, runner_name: str, log_sink: IngestionLogSink | None):
        self.runner_name = runner_name
        self.run_id = new_run_id()
        self.log_sink = log_sink
        self._start_ms = _now_ms()

    def _emit(self, event: IngestionEvent) -> None:
        if self.log_sink is None:
            return
        try:
            self.log_sink.write(event)
        except Exception:
            log.warning("ingestion_log_emit_failed", run_id=self.run_id)

    def started(self, platform: str | None = None) -> None:
        self._emit(
            IngestionEvent(
                run_id=self.run_id,
                runner_name=self.runner_name,
                action="started",
                platform=platform,
            )
        )

    def inserted(self, target_table: str, rows: int, platform: str | None) -> None:
        self._emit(
            IngestionEvent(
                run_id=self.run_id,
                runner_name=self.runner_name,
                action="inserted",
                platform=platform,
                target_table=target_table,
                rows_affected=rows,
            )
        )

    def skipped(self, target_table: str, rows: int, platform: str | None) -> None:
        self._emit(
            IngestionEvent(
                run_id=self.run_id,
                runner_name=self.runner_name,
                action="skipped_duplicate",
                platform=platform,
                target_table=target_table,
                rows_affected=rows,
            )
        )

    def completed(self, platform: str | None = None) -> None:
        self._emit(
            IngestionEvent(
                run_id=self.run_id,
                runner_name=self.runner_name,
                action="completed",
                platform=platform,
                duration_ms=_now_ms() - self._start_ms,
            )
        )

    def failed(self, error: BaseException, platform: str | None = None) -> None:
        self._emit(
            IngestionEvent(
                run_id=self.run_id,
                runner_name=self.runner_name,
                action="failed",
                platform=platform,
                error_class=type(error).__name__,
                error_message=str(error)[:1000],
                duration_ms=_now_ms() - self._start_ms,
            )
        )


class MatchIngestionRunner:
    """Fetches full match payloads and upserts them into `raw_matches`.

    Given a list of match IDs, fetches each one through `match-v5` and
    upserts the batch. One bad match does not stop the batch — the
    failed request is dead-lettered by the client and skipped here.
    """

    runner_name = "match_ingestion"

    def __init__(
        self,
        client: RiotApiClient,
        writer: BronzeWriter,
        log_sink: IngestionLogSink | None = None,
        api_key_hash: str | None = None,
    ):
        self.client = client
        self.writer = writer
        self.log_sink = log_sink
        self.api_key_hash = api_key_hash

    async def run(self, *, region: str, platform: str, match_ids: list[str]) -> int:
        """Ingest the given matches. Returns the number of rows inserted.

        Args:
            region: Routing super-region for `match-v5` (e.g. `americas`).
            platform: Platform shard the matches belong to (`BR1`).
            match_ids: Match IDs to fetch and upsert.
        """
        ctx = _RunContext(self.runner_name, self.log_sink)
        ctx.started(platform=platform)
        try:
            records = []
            for match_id in match_ids:
                try:
                    payload = await self.client.get_match(region, match_id)
                except Exception:
                    log.warning("match_skipped", match_id=match_id)
                    continue
                records.append(
                    build_match_record(
                        match_id=match_id,
                        platform=platform,
                        region=region,
                        payload=payload,
                        source_endpoint=f"/lol/match/v5/matches/{match_id}",
                        api_key_hash=self.api_key_hash,
                    )
                )

            result = self.writer.upsert_matches(records)
            ctx.inserted(self.writer.TABLE_RAW_MATCHES, result.inserted, platform)
            if result.skipped_duplicate:
                ctx.skipped(self.writer.TABLE_RAW_MATCHES, result.skipped_duplicate, platform)
            ctx.completed(platform=platform)
            return result.inserted
        except Exception as e:
            ctx.failed(e, platform=platform)
            raise


class TimelineIngestionRunner:
    """Fetches match timelines and upserts them into `raw_match_timeline`.

    Same shape as `MatchIngestionRunner` but hits the `match-v5/timeline`
    endpoint. Typically run after match ingestion, over the same IDs.
    """

    runner_name = "timeline_ingestion"

    def __init__(
        self,
        client: RiotApiClient,
        writer: BronzeWriter,
        log_sink: IngestionLogSink | None = None,
        api_key_hash: str | None = None,
    ):
        self.client = client
        self.writer = writer
        self.log_sink = log_sink
        self.api_key_hash = api_key_hash

    async def run(self, *, region: str, platform: str, match_ids: list[str]) -> int:
        """Ingest timelines for the given matches. Returns rows inserted."""
        ctx = _RunContext(self.runner_name, self.log_sink)
        ctx.started(platform=platform)
        try:
            records = []
            for match_id in match_ids:
                try:
                    payload = await self.client.get_match_timeline(region, match_id)
                except Exception:
                    log.warning("timeline_skipped", match_id=match_id)
                    continue
                records.append(
                    build_match_record(
                        match_id=match_id,
                        platform=platform,
                        region=region,
                        payload=payload,
                        source_endpoint=f"/lol/match/v5/matches/{match_id}/timeline",
                        api_key_hash=self.api_key_hash,
                    )
                )

            result = self.writer.upsert_timelines(records)
            ctx.inserted(self.writer.TABLE_RAW_MATCH_TIMELINE, result.inserted, platform)
            if result.skipped_duplicate:
                ctx.skipped(
                    self.writer.TABLE_RAW_MATCH_TIMELINE,
                    result.skipped_duplicate,
                    platform,
                )
            ctx.completed(platform=platform)
            return result.inserted
        except Exception as e:
            ctx.failed(e, platform=platform)
            raise


class LeagueEntriesIngestionRunner:
    """Snapshots the apex-tier league tables for a platform.

    Pulls Challenger, Grandmaster and Master for one platform and upserts
    every entry into `raw_league_entries`. Each run is one daily snapshot.
    """

    runner_name = "league_entries_ingestion"

    def __init__(
        self,
        client: RiotApiClient,
        writer: BronzeWriter,
        log_sink: IngestionLogSink | None = None,
    ):
        self.client = client
        self.writer = writer
        self.log_sink = log_sink

    async def run(self, *, platform: str, queue: str = "RANKED_SOLO_5x5") -> int:
        """Snapshot all apex tiers for one platform. Returns rows inserted."""
        ctx = _RunContext(self.runner_name, self.log_sink)
        ctx.started(platform=platform)
        try:
            records = []
            for tier in _APEX_TIERS:
                try:
                    payload = await self._fetch_tier(platform, tier, queue)
                except Exception:
                    log.warning("league_tier_skipped", platform=platform, tier=tier)
                    continue
                endpoint = f"/lol/league/v4/{tier.lower()}leagues/by-queue/{queue}"
                records.extend(
                    build_league_entry_records(
                        league_payload=payload,
                        platform=platform,
                        tier=tier,
                        source_endpoint=endpoint,
                    )
                )

            result = self.writer.upsert_league_entries(records)
            ctx.inserted(self.writer.TABLE_RAW_LEAGUE_ENTRIES, result.inserted, platform)
            if result.skipped_duplicate:
                ctx.skipped(
                    self.writer.TABLE_RAW_LEAGUE_ENTRIES,
                    result.skipped_duplicate,
                    platform,
                )
            ctx.completed(platform=platform)
            return result.inserted
        except Exception as e:
            ctx.failed(e, platform=platform)
            raise

    async def _fetch_tier(self, platform: str, tier: str, queue: str) -> dict[str, Any]:
        """Dispatch to the right apex-tier endpoint for `tier`."""
        if tier == "CHALLENGER":
            return await self.client.get_challenger_league(platform, queue)
        if tier == "GRANDMASTER":
            return await self.client.get_grandmaster_league(platform, queue)
        return await self.client.get_master_league(platform, queue)
