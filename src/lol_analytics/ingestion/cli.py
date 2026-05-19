"""Command-line interface for the ingestion layer.

Exposed as `lol-ingest` via the project script entry point in pyproject.toml.

Commands:
- `smoke-test`     — validate API key + rate limiter + routing (no Spark).
- `pull-leagues`   — snapshot apex-tier league entries into Bronze.
- `pull-matches`   — fetch full match payloads into Bronze.
- `pull-timelines` — fetch match timelines into Bronze.

The three `pull-*` commands write Delta tables and therefore require an
active SparkSession — they run inside Databricks, not on a local machine
(see `bootstrap.get_active_spark`).
"""

from __future__ import annotations

import asyncio

import typer

from lol_analytics.ingestion.bootstrap import build_ingestion_stack, get_active_spark
from lol_analytics.ingestion.riot_client import RiotApiClient
from lol_analytics.ingestion.runners import (
    LeagueEntriesIngestionRunner,
    MatchIngestionRunner,
    TimelineIngestionRunner,
)
from lol_analytics.ingestion.smoke_test import run_smoke_test
from lol_analytics.utils.config import get_settings
from lol_analytics.utils.logging import configure_logging, get_logger

app = typer.Typer(
    help="LoL analytics ingestion CLI.",
    no_args_is_help=True,
    add_completion=False,
)


@app.callback()
def _root() -> None:
    """Force multi-command mode so single-command setups don't collapse.

    Typer auto-flattens a Typer app with exactly one command into a
    single-command CLI, which would make `lol-ingest smoke-test` reject
    `smoke-test` as an unexpected argument. Registering a no-op callback
    keeps the subcommand structure stable as more commands land.
    """


@app.command("smoke-test")
def smoke_test() -> None:
    """Validate API key, rate limiter, and routing against the live Riot API.

    Pulls top Challenger players on BR1, fetches one player's recent match
    list, then loads one full match payload. Exits non-zero on failure.
    """
    asyncio.run(run_smoke_test())


@app.command("pull-leagues")
def pull_leagues(
    platform: str = typer.Option(..., help="Platform shard, e.g. BR1 or KR."),
) -> None:
    """Snapshot Challenger/Grandmaster/Master league entries for a platform.

    Writes one row per ranked player into `bronze.raw_league_entries`.
    Requires an active SparkSession (run inside Databricks).
    """
    settings = get_settings()
    configure_logging(settings.log_level)
    log = get_logger("cli.pull_leagues")

    spark = get_active_spark()
    stack = build_ingestion_stack(settings, spark)
    runner = LeagueEntriesIngestionRunner(stack.client, stack.writer, log_sink=stack.log_sink)

    async def _run() -> int:
        async with stack.client:
            return await runner.run(platform=platform)

    inserted = asyncio.run(_run())
    log.info("pull_leagues_done", platform=platform, rows_inserted=inserted)


@app.command("pull-matches")
def pull_matches(
    platform: str = typer.Option(..., help="Platform shard, e.g. BR1 or KR."),
    match_ids: list[str] = typer.Option(
        ..., "--match-id", help="Match ID to ingest. Repeat for multiple."
    ),
) -> None:
    """Fetch full match payloads for the given match IDs into Bronze.

    Writes to `bronze.raw_matches`, idempotent on `(match_id, platform)`.
    Requires an active SparkSession (run inside Databricks).
    """
    settings = get_settings()
    configure_logging(settings.log_level)
    log = get_logger("cli.pull_matches")

    region = RiotApiClient.region_for_platform(platform)
    spark = get_active_spark()
    stack = build_ingestion_stack(settings, spark)
    runner = MatchIngestionRunner(
        stack.client,
        stack.writer,
        log_sink=stack.log_sink,
        api_key_hash=stack.api_key_hash,
    )

    async def _run() -> int:
        async with stack.client:
            return await runner.run(region=region, platform=platform, match_ids=match_ids)

    inserted = asyncio.run(_run())
    log.info(
        "pull_matches_done",
        platform=platform,
        requested=len(match_ids),
        rows_inserted=inserted,
    )


@app.command("pull-timelines")
def pull_timelines(
    platform: str = typer.Option(..., help="Platform shard, e.g. BR1 or KR."),
    match_ids: list[str] = typer.Option(
        ..., "--match-id", help="Match ID whose timeline to ingest. Repeat for multiple."
    ),
) -> None:
    """Fetch match timelines for the given match IDs into Bronze.

    Writes to `bronze.raw_match_timeline`, idempotent on
    `(match_id, platform)`. Requires an active SparkSession (Databricks).
    """
    settings = get_settings()
    configure_logging(settings.log_level)
    log = get_logger("cli.pull_timelines")

    region = RiotApiClient.region_for_platform(platform)
    spark = get_active_spark()
    stack = build_ingestion_stack(settings, spark)
    runner = TimelineIngestionRunner(
        stack.client,
        stack.writer,
        log_sink=stack.log_sink,
        api_key_hash=stack.api_key_hash,
    )

    async def _run() -> int:
        async with stack.client:
            return await runner.run(region=region, platform=platform, match_ids=match_ids)

    inserted = asyncio.run(_run())
    log.info(
        "pull_timelines_done",
        platform=platform,
        requested=len(match_ids),
        rows_inserted=inserted,
    )


if __name__ == "__main__":
    app()
