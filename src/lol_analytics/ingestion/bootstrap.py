"""Wiring helpers for the ingestion CLI.

The CLI commands (`pull-matches`, `pull-timelines`, `pull-leagues`) all
need the same object graph: a `RiotApiClient` with a dead-letter sink,
a `BronzeWriter`, and an ingestion-log sink — all sharing one
`SparkSession`.

This module builds that graph. It is deliberately small and separate
from `cli.py` so the command functions stay readable.

**Where this runs:** inside Databricks. Bronze ingestion writes Delta
tables via Spark, and Spark does not run cleanly on a plain Windows dev
box (see CLAUDE.md). `get_active_spark()` therefore reads the *existing*
session that a Databricks notebook or job task already provides; run
locally with no session, it fails fast with an actionable message
rather than silently producing a broken session.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from lol_analytics.bronze.delta_sinks import DeltaDeadLetterSink, DeltaIngestionLogSink
from lol_analytics.bronze.writer import BronzeWriter
from lol_analytics.ingestion.rate_limiter import RiotRateLimiter
from lol_analytics.ingestion.riot_client import RiotApiClient
from lol_analytics.utils.config import Settings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def get_active_spark() -> SparkSession:
    """Return the SparkSession provided by the Databricks runtime.

    Returns:
        The active `SparkSession`.

    Raises:
        RuntimeError: If there is no active session — i.e. the command
            was run outside Databricks. Bronze ingestion is a Databricks
            workload; the error message says so.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            "No active SparkSession. The ingestion commands write Delta "
            "tables and must run inside Databricks (a notebook or a job "
            "task), not on a local machine. See docs/setup/databricks_workspace.md."
        )
    return spark


def _api_key_hash(api_key: str) -> str:
    """Last 4 chars of the API key — the only part safe to persist."""
    return api_key[-4:]


@dataclass(slots=True)
class IngestionStack:
    """The fully-wired set of collaborators an ingestion run needs."""

    client: RiotApiClient
    writer: BronzeWriter
    log_sink: DeltaIngestionLogSink
    api_key_hash: str


def build_ingestion_stack(settings: Settings, spark: SparkSession) -> IngestionStack:
    """Wire a `RiotApiClient` (+ dead-letter sink), `BronzeWriter`, and
    ingestion-log sink against one SparkSession.

    Args:
        settings: Application settings (provides the API key and rate
            limit windows).
        spark: The active SparkSession from `get_active_spark()`.

    Returns:
        An `IngestionStack` ready to hand to a runner.
    """
    rate_limiter = RiotRateLimiter(
        windows=[
            (settings.riot_rate_limit_per_second, 1.0),
            (settings.riot_rate_limit_per_2min, 120.0),
        ]
    )
    client = RiotApiClient(
        settings.riot_api_key,
        rate_limiter,
        dead_letter_sink=DeltaDeadLetterSink(spark),
    )
    return IngestionStack(
        client=client,
        writer=BronzeWriter(spark),
        log_sink=DeltaIngestionLogSink(spark),
        api_key_hash=_api_key_hash(settings.riot_api_key),
    )
