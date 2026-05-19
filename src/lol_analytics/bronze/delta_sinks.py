"""Delta-backed implementations of the dead-letter and ingestion-log sinks.

`bronze.dead_letter` and `bronze.ingestion_log` define `Protocol`s plus
in-memory implementations for testing. This module provides the
production implementations: each `write()` appends one row to a Delta
table in Unity Catalog.

Both sinks are **append-only** — they record events, never upsert. They
also follow the "never raise" contract of their protocols: a failure to
persist an observability row must not crash the ingestion run it is
observing. The caller (RiotApiClient / runners) already wraps sink calls
defensively, but these implementations log-and-swallow too, as a second
line of defence.

The `SparkSession` is injected, never created here (same rationale as
`BronzeWriter`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from lol_analytics.bronze.dead_letter import DeadLetterRecord
from lol_analytics.bronze.ingestion_log import IngestionEvent

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = structlog.get_logger(__name__)

CATALOG = "lol_analytics"
SCHEMA = "bronze"
TABLE_DEAD_LETTER = f"{CATALOG}.{SCHEMA}.ingestion_dead_letter"
TABLE_INGESTION_LOG = f"{CATALOG}.{SCHEMA}.ingestion_log"

# Column order excludes the Delta generated columns
# (`failed_at_date`, `emitted_at_date`).
_DEAD_LETTER_COLUMNS = [
    "request_id",
    "endpoint",
    "url",
    "http_status",
    "error_class",
    "error_message",
    "request_payload",
    "attempt_count",
    "failed_at",
]

_INGESTION_LOG_COLUMNS = [
    "event_id",
    "run_id",
    "runner_name",
    "action",
    "platform",
    "target_table",
    "rows_affected",
    "error_class",
    "error_message",
    "duration_ms",
    "emitted_at",
]


class DeltaDeadLetterSink:
    """Appends `DeadLetterRecord`s to `bronze.ingestion_dead_letter`.

    Satisfies the `DeadLetterSink` protocol from `bronze.dead_letter`.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write(self, record: DeadLetterRecord) -> None:
        """Append one dead-letter row. Logs and swallows on failure."""
        try:
            df = self.spark.createDataFrame(
                [
                    (
                        record.request_id,
                        record.endpoint,
                        record.url,
                        record.http_status,
                        record.error_class,
                        record.error_message,
                        record.request_payload,
                        record.attempt_count,
                        record.failed_at,
                    )
                ],
                schema=_DEAD_LETTER_COLUMNS,
            )
            df.write.format("delta").mode("append").saveAsTable(TABLE_DEAD_LETTER)
        except Exception:
            log.warning(
                "dead_letter_delta_write_failed",
                request_id=record.request_id,
                endpoint=record.endpoint,
            )


class DeltaIngestionLogSink:
    """Appends `IngestionEvent`s to `bronze.ingestion_log`.

    Satisfies the `IngestionLogSink` protocol from `bronze.ingestion_log`.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write(self, event: IngestionEvent) -> None:
        """Append one ingestion-log row. Logs and swallows on failure."""
        try:
            df = self.spark.createDataFrame(
                [
                    (
                        event.event_id,
                        event.run_id,
                        event.runner_name,
                        event.action,
                        event.platform,
                        event.target_table,
                        event.rows_affected,
                        event.error_class,
                        event.error_message,
                        event.duration_ms,
                        event.emitted_at,
                    )
                ],
                schema=_INGESTION_LOG_COLUMNS,
            )
            df.write.format("delta").mode("append").saveAsTable(TABLE_INGESTION_LOG)
        except Exception:
            log.warning(
                "ingestion_log_delta_write_failed",
                event_id=event.event_id,
                runner_name=event.runner_name,
            )
