"""Structured ingestion log for Bronze runners.

Every runner invocation emits a sequence of events that are persisted
to `lol_analytics.bronze.ingestion_log`. The events let an operator
answer questions in SQL like:

- "How many runs ran today, and how many failed?"
- "Which platform has the slowest match ingestion?"
- "How often does MERGE skip a duplicate?"

This module mirrors the DLQ design: a frozen dataclass plus a
`Protocol` for the sink. The runner takes the sink via DI, so tests
inject an in-memory list and production injects a Delta-backed writer.

Action vocabulary (closed set; do not invent new ones without
extending the DDL `CHECK` semantics by convention):

- `started` — runner began. Always the first event of a run.
- `inserted` — N rows newly inserted into a Bronze table.
- `skipped_duplicate` — N rows skipped because they already existed
  (MERGE INTO matched).
- `failed` — runner aborted with an exception. Terminal.
- `completed` — runner finished normally. Terminal.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal, Protocol

IngestionAction = Literal[
    "started",
    "inserted",
    "skipped_duplicate",
    "failed",
    "completed",
]


def _new_event_id() -> str:
    return str(uuid.uuid4())


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


@dataclass(frozen=True, slots=True)
class IngestionEvent:
    """One row destined for `lol_analytics.bronze.ingestion_log`.

    Fields mirror the DDL column-for-column. `event_id` and `emitted_at`
    default to fresh values so callers only need to fill the rest.

    Attributes:
        run_id: Shared across all events emitted by a single runner
            invocation. The caller generates this once at runner start
            and passes it to every subsequent event.
        runner_name: Identifier of the runner (e.g. `match_ingestion`,
            `timeline_ingestion`, `league_entries_ingestion`).
        action: One of the values in `IngestionAction`.
        platform: Platform shard, if the event is platform-scoped.
        target_table: Fully-qualified Bronze table written to, if any.
        rows_affected: Rows inserted/updated/skipped by this event.
        error_class: Exception class on `failed` events.
        error_message: Truncated error message on `failed` events.
        duration_ms: Wall-clock duration for terminal events
            (`completed`/`failed`).
        event_id: UUID for this event. Generated automatically.
        emitted_at: UTC timestamp the event was recorded. Generated
            automatically.
    """

    run_id: str
    runner_name: str
    action: IngestionAction
    platform: str | None = None
    target_table: str | None = None
    rows_affected: int | None = None
    error_class: str | None = None
    error_message: str | None = None
    duration_ms: int | None = None
    event_id: str = field(default_factory=_new_event_id)
    emitted_at: datetime = field(default_factory=_now_utc)


class IngestionLogSink(Protocol):
    """Where ingestion events go.

    Production implementations append to a Delta table; tests use an
    in-memory list. Must not raise on transient errors — losing an
    event row is acceptable; losing the original ingestion state is not.
    """

    def write(self, event: IngestionEvent) -> None:
        """Persist `event`."""
        ...


class InMemoryIngestionLogSink:
    """List-backed sink for tests and local development."""

    def __init__(self) -> None:
        self.events: list[IngestionEvent] = []

    def write(self, event: IngestionEvent) -> None:
        self.events.append(event)


def new_run_id() -> str:
    """Generate a fresh `run_id` for a runner invocation.

    Wrapped in a function (rather than calling `uuid.uuid4()` directly
    at the call site) so tests can monkey-patch a deterministic source.
    """
    return str(uuid.uuid4())
