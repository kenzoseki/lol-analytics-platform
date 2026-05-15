"""Dead-letter queue for Bronze ingestion failures.

When the Riot API client exhausts retries on a request (either a 4xx
non-retryable response, or a transport error after N attempts), we want
to:

1. Record the failure with full context so it can be triaged later.
2. NOT block the rest of the batch — one bad match must not stop the
   ingestion of 99 good ones.

This module defines:

- `DeadLetterRecord`: an immutable dataclass holding everything we need
  to debug a failure without touching logs.
- `DeadLetterSink`: a `Protocol` describing where the records go. In
  production this is a Delta-backed writer that appends to
  `lol_analytics.bronze.ingestion_dead_letter`; in tests it's a list.

The client / runner depends on the `Protocol`, not a concrete writer,
which keeps the ingestion layer testable without Spark.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol


def _new_request_id() -> str:
    return str(uuid.uuid4())


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


@dataclass(frozen=True, slots=True)
class DeadLetterRecord:
    """One row destined for `lol_analytics.bronze.ingestion_dead_letter`.

    Fields mirror the DDL column-for-column. `request_id` and `failed_at`
    default to fresh values so callers only need to fill the rest.

    Attributes:
        endpoint: Logical endpoint name (`get_match`, `get_match_timeline`).
        url: Full URL that was called.
        http_status: Last HTTP status seen. `None` for transport errors.
        error_class: Exception class name (`RiotApiError`, `TransportError`).
        error_message: Truncated error message (caller chooses truncation).
        request_payload: Query params or body, if any.
        attempt_count: How many attempts before giving up.
        request_id: UUID for this request. Generated automatically.
        failed_at: UTC timestamp of the final failure. Generated automatically.
    """

    endpoint: str
    url: str
    error_class: str
    attempt_count: int
    http_status: int | None = None
    error_message: str | None = None
    request_payload: str | None = None
    request_id: str = field(default_factory=_new_request_id)
    failed_at: datetime = field(default_factory=_now_utc)


class DeadLetterSink(Protocol):
    """Where dead-letter records go.

    Production implementations append to a Delta table; tests use an
    in-memory list. The protocol is intentionally minimal — `write`
    accepts one record and returns nothing. Batch flushing, if needed,
    is the implementation's concern.
    """

    def write(self, record: DeadLetterRecord) -> None:
        """Persist `record`. Implementations must not raise on transient
        errors — losing a dead-letter row is acceptable; losing the
        original failure context is not."""
        ...


class InMemoryDeadLetterSink:
    """List-backed sink for tests and local development.

    Use in production only as a fallback — records vanish when the
    process exits.
    """

    def __init__(self) -> None:
        self.records: list[DeadLetterRecord] = []

    def write(self, record: DeadLetterRecord) -> None:
        self.records.append(record)
