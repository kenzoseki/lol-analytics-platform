"""Tests for `lol_analytics.bronze.dead_letter`."""

from __future__ import annotations

from datetime import UTC, datetime

from lol_analytics.bronze.dead_letter import (
    DeadLetterRecord,
    InMemoryDeadLetterSink,
)


class TestDeadLetterRecord:
    def test_minimal_construction(self) -> None:
        rec = DeadLetterRecord(
            endpoint="get_match",
            url="https://br1.api.riotgames.com/lol/match/v5/matches/BR1_123",
            error_class="RiotApiError",
            attempt_count=5,
        )
        assert rec.endpoint == "get_match"
        assert rec.attempt_count == 5
        assert rec.http_status is None
        assert rec.error_message is None
        assert rec.request_payload is None

    def test_full_construction(self) -> None:
        when = datetime(2026, 5, 14, 12, 30, tzinfo=UTC)
        rec = DeadLetterRecord(
            endpoint="get_match",
            url="https://br1.api.riotgames.com/match/v5/matches/X",
            error_class="RiotApiError",
            attempt_count=5,
            http_status=404,
            error_message="not found",
            request_payload='{"foo": "bar"}',
            request_id="fixed-id",
            failed_at=when,
        )
        assert rec.http_status == 404
        assert rec.error_message == "not found"
        assert rec.request_id == "fixed-id"
        assert rec.failed_at == when

    def test_auto_request_id_is_unique(self) -> None:
        r1 = DeadLetterRecord(endpoint="x", url="x", error_class="x", attempt_count=1)
        r2 = DeadLetterRecord(endpoint="x", url="x", error_class="x", attempt_count=1)
        assert r1.request_id != r2.request_id

    def test_auto_failed_at_is_utc(self) -> None:
        rec = DeadLetterRecord(endpoint="x", url="x", error_class="x", attempt_count=1)
        # The default is now(tz=utc); tzinfo must be set so the row
        # serializes correctly to a TIMESTAMP column.
        assert rec.failed_at.tzinfo is not None
        assert rec.failed_at.tzinfo.utcoffset(rec.failed_at).total_seconds() == 0

    def test_record_is_frozen(self) -> None:
        rec = DeadLetterRecord(endpoint="x", url="x", error_class="x", attempt_count=1)
        import dataclasses

        try:
            rec.endpoint = "mutated"  # type: ignore[misc]
        except dataclasses.FrozenInstanceError:
            return
        raise AssertionError("DeadLetterRecord should be frozen")


class TestInMemoryDeadLetterSink:
    def test_starts_empty(self) -> None:
        sink = InMemoryDeadLetterSink()
        assert sink.records == []

    def test_write_appends(self) -> None:
        sink = InMemoryDeadLetterSink()
        rec = DeadLetterRecord(
            endpoint="get_match",
            url="https://x",
            error_class="RiotApiError",
            attempt_count=3,
        )
        sink.write(rec)
        assert sink.records == [rec]

    def test_write_preserves_order(self) -> None:
        sink = InMemoryDeadLetterSink()
        records = [
            DeadLetterRecord(endpoint=f"e{i}", url="x", error_class="x", attempt_count=1)
            for i in range(3)
        ]
        for r in records:
            sink.write(r)
        assert [r.endpoint for r in sink.records] == ["e0", "e1", "e2"]
