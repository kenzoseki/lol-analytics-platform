"""Tests for `lol_analytics.bronze.ingestion_log`."""

from __future__ import annotations

from datetime import UTC, datetime

from lol_analytics.bronze.ingestion_log import (
    IngestionEvent,
    InMemoryIngestionLogSink,
    new_run_id,
)


class TestIngestionEvent:
    def test_minimal_construction(self) -> None:
        ev = IngestionEvent(
            run_id="run-1",
            runner_name="match_ingestion",
            action="started",
        )
        assert ev.action == "started"
        assert ev.platform is None
        assert ev.rows_affected is None
        assert ev.duration_ms is None

    def test_inserted_event(self) -> None:
        ev = IngestionEvent(
            run_id="run-1",
            runner_name="match_ingestion",
            action="inserted",
            platform="BR1",
            target_table="lol_analytics.bronze.raw_matches",
            rows_affected=42,
        )
        assert ev.rows_affected == 42
        assert ev.target_table == "lol_analytics.bronze.raw_matches"

    def test_failed_event_carries_error(self) -> None:
        ev = IngestionEvent(
            run_id="run-1",
            runner_name="match_ingestion",
            action="failed",
            error_class="RiotApiError",
            error_message="404 on /matches/BR1_123",
            duration_ms=12_345,
        )
        assert ev.error_class == "RiotApiError"
        assert ev.duration_ms == 12_345

    def test_auto_event_id_is_unique(self) -> None:
        a = IngestionEvent(run_id="r", runner_name="x", action="started")
        b = IngestionEvent(run_id="r", runner_name="x", action="started")
        assert a.event_id != b.event_id

    def test_auto_emitted_at_is_utc(self) -> None:
        ev = IngestionEvent(run_id="r", runner_name="x", action="started")
        assert ev.emitted_at.tzinfo is not None
        assert ev.emitted_at.tzinfo.utcoffset(ev.emitted_at).total_seconds() == 0

    def test_emitted_at_can_be_overridden(self) -> None:
        when = datetime(2026, 1, 1, tzinfo=UTC)
        ev = IngestionEvent(run_id="r", runner_name="x", action="started", emitted_at=when)
        assert ev.emitted_at == when

    def test_event_is_frozen(self) -> None:
        import dataclasses

        ev = IngestionEvent(run_id="r", runner_name="x", action="started")
        try:
            ev.action = "completed"  # type: ignore[misc]
        except dataclasses.FrozenInstanceError:
            return
        raise AssertionError("IngestionEvent should be frozen")


class TestInMemoryIngestionLogSink:
    def test_starts_empty(self) -> None:
        sink = InMemoryIngestionLogSink()
        assert sink.events == []

    def test_write_appends(self) -> None:
        sink = InMemoryIngestionLogSink()
        ev = IngestionEvent(run_id="r", runner_name="x", action="started")
        sink.write(ev)
        assert sink.events == [ev]

    def test_filter_by_action_via_python(self) -> None:
        # No special API; consumers just filter the list. This test
        # documents the expected usage pattern.
        sink = InMemoryIngestionLogSink()
        for action in ("started", "inserted", "inserted", "completed"):
            sink.write(
                IngestionEvent(run_id="r", runner_name="x", action=action)  # type: ignore[arg-type]
            )
        inserts = [e for e in sink.events if e.action == "inserted"]
        assert len(inserts) == 2


class TestNewRunId:
    def test_returns_str(self) -> None:
        rid = new_run_id()
        assert isinstance(rid, str)
        assert len(rid) > 0

    def test_unique_across_calls(self) -> None:
        assert new_run_id() != new_run_id()
