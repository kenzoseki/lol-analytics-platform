"""Tests for `lol_analytics.bronze.records` — pure, no Spark."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime

from lol_analytics.bronze.payload_hash import sha256_hex
from lol_analytics.bronze.records import (
    BronzeLeagueEntryRecord,
    BronzeMatchRecord,
    build_league_entry_records,
    build_match_record,
    canonical_json,
)


class TestCanonicalJson:
    def test_sorts_keys(self) -> None:
        # Same logical payload, different insertion order → same string.
        a = canonical_json({"b": 1, "a": 2})
        b = canonical_json({"a": 2, "b": 1})
        assert a == b == '{"a":2,"b":1}'

    def test_no_incidental_whitespace(self) -> None:
        assert canonical_json({"x": [1, 2]}) == '{"x":[1,2]}'

    def test_non_ascii_preserved(self) -> None:
        # Summoner names with accents stay readable, not \\uXXXX-escaped.
        assert "ção" in canonical_json({"name": "ção"})

    def test_accepts_list_payload(self) -> None:
        assert canonical_json(["BR1_1", "BR1_2"]) == '["BR1_1","BR1_2"]'


class TestBuildMatchRecord:
    def test_basic_fields(self) -> None:
        rec = build_match_record(
            match_id="BR1_123",
            platform="BR1",
            region="americas",
            payload={"info": {"gameDuration": 1800}},
            source_endpoint="/lol/match/v5/matches/BR1_123",
        )
        assert isinstance(rec, BronzeMatchRecord)
        assert rec.match_id == "BR1_123"
        assert rec.platform == "BR1"
        assert rec.region == "americas"
        assert rec.source_endpoint == "/lol/match/v5/matches/BR1_123"
        assert rec.api_key_hash is None

    def test_payload_is_canonical_json(self) -> None:
        rec = build_match_record(
            match_id="BR1_123",
            platform="BR1",
            region="americas",
            payload={"b": 1, "a": 2},
            source_endpoint="/x",
        )
        assert rec.payload == '{"a":2,"b":1}'

    def test_hash_matches_payload(self) -> None:
        rec = build_match_record(
            match_id="BR1_123",
            platform="BR1",
            region="americas",
            payload={"x": 1},
            source_endpoint="/x",
        )
        # The hash must be computed over the exact stored payload string.
        assert rec.payload_hash == sha256_hex(rec.payload)

    def test_same_payload_different_key_order_same_hash(self) -> None:
        # Idempotency hinges on this: re-fetching a match whose JSON keys
        # come back reordered must not look like a changed payload.
        r1 = build_match_record(
            match_id="BR1_123",
            platform="BR1",
            region="americas",
            payload={"a": 1, "b": 2},
            source_endpoint="/x",
        )
        r2 = build_match_record(
            match_id="BR1_123",
            platform="BR1",
            region="americas",
            payload={"b": 2, "a": 1},
            source_endpoint="/x",
        )
        assert r1.payload_hash == r2.payload_hash

    def test_ingestion_timestamp_defaults_to_utc(self) -> None:
        rec = build_match_record(
            match_id="BR1_123",
            platform="BR1",
            region="americas",
            payload={},
            source_endpoint="/x",
        )
        assert rec.ingestion_timestamp.tzinfo is not None
        assert (
            rec.ingestion_timestamp.tzinfo.utcoffset(rec.ingestion_timestamp).total_seconds() == 0
        )

    def test_ingestion_timestamp_override(self) -> None:
        when = datetime(2026, 5, 1, tzinfo=UTC)
        rec = build_match_record(
            match_id="BR1_123",
            platform="BR1",
            region="americas",
            payload={},
            source_endpoint="/x",
            ingestion_timestamp=when,
        )
        assert rec.ingestion_timestamp == when

    def test_api_key_hash_passthrough(self) -> None:
        rec = build_match_record(
            match_id="BR1_123",
            platform="BR1",
            region="americas",
            payload={},
            source_endpoint="/x",
            api_key_hash="ab12",
        )
        assert rec.api_key_hash == "ab12"

    def test_record_is_frozen(self) -> None:
        rec = build_match_record(
            match_id="BR1_123",
            platform="BR1",
            region="americas",
            payload={},
            source_endpoint="/x",
        )
        try:
            rec.match_id = "mutated"  # type: ignore[misc]
        except dataclasses.FrozenInstanceError:
            return
        raise AssertionError("BronzeMatchRecord should be frozen")


class TestBuildLeagueEntryRecords:
    def _league_payload(self) -> dict:
        # fixture: synthetic data — shape of a masterleagues response
        return {
            "tier": "MASTER",
            "queue": "RANKED_SOLO_5x5",
            "entries": [
                {
                    "puuid": "puuid-A",
                    "summonerId": "summ-A",
                    "leaguePoints": 500,
                    "wins": 120,
                    "losses": 90,
                },
                {
                    "puuid": "puuid-B",
                    "leaguePoints": 320,
                    "wins": 60,
                    "losses": 55,
                },
            ],
        }

    def test_one_record_per_entry(self) -> None:
        recs = build_league_entry_records(
            league_payload=self._league_payload(),
            platform="BR1",
            tier="MASTER",
            source_endpoint="/lol/league/v4/masterleagues/by-queue/RANKED_SOLO_5x5",
        )
        assert len(recs) == 2
        assert all(isinstance(r, BronzeLeagueEntryRecord) for r in recs)

    def test_fields_extracted(self) -> None:
        recs = build_league_entry_records(
            league_payload=self._league_payload(),
            platform="BR1",
            tier="MASTER",
            source_endpoint="/x",
        )
        a = recs[0]
        assert a.puuid == "puuid-A"
        assert a.summoner_id == "summ-A"
        assert a.platform == "BR1"
        assert a.tier == "MASTER"
        assert a.queue_type == "RANKED_SOLO_5x5"
        assert a.league_points == 500
        assert a.wins == 120
        assert a.losses == 90

    def test_missing_summoner_id_is_none(self) -> None:
        recs = build_league_entry_records(
            league_payload=self._league_payload(),
            platform="BR1",
            tier="MASTER",
            source_endpoint="/x",
        )
        # Entry B has no summonerId — should be None, not KeyError.
        assert recs[1].summoner_id is None

    def test_missing_rank_is_none(self) -> None:
        # Apex tiers have no division; rank is absent in the payload.
        recs = build_league_entry_records(
            league_payload=self._league_payload(),
            platform="BR1",
            tier="MASTER",
            source_endpoint="/x",
        )
        assert recs[0].rank is None

    def test_empty_entries_returns_empty_list(self) -> None:
        recs = build_league_entry_records(
            league_payload={"tier": "MASTER", "entries": []},
            platform="BR1",
            tier="MASTER",
            source_endpoint="/x",
        )
        assert recs == []

    def test_no_entries_key_returns_empty_list(self) -> None:
        recs = build_league_entry_records(
            league_payload={"tier": "MASTER"},
            platform="BR1",
            tier="MASTER",
            source_endpoint="/x",
        )
        assert recs == []

    def test_queue_type_defaults_when_absent(self) -> None:
        recs = build_league_entry_records(
            league_payload={"entries": [{"puuid": "p", "leaguePoints": 1, "wins": 1, "losses": 1}]},
            platform="BR1",
            tier="MASTER",
            source_endpoint="/x",
        )
        assert recs[0].queue_type == "RANKED_SOLO_5x5"

    def test_all_entries_share_ingestion_timestamp(self) -> None:
        when = datetime(2026, 5, 1, tzinfo=UTC)
        recs = build_league_entry_records(
            league_payload=self._league_payload(),
            platform="BR1",
            tier="MASTER",
            source_endpoint="/x",
            ingestion_timestamp=when,
        )
        assert all(r.ingestion_timestamp == when for r in recs)

    def test_entry_payload_is_canonical_json(self) -> None:
        recs = build_league_entry_records(
            league_payload=self._league_payload(),
            platform="BR1",
            tier="MASTER",
            source_endpoint="/x",
        )
        # Each entry's payload is the canonical JSON of that entry alone.
        assert recs[0].payload == canonical_json(self._league_payload()["entries"][0])
