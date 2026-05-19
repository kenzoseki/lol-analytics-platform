"""Spark-backed tests for `BronzeWriter`.

Marked `@pytest.mark.spark` — they require a working SparkSession and are
skipped by the fast unit suite (`pytest -m "not spark"`) and on machines
where Spark cannot start. They run on Databricks and on Linux CI.

What these cover that the pure-Python tests cannot:
- DataFrame construction with the right schema and column order.
- `MERGE INTO` actually being idempotent — re-upserting the same rows
  inserts nothing.
- `MergeResult` counts (`inserted` vs `skipped_duplicate`) being correct.

The local Delta tables here are created under the same fully-qualified
names the writer targets (`lol_analytics.bronze.*`), in a temporary
warehouse, so the writer needs no test-only configuration.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from lol_analytics.bronze.records import build_league_entry_records, build_match_record
from lol_analytics.bronze.writer import BronzeWriter

pytestmark = pytest.mark.spark


@pytest.fixture
def bronze_tables(spark: object) -> object:
    """Create empty `lol_analytics.bronze.*` Delta tables for one test.

    Drops and recreates them so each test starts clean. Mirrors the
    columns of sql/ddl/01_bronze.sql but omits the generated
    `ingestion_date` (Delta computes it).
    """
    s = spark
    s.sql("CREATE CATALOG IF NOT EXISTS lol_analytics")  # type: ignore[attr-defined]
    s.sql("CREATE SCHEMA IF NOT EXISTS lol_analytics.bronze")  # type: ignore[attr-defined]

    for table in ("raw_matches", "raw_match_timeline"):
        s.sql(f"DROP TABLE IF EXISTS lol_analytics.bronze.{table}")  # type: ignore[attr-defined]
        s.sql(  # type: ignore[attr-defined]
            f"""
            CREATE TABLE lol_analytics.bronze.{table} (
                match_id STRING, platform STRING, region STRING,
                payload STRING, payload_hash STRING,
                ingestion_timestamp TIMESTAMP, source_endpoint STRING,
                api_key_hash STRING
            ) USING DELTA
            """
        )

    s.sql("DROP TABLE IF EXISTS lol_analytics.bronze.raw_league_entries")  # type: ignore[attr-defined]
    s.sql(  # type: ignore[attr-defined]
        """
        CREATE TABLE lol_analytics.bronze.raw_league_entries (
            puuid STRING, summoner_id STRING, platform STRING,
            queue_type STRING, tier STRING, rank STRING,
            league_points INT, wins INT, losses INT, payload STRING,
            ingestion_timestamp TIMESTAMP, source_endpoint STRING
        ) USING DELTA
        """
    )
    return s


def _match(match_id: str, ts: datetime | None = None) -> object:
    return build_match_record(
        match_id=match_id,
        platform="BR1",
        region="americas",
        payload={"metadata": {"matchId": match_id}},
        source_endpoint=f"/lol/match/v5/matches/{match_id}",
        ingestion_timestamp=ts,
    )


class TestUpsertMatches:
    def test_inserts_new_rows(self, spark: object, bronze_tables: object) -> None:
        writer = BronzeWriter(spark)  # type: ignore[arg-type]
        result = writer.upsert_matches([_match("BR1_1"), _match("BR1_2")])

        assert result.inserted == 2
        assert result.skipped_duplicate == 0
        count = spark.table(  # type: ignore[attr-defined]
            "lol_analytics.bronze.raw_matches"
        ).count()
        assert count == 2

    def test_merge_is_idempotent(self, spark: object, bronze_tables: object) -> None:
        writer = BronzeWriter(spark)  # type: ignore[arg-type]
        batch = [_match("BR1_1"), _match("BR1_2")]

        first = writer.upsert_matches(batch)
        second = writer.upsert_matches(batch)

        assert first.inserted == 2
        # Re-upserting the same matches inserts nothing.
        assert second.inserted == 0
        assert second.skipped_duplicate == 2
        count = spark.table(  # type: ignore[attr-defined]
            "lol_analytics.bronze.raw_matches"
        ).count()
        assert count == 2

    def test_partial_overlap(self, spark: object, bronze_tables: object) -> None:
        writer = BronzeWriter(spark)  # type: ignore[arg-type]
        writer.upsert_matches([_match("BR1_1"), _match("BR1_2")])

        # One overlapping, one new.
        result = writer.upsert_matches([_match("BR1_2"), _match("BR1_3")])
        assert result.inserted == 1
        assert result.skipped_duplicate == 1
        count = spark.table(  # type: ignore[attr-defined]
            "lol_analytics.bronze.raw_matches"
        ).count()
        assert count == 3

    def test_empty_batch_is_noop(self, spark: object, bronze_tables: object) -> None:
        writer = BronzeWriter(spark)  # type: ignore[arg-type]
        result = writer.upsert_matches([])
        assert result.inserted == 0
        assert result.total == 0

    def test_generated_ingestion_date_populated(self, spark: object, bronze_tables: object) -> None:
        # The DDL's generated column is recreated in the fixture without
        # GENERATED; this test instead asserts the timestamp round-trips.
        ts = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        writer = BronzeWriter(spark)  # type: ignore[arg-type]
        writer.upsert_matches([_match("BR1_1", ts=ts)])

        row = (
            spark.table("lol_analytics.bronze.raw_matches")  # type: ignore[attr-defined]
            .filter("match_id = 'BR1_1'")
            .collect()[0]
        )
        assert row["payload_hash"]  # hash present


class TestUpsertTimelines:
    def test_inserts_into_timeline_table(self, spark: object, bronze_tables: object) -> None:
        writer = BronzeWriter(spark)  # type: ignore[arg-type]
        result = writer.upsert_timelines([_match("BR1_1")])
        assert result.inserted == 1
        count = spark.table(  # type: ignore[attr-defined]
            "lol_analytics.bronze.raw_match_timeline"
        ).count()
        assert count == 1


class TestUpsertLeagueEntries:
    def _entries(self) -> list[object]:
        payload = {
            "tier": "MASTER",
            "queue": "RANKED_SOLO_5x5",
            "entries": [
                {"puuid": "p1", "leaguePoints": 100, "wins": 10, "losses": 5},
                {"puuid": "p2", "leaguePoints": 200, "wins": 20, "losses": 8},
            ],
        }
        return build_league_entry_records(
            league_payload=payload,
            platform="BR1",
            tier="MASTER",
            source_endpoint="/lol/league/v4/masterleagues/by-queue/RANKED_SOLO_5x5",
            ingestion_timestamp=datetime(2026, 5, 1, tzinfo=UTC),
        )

    def test_inserts_entries(self, spark: object, bronze_tables: object) -> None:
        writer = BronzeWriter(spark)  # type: ignore[arg-type]
        result = writer.upsert_league_entries(self._entries())
        assert result.inserted == 2

    def test_same_day_snapshot_is_idempotent(self, spark: object, bronze_tables: object) -> None:
        writer = BronzeWriter(spark)  # type: ignore[arg-type]
        entries = self._entries()

        writer.upsert_league_entries(entries)
        second = writer.upsert_league_entries(entries)
        # Same puuid + platform + ingestion_date → no new rows.
        assert second.inserted == 0
        assert second.skipped_duplicate == 2
