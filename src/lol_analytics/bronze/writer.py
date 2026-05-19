"""BronzeWriter — persists Bronze records to Delta via MERGE INTO.

This is the **Spark half** of Bronze ingestion. It takes the typed,
hashed records produced by `bronze.records` and upserts them into the
Unity Catalog Delta tables defined in `sql/ddl/01_bronze.sql`.

Why `MERGE INTO` and not `INSERT`:
- Riot match data is immutable once a game ends, but the API may be
  polled for the same match twice (backfill re-runs, overlapping match
  lists between players). `MERGE` on the natural key makes re-ingestion
  a no-op instead of a duplicate.
- `WHEN NOT MATCHED THEN INSERT` only — we never update an existing
  Bronze row. If a payload genuinely changed, `payload_hash` differs
  and that is surfaced by querying both rows' history via Delta time
  travel; Bronze itself stays append-only in spirit.

Dependency injection:
- The `SparkSession` is passed in, never created here. Tests inject a
  local session; Databricks jobs inject the cluster session. This also
  keeps the module importable on machines without a working Spark.

Local-Spark caveat (see CLAUDE.md): `MERGE INTO`, Liquid Clustering and
CDF only behave identically to Databricks Runtime on a real workspace.
Unit tests here cover DataFrame construction and schema; the MERGE
semantics are validated by the Sprint 2 Databricks notebook.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from lol_analytics.bronze.records import BronzeLeagueEntryRecord, BronzeMatchRecord

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = structlog.get_logger(__name__)

# Fully-qualified Bronze table names. Single source of truth for the
# writer; the DDL in sql/ddl/01_bronze.sql must stay in sync.
CATALOG = "lol_analytics"
SCHEMA = "bronze"
TABLE_RAW_MATCHES = f"{CATALOG}.{SCHEMA}.raw_matches"
TABLE_RAW_MATCH_TIMELINE = f"{CATALOG}.{SCHEMA}.raw_match_timeline"
TABLE_RAW_LEAGUE_ENTRIES = f"{CATALOG}.{SCHEMA}.raw_league_entries"


# Column order for the match/timeline tables. `ingestion_date` is omitted
# on purpose — it is a Delta generated column.
_MATCH_COLUMNS = [
    "match_id",
    "platform",
    "region",
    "payload",
    "payload_hash",
    "ingestion_timestamp",
    "source_endpoint",
    "api_key_hash",
]

_LEAGUE_COLUMNS = [
    "puuid",
    "summoner_id",
    "platform",
    "queue_type",
    "tier",
    "rank",
    "league_points",
    "wins",
    "losses",
    "payload",
    "ingestion_timestamp",
    "source_endpoint",
]


class MergeResult:
    """Outcome of one `MERGE INTO` call.

    `inserted` is how many source rows were new; `skipped_duplicate` is
    how many already existed (matched on the natural key and therefore
    not re-inserted).
    """

    __slots__ = ("inserted", "skipped_duplicate")

    def __init__(self, inserted: int, skipped_duplicate: int):
        self.inserted = inserted
        self.skipped_duplicate = skipped_duplicate

    @property
    def total(self) -> int:
        return self.inserted + self.skipped_duplicate

    def __repr__(self) -> str:
        return f"MergeResult(inserted={self.inserted}, skipped_duplicate={self.skipped_duplicate})"


class BronzeWriter:
    """Upserts Bronze records into Delta tables.

    One instance per ingestion run is fine. The `SparkSession` is shared
    and not owned by the writer — the caller manages its lifecycle.

    The target table names are exposed as class attributes so callers
    (e.g. runners emitting ingestion-log events) can reference them
    without re-importing the module constants.
    """

    TABLE_RAW_MATCHES = TABLE_RAW_MATCHES
    TABLE_RAW_MATCH_TIMELINE = TABLE_RAW_MATCH_TIMELINE
    TABLE_RAW_LEAGUE_ENTRIES = TABLE_RAW_LEAGUE_ENTRIES

    def __init__(self, spark: SparkSession):
        """Construct the writer.

        Args:
            spark: An active SparkSession with Delta enabled. On
                Databricks this is the notebook/job session; locally
                it is a test fixture.
        """
        self.spark = spark

    # ---------- DataFrame construction (Spark, but pure-ish) ----------

    def _match_dataframe(self, records: list[BronzeMatchRecord]) -> DataFrame:
        """Build a DataFrame for raw_matches / raw_match_timeline."""
        rows = [
            (
                r.match_id,
                r.platform,
                r.region,
                r.payload,
                r.payload_hash,
                r.ingestion_timestamp,
                r.source_endpoint,
                r.api_key_hash,
            )
            for r in records
        ]
        return self.spark.createDataFrame(rows, schema=_MATCH_COLUMNS)

    def _league_dataframe(self, records: list[BronzeLeagueEntryRecord]) -> DataFrame:
        """Build a DataFrame for raw_league_entries."""
        rows = [
            (
                r.puuid,
                r.summoner_id,
                r.platform,
                r.queue_type,
                r.tier,
                r.rank,
                r.league_points,
                r.wins,
                r.losses,
                r.payload,
                r.ingestion_timestamp,
                r.source_endpoint,
            )
            for r in records
        ]
        return self.spark.createDataFrame(rows, schema=_LEAGUE_COLUMNS)

    # ---------- MERGE helpers ----------

    def _merge(
        self,
        df: DataFrame,
        target_table: str,
        on_condition: str,
        temp_view: str,
        columns: list[str],
    ) -> MergeResult:
        """Run an idempotent `WHEN NOT MATCHED THEN INSERT` MERGE.

        Counts inserted vs skipped by diffing the target row count
        before and after. This is simpler and more portable than
        parsing the MERGE operation metrics, and accurate because the
        writer is the only writer to these tables during a run.

        The `INSERT` lists columns explicitly rather than using
        `INSERT *`. The Bronze tables have a generated column
        (`ingestion_date`), and `INSERT *` fails on Databricks when the
        source DataFrame's column set does not match the target's —
        Delta will not let you write a value into a generated column.
        Listing the non-generated columns sidesteps that entirely.

        Args:
            df: Source rows.
            target_table: Fully-qualified Delta table name.
            on_condition: The `ON` predicate joining target and source,
                using aliases `t` (target) and `s` (source).
            temp_view: Name to register `df` under for the SQL MERGE.
            columns: The non-generated columns to insert, in order.
        """
        incoming = df.count()
        before = self.spark.table(target_table).count()

        col_list = ", ".join(columns)
        value_list = ", ".join(f"s.{c}" for c in columns)

        df.createOrReplaceTempView(temp_view)
        self.spark.sql(
            f"""
            MERGE INTO {target_table} AS t
            USING {temp_view} AS s
            ON {on_condition}
            WHEN NOT MATCHED THEN
              INSERT ({col_list})
              VALUES ({value_list})
            """
        )

        after = self.spark.table(target_table).count()
        inserted = after - before
        result = MergeResult(
            inserted=inserted,
            skipped_duplicate=incoming - inserted,
        )
        log.info(
            "bronze_merge_completed",
            target_table=target_table,
            inserted=result.inserted,
            skipped_duplicate=result.skipped_duplicate,
        )
        return result

    # ---------- Public upsert API ----------

    def upsert_matches(self, records: list[BronzeMatchRecord]) -> MergeResult:
        """Upsert match payloads into `bronze.raw_matches`.

        Idempotent on `(match_id, platform)`. Re-running with already
        ingested matches inserts nothing.
        """
        if not records:
            return MergeResult(inserted=0, skipped_duplicate=0)
        df = self._match_dataframe(records)
        return self._merge(
            df,
            TABLE_RAW_MATCHES,
            on_condition="t.match_id = s.match_id AND t.platform = s.platform",
            temp_view="_incoming_raw_matches",
            columns=_MATCH_COLUMNS,
        )

    def upsert_timelines(self, records: list[BronzeMatchRecord]) -> MergeResult:
        """Upsert timeline payloads into `bronze.raw_match_timeline`.

        Same record shape and natural key as matches.
        """
        if not records:
            return MergeResult(inserted=0, skipped_duplicate=0)
        df = self._match_dataframe(records)
        return self._merge(
            df,
            TABLE_RAW_MATCH_TIMELINE,
            on_condition="t.match_id = s.match_id AND t.platform = s.platform",
            temp_view="_incoming_raw_match_timeline",
            columns=_MATCH_COLUMNS,
        )

    def upsert_league_entries(self, records: list[BronzeLeagueEntryRecord]) -> MergeResult:
        """Upsert league entries into `bronze.raw_league_entries`.

        Idempotent on `(puuid, platform, ingestion_date)` — re-running
        the same snapshot on the same day inserts nothing, but a new
        day's snapshot of the same player is a new row (time series).
        """
        if not records:
            return MergeResult(inserted=0, skipped_duplicate=0)
        df = self._league_dataframe(records)
        return self._merge(
            df,
            TABLE_RAW_LEAGUE_ENTRIES,
            on_condition=(
                "t.puuid = s.puuid "
                "AND t.platform = s.platform "
                "AND t.ingestion_date = CAST(s.ingestion_timestamp AS DATE)"
            ),
            temp_view="_incoming_raw_league_entries",
            columns=_LEAGUE_COLUMNS,
        )
