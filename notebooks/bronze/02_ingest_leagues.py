# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion — league entries
# MAGIC
# MAGIC Snapshots the apex-tier league tables (Challenger / Grandmaster /
# MAGIC Master) for a platform and upserts every entry into
# MAGIC `lol_analytics.bronze.raw_league_entries`. One run is one daily
# MAGIC snapshot. See ADR 004 for why ingestion lives in notebooks.
# MAGIC
# MAGIC **Pre-requisites:** Sprint 1 setup notebook run; `RIOT_API_KEY` in
# MAGIC the `lol-analytics` Databricks secret scope; repo as a Git Folder.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Install the project package

# COMMAND ----------

_notebook_path = (
    dbutils.notebook.entry_point.getDbutils()  # noqa: F821 — Databricks builtin
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
repo_root = "/Workspace" + _notebook_path.rsplit("/notebooks/", 1)[0]
print(f"Repo root detected: {repo_root}")

# COMMAND ----------

# MAGIC %pip install -e $repo_root

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821 — Databricks builtin

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Parameters and the Riot client

# COMMAND ----------

dbutils.widgets.text("platform", "BR1", "Platform shard")  # noqa: F821
PLATFORM = dbutils.widgets.get("platform")  # noqa: F821

# COMMAND ----------

import os

from lol_analytics.ingestion.rate_limiter import RiotRateLimiter
from lol_analytics.ingestion.riot_client import RiotApiClient
from lol_analytics.utils.logging import configure_logging

os.environ["RIOT_API_KEY"] = dbutils.secrets.get(  # noqa: F821
    scope="lol-analytics", key="riot-api-key"
)
RIOT_API_KEY = os.environ["RIOT_API_KEY"]

configure_logging("INFO")
rate_limiter = RiotRateLimiter(windows=[(20, 1.0), (100, 120.0)])
QUEUE = "RANKED_SOLO_5x5"
print(f"Platform={PLATFORM}  Queue={QUEUE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Fetch the three apex tiers
# MAGIC
# MAGIC `challengerleagues` / `grandmasterleagues` / `masterleagues` each
# MAGIC return a single league object with an `entries` array. A tier that
# MAGIC fails terminally is dead-lettered by the client and skipped.

# COMMAND ----------

import asyncio

from lol_analytics.bronze.records import build_league_entry_records

APEX_TIERS = ("CHALLENGER", "GRANDMASTER", "MASTER")


async def fetch_league_entries() -> tuple[list, list[dict]]:
    """Returns (records, dead_letters)."""
    records = []
    async with RiotApiClient(RIOT_API_KEY, rate_limiter) as client:
        for tier in APEX_TIERS:
            try:
                if tier == "CHALLENGER":
                    payload = await client.get_challenger_league(PLATFORM, QUEUE)
                elif tier == "GRANDMASTER":
                    payload = await client.get_grandmaster_league(PLATFORM, QUEUE)
                else:
                    payload = await client.get_master_league(PLATFORM, QUEUE)
            except Exception:  # noqa: BLE001 — already dead-lettered by the client
                print(f"  skipped {tier} (see dead-letter queue)")
                continue
            endpoint = f"/lol/league/v4/{tier.lower()}leagues/by-queue/{QUEUE}"
            records.extend(
                build_league_entry_records(
                    league_payload=payload,
                    platform=PLATFORM,
                    tier=tier,
                    source_endpoint=endpoint,
                )
            )
        dead_letters = list(client.dead_letters)
    return records, dead_letters


records, dead_letters = asyncio.run(fetch_league_entries())
print(f"Fetched {len(records)} league entries, {len(dead_letters)} dead-letters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — MERGE into `bronze.raw_league_entries`
# MAGIC
# MAGIC Idempotent on `(puuid, platform, ingestion_date)`: re-running the
# MAGIC same snapshot on the same day inserts nothing, but tomorrow's
# MAGIC snapshot of the same player is a new row (the table is a time
# MAGIC series). `INSERT` lists columns explicitly — `ingestion_date` is a
# MAGIC generated column.

# COMMAND ----------

LEAGUE_COLUMNS = [
    "puuid", "summoner_id", "platform", "queue_type", "tier", "rank",
    "league_points", "wins", "losses", "payload", "ingestion_timestamp",
    "source_endpoint",
]


def merge_league_entries(records: list) -> tuple[int, int]:
    """MERGE the records; returns (inserted, skipped_duplicate)."""
    if not records:
        return 0, 0

    rows = [
        (r.puuid, r.summoner_id, r.platform, r.queue_type, r.tier, r.rank,
         r.league_points, r.wins, r.losses, r.payload,
         r.ingestion_timestamp, r.source_endpoint)
        for r in records
    ]
    df = spark.createDataFrame(rows, schema=LEAGUE_COLUMNS)  # noqa: F821
    df.createOrReplaceTempView("_incoming_leagues")

    target = "lol_analytics.bronze.raw_league_entries"
    before = spark.table(target).count()  # noqa: F821
    col_list = ", ".join(LEAGUE_COLUMNS)
    value_list = ", ".join(f"s.{c}" for c in LEAGUE_COLUMNS)
    spark.sql(  # noqa: F821
        f"""
        MERGE INTO {target} AS t
        USING _incoming_leagues AS s
        ON t.puuid = s.puuid
           AND t.platform = s.platform
           AND t.ingestion_date = CAST(s.ingestion_timestamp AS DATE)
        WHEN NOT MATCHED THEN
          INSERT ({col_list})
          VALUES ({value_list})
        """
    )
    after = spark.table(target).count()  # noqa: F821
    inserted = after - before
    return inserted, len(records) - inserted


inserted, skipped = merge_league_entries(records)
print(f"raw_league_entries: inserted={inserted}, skipped_duplicate={skipped}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Persist dead-letters and an ingestion-log row

# COMMAND ----------

import uuid
from datetime import datetime

if dead_letters:
    dl_cols = [
        "request_id", "endpoint", "url", "http_status", "error_class",
        "error_message", "request_payload", "attempt_count", "failed_at",
    ]
    dl_rows = [tuple(d[c] for c in dl_cols) for d in dead_letters]
    (
        spark.createDataFrame(dl_rows, schema=dl_cols)  # noqa: F821
        .write.format("delta").mode("append")
        .saveAsTable("lol_analytics.bronze.ingestion_dead_letter")
    )
    print(f"Wrote {len(dead_letters)} dead-letter rows")
else:
    print("No dead-letters this run")

# COMMAND ----------

log_cols = [
    "event_id", "run_id", "runner_name", "action", "platform",
    "target_table", "rows_affected", "error_class", "error_message",
    "duration_ms", "emitted_at",
]
run_id = str(uuid.uuid4())
log_row = (
    str(uuid.uuid4()), run_id, "notebook_ingest_leagues", "completed",
    PLATFORM, "lol_analytics.bronze.raw_league_entries", inserted, None,
    None, None, datetime.utcnow(),
)
(
    spark.createDataFrame([log_row], schema=log_cols)  # noqa: F821
    .write.format("delta").mode("append")
    .saveAsTable("lol_analytics.bronze.ingestion_log")
)
print(f"Ingestion-log row written (run_id={run_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Validation

# COMMAND ----------

reinserted, _ = merge_league_entries(records)
print(f"Re-run: inserted={reinserted} (expected 0)")
assert reinserted == 0, "MERGE is not idempotent — re-run inserted rows"
print("Idempotency confirmed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tier, COUNT(*) AS players
# MAGIC FROM lol_analytics.bronze.raw_league_entries
# MAGIC GROUP BY tier ORDER BY tier;
