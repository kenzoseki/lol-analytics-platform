# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion — matches
# MAGIC
# MAGIC Fetches full match payloads from `match-v5` and upserts them into
# MAGIC `lol_analytics.bronze.raw_matches`. This notebook **is** the Bronze
# MAGIC match-ingestion pipeline — there is no separate orchestration layer
# MAGIC in `src/` (see ADR 004, Databricks-first simplification).
# MAGIC
# MAGIC What it does:
# MAGIC 1. Imports the pure-logic helpers from the `lol_analytics` package
# MAGIC    (`RiotApiClient`, `RiotRateLimiter`, `build_match_record`).
# MAGIC 2. Sources ~60 recent ranked match IDs from top Challenger players.
# MAGIC 3. Fetches each match (async, rate-limited).
# MAGIC 4. `MERGE INTO` the Bronze table — idempotent on `(match_id, platform)`.
# MAGIC 5. Drains the client's dead-letter list to `ingestion_dead_letter`.
# MAGIC 6. Writes an `ingestion_log` summary row.
# MAGIC 7. Validation cells: idempotency re-run, row counts.
# MAGIC
# MAGIC **Pre-requisites:** Sprint 1 setup notebook already run (catalog +
# MAGIC Bronze tables exist); `RIOT_API_KEY` available as a Databricks
# MAGIC secret; repo connected as a Git Folder.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Install the project package
# MAGIC
# MAGIC Auto-detects the repo root from this notebook's own path, so there
# MAGIC is no hardcoded `/Workspace/Users/...` to break.

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
# MAGIC
# MAGIC Edit the widgets to change platform or how many matches to ingest.

# COMMAND ----------

dbutils.widgets.text("platform", "BR1", "Platform shard")  # noqa: F821
dbutils.widgets.text("match_target", "60", "Number of matches to ingest")  # noqa: F821

PLATFORM = dbutils.widgets.get("platform")  # noqa: F821
MATCH_TARGET = int(dbutils.widgets.get("match_target"))  # noqa: F821

# COMMAND ----------

import os

from lol_analytics.ingestion.rate_limiter import RiotRateLimiter
from lol_analytics.ingestion.riot_client import RiotApiClient
from lol_analytics.utils.logging import configure_logging

# Riot API key from the Databricks secret scope (create once via the CLI:
# `databricks secrets create-scope lol-analytics`).
os.environ["RIOT_API_KEY"] = dbutils.secrets.get(  # noqa: F821
    scope="lol-analytics", key="riot-api-key"
)
RIOT_API_KEY = os.environ["RIOT_API_KEY"]
API_KEY_HASH = RIOT_API_KEY[-4:]  # only the last 4 chars are safe to persist

configure_logging("INFO")

REGION = RiotApiClient.region_for_platform(PLATFORM)
rate_limiter = RiotRateLimiter(windows=[(20, 1.0), (100, 120.0)])
print(f"Platform={PLATFORM}  Region={REGION}  Target={MATCH_TARGET} matches")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Collect match IDs
# MAGIC
# MAGIC Take a handful of top Challenger players, pull their recent ranked
# MAGIC match IDs, dedup, and cap at `MATCH_TARGET`.

# COMMAND ----------

import asyncio


async def collect_match_ids() -> list[str]:
    async with RiotApiClient(RIOT_API_KEY, rate_limiter) as client:
        league = await client.get_challenger_league(PLATFORM)
        top_puuids = [e["puuid"] for e in league.get("entries", [])[:6]]

        seen: list[str] = []
        for puuid in top_puuids:
            ids = await client.get_match_ids_by_puuid(REGION, puuid, count=15)
            for mid in ids:
                if mid not in seen:
                    seen.append(mid)
            if len(seen) >= MATCH_TARGET:
                break
        return seen[:MATCH_TARGET]


match_ids = asyncio.run(collect_match_ids())
print(f"Collected {len(match_ids)} unique match IDs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Fetch the match payloads
# MAGIC
# MAGIC Async fetch under the rate limiter. A match that fails terminally is
# MAGIC dead-lettered by the client (its dict lands in `client.dead_letters`)
# MAGIC and skipped — one bad match does not abort the batch.

# COMMAND ----------

from lol_analytics.ingestion.riot_client import RiotApiClient as _Client
from lol_analytics.bronze.records import build_match_record


async def fetch_matches(ids: list[str]) -> tuple[list, list[dict]]:
    """Returns (records, dead_letters)."""
    records = []
    async with _Client(RIOT_API_KEY, rate_limiter) as client:
        for match_id in ids:
            try:
                payload = await client.get_match(REGION, match_id)
            except Exception:  # noqa: BLE001 — already dead-lettered by the client
                print(f"  skipped {match_id} (see dead-letter queue)")
                continue
            records.append(
                build_match_record(
                    match_id=match_id,
                    platform=PLATFORM,
                    region=REGION,
                    payload=payload,
                    source_endpoint=f"/lol/match/v5/matches/{match_id}",
                    api_key_hash=API_KEY_HASH,
                )
            )
        dead_letters = list(client.dead_letters)
    return records, dead_letters


records, dead_letters = asyncio.run(fetch_matches(match_ids))
print(f"Fetched {len(records)} matches, {len(dead_letters)} dead-letters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — MERGE into `bronze.raw_matches`
# MAGIC
# MAGIC `MERGE INTO` is idempotent on `(match_id, platform)`. The `INSERT`
# MAGIC lists columns explicitly — `INSERT *` fails because `ingestion_date`
# MAGIC is a generated column and cannot be written directly.

# COMMAND ----------

from datetime import datetime

MATCH_COLUMNS = [
    "match_id", "platform", "region", "payload", "payload_hash",
    "ingestion_timestamp", "source_endpoint", "api_key_hash",
]


def merge_matches(records: list) -> tuple[int, int]:
    """MERGE the records; returns (inserted, skipped_duplicate)."""
    if not records:
        return 0, 0

    rows = [
        (r.match_id, r.platform, r.region, r.payload, r.payload_hash,
         r.ingestion_timestamp, r.source_endpoint, r.api_key_hash)
        for r in records
    ]
    df = spark.createDataFrame(rows, schema=MATCH_COLUMNS)  # noqa: F821
    df.createOrReplaceTempView("_incoming_matches")

    target = "lol_analytics.bronze.raw_matches"
    before = spark.table(target).count()  # noqa: F821
    col_list = ", ".join(MATCH_COLUMNS)
    value_list = ", ".join(f"s.{c}" for c in MATCH_COLUMNS)
    spark.sql(  # noqa: F821
        f"""
        MERGE INTO {target} AS t
        USING _incoming_matches AS s
        ON t.match_id = s.match_id AND t.platform = s.platform
        WHEN NOT MATCHED THEN
          INSERT ({col_list})
          VALUES ({value_list})
        """
    )
    after = spark.table(target).count()  # noqa: F821
    inserted = after - before
    return inserted, len(records) - inserted


inserted, skipped = merge_matches(records)
print(f"raw_matches: inserted={inserted}, skipped_duplicate={skipped}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Persist dead-letters and an ingestion-log row
# MAGIC
# MAGIC The client's dead-letter dicts already match the
# MAGIC `ingestion_dead_letter` columns, so they append straight to Delta.
# MAGIC One summary row goes to `ingestion_log`.

# COMMAND ----------

import uuid

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

# One ingestion_log summary row. Columns match the table; the generated
# `emitted_at_date` is omitted.
log_cols = [
    "event_id", "run_id", "runner_name", "action", "platform",
    "target_table", "rows_affected", "error_class", "error_message",
    "duration_ms", "emitted_at",
]
run_id = str(uuid.uuid4())
log_row = (
    str(uuid.uuid4()), run_id, "notebook_ingest_matches", "completed",
    PLATFORM, "lol_analytics.bronze.raw_matches", inserted, None, None,
    None, datetime.utcnow(),
)
(
    spark.createDataFrame([log_row], schema=log_cols)  # noqa: F821
    .write.format("delta").mode("append")
    .saveAsTable("lol_analytics.bronze.ingestion_log")
)
print(f"Ingestion-log row written (run_id={run_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Validation
# MAGIC
# MAGIC ### 7a. MERGE idempotency — re-run inserts nothing

# COMMAND ----------

reinserted, reskipped = merge_matches(records)
print(f"Re-run: inserted={reinserted} (expected 0), skipped={reskipped}")
assert reinserted == 0, "MERGE is not idempotent — re-run inserted rows"
print("Idempotency confirmed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7b. Row counts and payload_hash coverage

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT(DISTINCT match_id) AS distinct_matches,
# MAGIC   COUNT(payload_hash) AS rows_with_hash
# MAGIC FROM lol_analytics.bronze.raw_matches
# MAGIC WHERE match_id NOT LIKE 'TEST_%';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dead-letters captured this run (if any)
# MAGIC SELECT endpoint, http_status, error_class, COUNT(*) AS failures
# MAGIC FROM lol_analytics.bronze.ingestion_dead_letter
# MAGIC GROUP BY endpoint, http_status, error_class
# MAGIC ORDER BY failures DESC;
