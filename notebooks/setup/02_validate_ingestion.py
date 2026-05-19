# Databricks notebook source
# MAGIC %md
# MAGIC # Sprint 2 Validation — Bronze ingestion against real Riot data
# MAGIC
# MAGIC Closes **Definition of Done item 7** for Sprint 2: proves the
# MAGIC ingestion *code path* (not just the table infrastructure, which
# MAGIC Sprint 1 already validated) works end-to-end on a real Databricks
# MAGIC workspace with real Riot API data.
# MAGIC
# MAGIC | Step | Validates |
# MAGIC |---|---|
# MAGIC | 3 | `LeagueEntriesIngestionRunner` → `raw_league_entries` populated |
# MAGIC | 4 | `MatchIngestionRunner` → `raw_matches` populated, `payload_hash` set |
# MAGIC | 5 | MERGE idempotency — re-running ingestion inserts nothing |
# MAGIC | 6 | `TimelineIngestionRunner` → `raw_match_timeline` populated |
# MAGIC | 7 | `ingestion_log` populated with started/inserted/completed events |
# MAGIC | 8 | Dead-letter queue behaviour (intentional bad match ID) |
# MAGIC
# MAGIC **Pre-requisites:**
# MAGIC - Sprint 1 validation notebook already run (catalog + Bronze tables exist).
# MAGIC - The repo is connected as a Git Folder and pulled to `main`.
# MAGIC - A valid `RIOT_API_KEY` available as a Databricks secret or notebook
# MAGIC   widget. Dev keys expire every 24h — refresh before running.
# MAGIC - Compute: serverless (Free Edition).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Install the project package
# MAGIC
# MAGIC The Git Folder gives us the source; install it (editable) so
# MAGIC `import lol_analytics` works. `httpx`, `tenacity`, `structlog`,
# MAGIC `pydantic-settings`, `typer` come along as dependencies.
# MAGIC
# MAGIC The repo root is **auto-detected** from this notebook's own path —
# MAGIC no need to hardcode `/Workspace/Users/<you>/...`. The detected path
# MAGIC is exposed as `$repo_root`, which the `%pip` magic in the next cell
# MAGIC expands (Databricks substitutes notebook-scoped Python variables
# MAGIC into `%pip` lines).

# COMMAND ----------

# This notebook lives at <repo_root>/notebooks/setup/02_validate_ingestion.py,
# so the repo root is three directories up. `dbutils.notebook.entry_point`
# is the documented way to get the running notebook's workspace path.
_notebook_path = (
    dbutils.notebook.entry_point.getDbutils()  # noqa: F821 — Databricks builtin
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
# /Workspace prefix makes it a real filesystem path the pip can install from.
repo_root = "/Workspace" + _notebook_path.rsplit("/notebooks/", 1)[0]
print(f"Repo root detected: {repo_root}")

# COMMAND ----------

# MAGIC %pip install -e $repo_root

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821 — dbutils is a Databricks builtin

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Configure the API key and build the ingestion stack
# MAGIC
# MAGIC Reads `RIOT_API_KEY` from a Databricks secret scope. Create the
# MAGIC secret once with the Databricks CLI:
# MAGIC
# MAGIC ```
# MAGIC databricks secrets create-scope lol-analytics
# MAGIC databricks secrets put-secret lol-analytics riot-api-key
# MAGIC ```

# COMMAND ----------

import os

# Pull the key from the secret scope into the environment so
# `pydantic-settings` (Settings) picks it up like it would from .env.
os.environ["RIOT_API_KEY"] = dbutils.secrets.get(  # noqa: F821
    scope="lol-analytics", key="riot-api-key"
)

from lol_analytics.ingestion.bootstrap import build_ingestion_stack
from lol_analytics.utils.config import get_settings
from lol_analytics.utils.logging import configure_logging

settings = get_settings()
configure_logging(settings.log_level)

# `spark` is the notebook-provided SparkSession.
stack = build_ingestion_stack(settings, spark)  # noqa: F821
print("Ingestion stack built. API key hash:", stack.api_key_hash)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Pull league entries for BR1
# MAGIC
# MAGIC Snapshots Challenger + Grandmaster + Master into
# MAGIC `raw_league_entries`. This also gives us a pool of PUUIDs to source
# MAGIC match IDs from in the next step.

# COMMAND ----------

import asyncio

from lol_analytics.ingestion.runners import LeagueEntriesIngestionRunner

league_runner = LeagueEntriesIngestionRunner(
    stack.client, stack.writer, log_sink=stack.log_sink
)


async def _pull_leagues() -> int:
    async with stack.client:
        return await league_runner.run(platform="BR1")


league_rows = asyncio.run(_pull_leagues())
print(f"League entries inserted: {league_rows}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tier, COUNT(*) AS players
# MAGIC FROM lol_analytics.bronze.raw_league_entries
# MAGIC WHERE platform = 'BR1'
# MAGIC GROUP BY tier ORDER BY tier;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Pull ~60 matches
# MAGIC
# MAGIC Take a few Challenger players, fetch their recent ranked match IDs,
# MAGIC dedup, cap at ~60, and ingest. 60 is within the Definition of Done
# MAGIC target of 50–100 and comfortably inside the dev-key rate limit.

# COMMAND ----------

from lol_analytics.ingestion.riot_client import RiotApiClient
from lol_analytics.ingestion.runners import MatchIngestionRunner

REGION = RiotApiClient.region_for_platform("BR1")
MATCH_TARGET = 60


async def _collect_match_ids() -> list[str]:
    """Source match IDs from a handful of top players' recent history."""
    async with stack.client:
        league = await stack.client.get_challenger_league("BR1")
        top_puuids = [e["puuid"] for e in league.get("entries", [])[:6]]

        seen: list[str] = []
        for puuid in top_puuids:
            ids = await stack.client.get_match_ids_by_puuid(
                REGION, puuid, count=15
            )
            for mid in ids:
                if mid not in seen:
                    seen.append(mid)
            if len(seen) >= MATCH_TARGET:
                break
        return seen[:MATCH_TARGET]


match_ids = asyncio.run(_collect_match_ids())
print(f"Collected {len(match_ids)} unique match IDs")

# COMMAND ----------

match_runner = MatchIngestionRunner(
    stack.client, stack.writer, log_sink=stack.log_sink,
    api_key_hash=stack.api_key_hash,
)


async def _pull_matches() -> int:
    async with stack.client:
        return await match_runner.run(
            region=REGION, platform="BR1", match_ids=match_ids
        )


inserted = asyncio.run(_pull_matches())
print(f"Matches inserted: {inserted}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT(DISTINCT match_id) AS distinct_matches,
# MAGIC   COUNT(payload_hash) AS rows_with_hash,
# MAGIC   MIN(ingestion_date) AS first_date
# MAGIC FROM lol_analytics.bronze.raw_matches
# MAGIC WHERE match_id NOT LIKE 'TEST_%';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — MERGE idempotency
# MAGIC
# MAGIC Re-run the exact same match ingestion. Expectation: **0 rows
# MAGIC inserted** the second time — every match already exists, MERGE
# MAGIC skips them all.

# COMMAND ----------

second_run = asyncio.run(_pull_matches())
print(f"Second run inserted: {second_run}  (expected: 0)")
assert second_run == 0, "MERGE is not idempotent — duplicates were inserted"
print("Idempotency confirmed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Pull timelines for the same matches
# MAGIC
# MAGIC Timelines are large; ingesting the same ~60 matches' timelines
# MAGIC exercises `TimelineIngestionRunner` and `raw_match_timeline`.

# COMMAND ----------

from lol_analytics.ingestion.runners import TimelineIngestionRunner

timeline_runner = TimelineIngestionRunner(
    stack.client, stack.writer, log_sink=stack.log_sink,
    api_key_hash=stack.api_key_hash,
)


async def _pull_timelines() -> int:
    async with stack.client:
        return await timeline_runner.run(
            region=REGION, platform="BR1", match_ids=match_ids
        )


timelines_inserted = asyncio.run(_pull_timelines())
print(f"Timelines inserted: {timelines_inserted}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Inspect the ingestion log
# MAGIC
# MAGIC Every runner emits `started` → `inserted`/`skipped_duplicate` →
# MAGIC `completed`. There should be one `run_id` per runner invocation
# MAGIC (4 so far: leagues, matches x2, timelines).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT runner_name, action, COUNT(*) AS events,
# MAGIC        SUM(rows_affected) AS total_rows
# MAGIC FROM lol_analytics.bronze.ingestion_log
# MAGIC GROUP BY runner_name, action
# MAGIC ORDER BY runner_name, action;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Dead-letter queue
# MAGIC
# MAGIC Ingest a deliberately invalid match ID. The client retries, then
# MAGIC writes a dead-letter row (404, non-retryable). The runner skips it
# MAGIC and finishes — one bad ID does not abort the batch.

# COMMAND ----------

BAD_MATCH = "BR1_0000000000"  # fixture: synthetic data — match that cannot exist


async def _pull_bad() -> int:
    async with stack.client:
        return await match_runner.run(
            region=REGION, platform="BR1", match_ids=[BAD_MATCH]
        )


bad_inserted = asyncio.run(_pull_bad())
print(f"Bad-match run inserted: {bad_inserted}  (expected: 0)")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT endpoint, http_status, error_class, COUNT(*) AS failures
# MAGIC FROM lol_analytics.bronze.ingestion_dead_letter
# MAGIC GROUP BY endpoint, http_status, error_class
# MAGIC ORDER BY failures DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Final inventory
# MAGIC
# MAGIC Sprint 2 validation passes when:
# MAGIC - `raw_matches` holds 50–100 real matches (+ 2 TEST_ rows from Sprint 1).
# MAGIC - `raw_match_timeline` holds the same matches' timelines.
# MAGIC - `raw_league_entries` is populated.
# MAGIC - `ingestion_log` shows started/completed events for every runner.
# MAGIC - Step 5 confirmed MERGE inserted 0 rows on the second run.
# MAGIC - `ingestion_dead_letter` has exactly one row (the bad match ID).
# MAGIC
# MAGIC Screenshot the outputs of steps 4, 5, 7, 8 and 9 into
# MAGIC `docs/sprint-2-validation.md`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'raw_matches'            AS table_name, COUNT(*) AS rows FROM lol_analytics.bronze.raw_matches
# MAGIC UNION ALL
# MAGIC SELECT 'raw_match_timeline',    COUNT(*) FROM lol_analytics.bronze.raw_match_timeline
# MAGIC UNION ALL
# MAGIC SELECT 'raw_league_entries',    COUNT(*) FROM lol_analytics.bronze.raw_league_entries
# MAGIC UNION ALL
# MAGIC SELECT 'ingestion_dead_letter', COUNT(*) FROM lol_analytics.bronze.ingestion_dead_letter
# MAGIC UNION ALL
# MAGIC SELECT 'ingestion_log',         COUNT(*) FROM lol_analytics.bronze.ingestion_log
# MAGIC ORDER BY table_name;
