# Architecture

High-level system overview for the **lol-analytics-platform** pipeline.
For decisions and tradeoffs, see [`adr/`](adr/). For schema details, see
[`data_dictionary.md`](data_dictionary.md).

---

## System Diagram

```
                ┌─────────────────────────────────┐
                │      Riot Games API             │
                │  - league-v4   (top players)    │
                │  - summoner-v4 (PUUID lookup)   │
                │  - match-v5    (match + timeline)│
                └────────────────┬────────────────┘
                                 │
                  rate-limited (20/s, 100/2m)
                                 │
                                 ▼
        ┌────────────────────────────────────────────────┐
        │  Ingestion (Python 3.11, async httpx)          │
        │  - RiotApiClient (retry, routing)              │
        │  - RiotRateLimiter (multi-window token bucket) │
        │  - Dead-letter queue on terminal failures      │
        └────────────────┬───────────────────────────────┘
                         │ append + MERGE on natural key
                         ▼
        ┌────────────────────────────────────────────────┐
        │  BRONZE  (Delta Lake, partitioned by date)     │
        │  raw_matches | raw_match_timeline              │
        │  raw_league_entries | ingestion_dead_letter    │
        │  Raw JSON preserved verbatim; no parsing here  │
        └────────────────┬───────────────────────────────┘
                         │ PySpark transforms
                         ▼
        ┌────────────────────────────────────────────────┐
        │  SILVER  (dimensional, conformed)              │
        │  dim_champion (SCD2) | dim_patch | dim_summoner│
        │  fact_match_participant | fact_match_event     │
        │  Quality-gated, partitioned by patch_version   │
        └────────────────┬───────────────────────────────┘
                         │ business aggregations
                         ▼
        ┌────────────────────────────────────────────────┐
        │  GOLD  (analytics-ready, Liquid Clustered)     │
        │  agg_champion_patch_elo                        │
        │  agg_champion_synergy                          │
        │  agg_meta_evolution                            │
        └────────────────┬───────────────────────────────┘
                         │
                         ▼
            Databricks SQL Dashboard + 10 analytical queries
```

---

## Layer Responsibilities

### Bronze — raw landing zone

- **Goal:** never lose data. Anything pulled from Riot lands here verbatim
  as a JSON string, plus lineage columns.
- **Idempotency:** `MERGE INTO` on `(match_id, platform)`. Re-running a
  backfill is safe.
- **Partitioning:** `ingestion_date` (daily). Cheap to backfill one day
  at a time; supports time-travel queries ("what did we know on date X?").
- **Schema discipline:** stays minimal — identity, raw payload,
  payload hash, lineage. Field-level parsing is Silver's job, not Bronze's.

### Silver — dimensional model

- **Goal:** a clean, conformed model that downstream queries can join
  without parsing JSON.
- **Conformed dimensions:** `dim_champion` (SCD Type 2 — champion stats
  drift across patches), `dim_patch`, `dim_summoner`.
- **Facts:** `fact_match_participant` (one row per player per match),
  `fact_match_event` (timeline events — kills, objectives, item buys).
- **Quality gates:** every Silver write runs lightweight checks (row
  counts, key uniqueness, null rates on required columns) before
  publishing the new version.

### Gold — business aggregations

- **Goal:** answer the meta-game questions directly with single-table
  scans, no joins required.
- **Grain examples:** `(champion_id, patch_version, region, elo_tier)` for
  win/pick rates; `(champion_a, champion_b, patch_version)` for synergies.
- **Storage:** Liquid Clustered on the columns most frequently filtered
  by the dashboard (`patch_version`, `region`, `elo_tier`).

---

## Cross-Cutting Concerns

### Observability
- All Python code uses `structlog` with a JSON renderer.
- Every ingestion job emits structured events: `ingestion_started`,
  `match_loaded`, `rate_limited`, `match_failed`, `ingestion_completed`.
- Failed requests land in `bronze.ingestion_dead_letter` with the
  full error context, so a recruiter reviewing the project can run a
  single SQL query to see "what's broken."

### Configuration
- All secrets and env-specific values in `.env`, loaded via
  `pydantic-settings`. The `.env` file is gitignored; `.env.example`
  documents the contract.
- API key is never logged in full — only the last 4 chars as a hash.

### Rate limiting
- Riot enforces two concurrent windows (per-second AND per-2-min).
- We model both with a sliding-window token bucket
  (`ingestion/rate_limiter.py`). All HTTP calls go through a single
  limiter instance per process; concurrent calls share a `asyncio.Lock`
  so two coroutines never both think a slot is free.

### Idempotency
- Bronze: `MERGE INTO` on `(match_id, platform)` (or the equivalent
  natural key per table). Same input, same row count.
- Silver: `MERGE INTO` for SCD2 dimensions; idempotent fact loads keyed
  on `(match_id, participant_id)`.
- Gold: full `OVERWRITE` of aggregations is acceptable — they're small
  and re-derived from Silver.

---

## Phase Scope

**Phase 1 (current):** BR1 + KR, Master+ tier, last 3 patches.
- Volume target: ~50k matches.
- Justification: enough for statistical validity at champion × patch ×
  elo grain, fits comfortably within the development API rate limit.

**Phase 2 (planned):** add NA1 + EUW1, all elo tiers via stratified
sampling. Do not start Phase 2 work until Phase 1 ships end-to-end.

---

## Where the code lives

| Concern | Path |
|---|---|
| Riot API client | [`src/lol_analytics/ingestion/riot_client.py`](../src/lol_analytics/ingestion/riot_client.py) |
| Rate limiter | [`src/lol_analytics/ingestion/rate_limiter.py`](../src/lol_analytics/ingestion/rate_limiter.py) |
| Smoke test | [`src/lol_analytics/ingestion/smoke_test.py`](../src/lol_analytics/ingestion/smoke_test.py) |
| CLI entry point | [`src/lol_analytics/ingestion/cli.py`](../src/lol_analytics/ingestion/cli.py) |
| Bronze DDL | [`sql/ddl/01_bronze.sql`](../sql/ddl/01_bronze.sql) |
| Architecture decisions | [`docs/adr/`](adr/) |
