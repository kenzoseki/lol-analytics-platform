# Sprint 2 Validation Evidence

Evidence that the Bronze ingestion code path works end-to-end against a
real Databricks workspace and real Riot API data, satisfying Definition
of Done item 7 for Sprint 2 (see [CLAUDE.md](../CLAUDE.md)).

Sprint 1 validated the table *infrastructure* (DDL, Liquid Clustering,
CDF, etc.). Sprint 2 validates the *ingestion logic* — runners, MERGE
idempotency, dead-letter queue, and the structured ingestion log.

---

## Run metadata

| Field | Value |
|---|---|
| Date of run | _yyyy-mm-dd_ |
| Environment | Databricks Free Edition (serverless compute) |
| Notebook | [`notebooks/setup/02_validate_ingestion.py`](../../notebooks/setup/02_validate_ingestion.py) |
| Repo commit at validation | _e.g. abc1234_ |
| Riot API key | dev key (expires 24h) — refreshed before run |
| Result | _PASS / PASS-with-caveats / FAIL_ |

---

## Checklist

| # | Check | Notebook step | Status |
|---|---|---|---|
| 1 | Project package installs in the workspace | 1 | ☐ |
| 2 | Ingestion stack builds (client + writer + sinks) | 2 | ☐ |
| 3 | `raw_league_entries` populated for BR1 apex tiers | 3 | ☐ |
| 4 | 50–100 real matches ingested into `raw_matches` | 4 | ☐ |
| 5 | Every match row has a non-null `payload_hash` | 4 | ☐ |
| 6 | MERGE idempotency — second run inserts 0 rows | 5 | ☐ |
| 7 | `raw_match_timeline` populated for the same matches | 6 | ☐ |
| 8 | `ingestion_log` has started/inserted/completed per runner | 7 | ☐ |
| 9 | Dead-letter queue captures one bad match ID | 8 | ☐ |
| 10 | Final inventory consistent | 9 | ☐ |

Replace ☐ with ✅ / ❌. On any ❌, open an ADR documenting the cause and
the workaround before marking Sprint 2 done.

---

## Step 4 — Matches ingested

> Expected: `COUNT(DISTINCT match_id)` between 50 and 100;
> `rows_with_hash` equals `total_rows` (every payload hashed).

_paste cell 4 SQL output here_

**Status:** ☐

---

## Step 5 — MERGE idempotency

> Expected: the second `_pull_matches()` run prints
> `Second run inserted: 0` and the assertion passes.

_paste cell 5 output here_

**Status:** ☐

---

## Step 7 — Ingestion log

> Expected: `started` and `completed` rows for `league_entries_ingestion`,
> `match_ingestion` (twice — the idempotency re-run), and
> `timeline_ingestion`. `inserted` rows carry positive `rows_affected`;
> the second match run shows `skipped_duplicate`.

_paste cell 7 SQL output here_

**Status:** ☐

---

## Step 8 — Dead-letter queue

> Expected: exactly one row, `endpoint = get_match`, `http_status = 404`,
> `error_class = RiotApiError`. The bad-match run inserted 0 rows and did
> not raise — the batch tolerated the failure.

_paste cell 8 SQL output here_

**Status:** ☐

---

## Step 9 — Final inventory

> Expected (TEST_ rows from Sprint 1 still present):
>
> | table_name | rows |
> |---|---|
> | `raw_matches` | 50–100 + 2 TEST_ |
> | `raw_match_timeline` | 50–100 |
> | `raw_league_entries` | hundreds (apex tiers) |
> | `ingestion_dead_letter` | 1 |
> | `ingestion_log` | a dozen-plus events |

_paste cell 9 SQL output here_

**Status:** ☐

---

## Conclusion

_Fill in when validation completes._

- All checks pass: ☐
- Failures / caveats: _list, link ADRs_
- Sprint 2 marked validated in the CLAUDE.md roadmap: ☐
