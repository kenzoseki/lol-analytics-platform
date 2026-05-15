# Sprint 1 Validation Evidence

Evidence of Sprint 1 deliverables validated against a real Databricks
workspace, satisfying Definition of Done item 7 (see [CLAUDE.md](../CLAUDE.md)).

---

## Run metadata

| Field | Value |
|---|---|
| Date of run | _yyyy-mm-dd_ |
| Workspace URL | _https://<your-workspace>.cloud.databricks.com_ |
| Workspace user | _you@example.com_ |
| Notebook path | `/Workspace/Users/<you>/lol-analytics-platform/notebooks/setup/01_validate_bronze.py` |
| Compute | Serverless (Free Edition) |
| Databricks Runtime | _e.g. 15.x (serverless)_ |
| Commit at validation | _e.g. 164d075_ |

---

## What was validated

| # | Feature | DDL/Code reference | Notebook step | Status |
|---|---|---|---|---|
| 1 | Unity Catalog three-level namespacing | [01_bronze.sql:33-37](../sql/ddl/01_bronze.sql#L33-L37) | 2, 3 | ☐ |
| 2 | Predictive Optimization (catalog-level) | [CLAUDE.md `Setup Databricks`](../CLAUDE.md) | 4 | ☐ |
| 3 | 5 Bronze tables created | [01_bronze.sql](../sql/ddl/01_bronze.sql) | 5 | ☐ |
| 4 | Generated columns (`ingestion_date`) | [01_bronze.sql:65](../sql/ddl/01_bronze.sql#L65) | 6, 9 | ☐ |
| 5 | Liquid Clustering | [01_bronze.sql:71](../sql/ddl/01_bronze.sql#L71), [ADR 003](adr/003-clustering-strategy.md) | 7 | ☐ |
| 6 | Column Mapping (`mode=name`) | [01_bronze.sql:73](../sql/ddl/01_bronze.sql#L73) | 8 | ☐ |
| 7 | Deletion Vectors | [01_bronze.sql:76](../sql/ddl/01_bronze.sql#L76) | 8, 12 | ☐ |
| 8 | Change Data Feed | [01_bronze.sql:77](../sql/ddl/01_bronze.sql#L77) | 8, 11 | ☐ |
| 9 | `MERGE INTO` idempotency | DDL idempotency intent | 10 | ☐ |
| 10 | All 5 tables alive at end of run | DDL completeness | 13 | ☐ |

Replace ☐ with ✅ or ❌ as you fill in evidence below. If anything fails,
open an ADR documenting the workaround before proceeding to Sprint 2.

---

## Step 4 — Predictive Optimization enabled

> Expected: `DESCRIBE CATALOG EXTENDED lol_analytics` shows
> `Predictive Optimization | ENABLE`.

**Screenshot:**

_paste image of cell 4 output here_

**Status:** ☐

---

## Step 5 — Five Bronze tables created

> Expected: `SHOW TABLES IN lol_analytics.bronze` lists exactly five
> tables — `raw_matches`, `raw_match_timeline`, `raw_league_entries`,
> `ingestion_dead_letter`, `ingestion_log`.

**Screenshot:**

_paste image of cell 5 final SHOW TABLES output here_

**Status:** ☐

---

## Step 7 — Liquid Clustering on `raw_matches`

> Expected: `DESCRIBE DETAIL` shows
> `clusteringColumns = ["ingestion_date", "match_id"]`. The `partitionColumns`
> field should be empty (Liquid Clustering is mutually exclusive with
> partitioning).

**Screenshot:**

_paste image of cell 7 output here_

**Status:** ☐

---

## Step 8 — Delta table properties

> Expected: `SHOW TBLPROPERTIES lol_analytics.bronze.raw_matches` lists
> at least:
> - `delta.columnMapping.mode = name`
> - `delta.enableDeletionVectors = true`
> - `delta.enableChangeDataFeed = true`
> - `delta.minReaderVersion = 2`
> - `delta.minWriterVersion = 5`

**Screenshot:**

_paste image of cell 8 output here_

**Status:** ☐

---

## Step 9 — Generated columns populated automatically

> Expected: after the INSERT, `SELECT match_id, ingestion_timestamp,
> ingestion_date FROM raw_matches WHERE match_id LIKE 'TEST_%'` shows
> the `ingestion_date` column populated with a value derived from
> `ingestion_timestamp`, even though the INSERT did not provide it.

**Screenshot:**

_paste image of cell 9 output here_

**Status:** ☐

---

## Step 10 — MERGE INTO idempotency

> Expected: after re-merging the same two rows plus one new one,
> `SELECT COUNT(*) WHERE match_id LIKE 'TEST_%'` returns **3** (not 5).

**Screenshot:**

_paste image of cell 10 output here_

**Status:** ☐

---

## Step 11 — Change Data Feed

> Expected: `SELECT * FROM table_changes('lol_analytics.bronze.raw_matches', 0)`
> returns rows with `_change_type = 'insert'` for steps 9 and 10's commits.

**Screenshot:**

_paste image of cell 11 output here_

**Status:** ☐

---

## Step 12 — Deletion Vectors

> Expected: `DELETE FROM raw_matches WHERE match_id = 'TEST_BR1_2'`
> succeeds, and `COUNT(*) WHERE match_id LIKE 'TEST_%'` drops to **2**.
> The delete records a deletion vector instead of rewriting the data file.

**Screenshot:**

_paste image of cell 12 output here_

**Status:** ☐

---

## Step 13 — Final inventory

> Expected (with the leave-in-place convention from this notebook):
>
> | table_name | row_count |
> |---|---|
> | `ingestion_dead_letter` | 0 |
> | `ingestion_log` | 0 |
> | `raw_league_entries` | 0 |
> | `raw_match_timeline` | 0 |
> | `raw_matches` | 2 |

**Screenshot:**

_paste image of cell 13 output here_

**Status:** ☐

---

## Conclusion

_Fill in when validation completes._

- All checks pass: ☐
- Any failures: _list here, link to ADRs documenting the workaround_
- Sprint 1 marked `[x] (validated YYYY-MM-DD)` in the CLAUDE.md roadmap: ☐

Once everything above is ✅, the Sprint 1 entry in the
[Sprint Roadmap](../CLAUDE.md) should be updated and a commit
`docs(sprint-1): add Databricks validation evidence` should record this file.
