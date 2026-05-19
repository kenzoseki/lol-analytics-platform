# Sprint 1 Validation Evidence

Evidence that Sprint 1 deliverables were validated against a real
Databricks workspace, satisfying Definition of Done item 7 (see
[CLAUDE.md](../CLAUDE.md)).

The DoD allows evidence as "screenshot **or log**" — this file is the
log form: a textual record of the validation run. The notebook itself
([`notebooks/setup/01_validate_bronze.py`](../setup/01_validate_bronze.py))
is committed and re-runnable, so the validation is reproducible rather
than merely asserted.

---

## Run metadata

| Field | Value |
|---|---|
| Date of run | 2026-05-15 |
| Environment | Databricks Free Edition (serverless compute) |
| Notebook | [`notebooks/setup/01_validate_bronze.py`](../../notebooks/setup/01_validate_bronze.py) |
| DDL under test | [`sql/ddl/01_bronze.sql`](../../sql/ddl/01_bronze.sql) |
| Repo commit at validation | `17fc6d4` |
| Result | All 10 checks passed, no caveats |

---

## Results

Every notebook cell ran top-to-bottom without error. The features below
were each verified by a dedicated step.

| # | Feature | How it was verified | Result |
|---|---|---|---|
| 1 | Unity Catalog three-level namespacing | `CREATE CATALOG lol_analytics` + `CREATE SCHEMA` for bronze/silver/gold succeeded; `SHOW SCHEMAS` listed all three. | ✅ |
| 2 | Predictive Optimization (catalog-level) | `ALTER CATALOG lol_analytics ENABLE PREDICTIVE OPTIMIZATION` accepted; `DESCRIBE CATALOG EXTENDED` showed it enabled. | ✅ |
| 3 | Five Bronze tables created | All five `CREATE TABLE` statements succeeded; `SHOW TABLES IN lol_analytics.bronze` listed `raw_matches`, `raw_match_timeline`, `raw_league_entries`, `ingestion_dead_letter`, `ingestion_log`. | ✅ |
| 4 | Generated columns | `DESCRIBE EXTENDED raw_matches` showed `ingestion_date` as a generated column; the INSERT in step 9 did not provide `ingestion_date` and it was populated automatically from `ingestion_timestamp`. | ✅ |
| 5 | Liquid Clustering | `DESCRIBE DETAIL raw_matches` returned `clusteringColumns` populated with `(ingestion_date, match_id)`; no partition columns present. | ✅ |
| 6 | Column Mapping (`mode = name`) | `SHOW TBLPROPERTIES raw_matches` showed `delta.columnMapping.mode = name`. | ✅ |
| 7 | Deletion Vectors | `SHOW TBLPROPERTIES` showed `delta.enableDeletionVectors = true`; the `DELETE` in step 12 succeeded and the row count dropped accordingly. | ✅ |
| 8 | Change Data Feed | `SHOW TBLPROPERTIES` showed `delta.enableChangeDataFeed = true`; `table_changes(...)` in step 11 returned the insert events with `_change_type`. | ✅ |
| 9 | `MERGE INTO` idempotency | Re-merging the two existing TEST_ rows plus one new row left the table with exactly 3 TEST_ rows — no duplicates. | ✅ |
| 10 | All five tables alive at end of run | Final inventory (step 13): `raw_matches` = 2 TEST_ rows, all other Bronze tables = 0 rows. | ✅ |

---

## Notes

- The notebook left **two `TEST_*` rows** in `bronze.raw_matches` as a
  permanent sanity check. Sprint 2 ingestion writes real rows alongside
  them. Downstream Silver queries must filter with
  `WHERE match_id NOT LIKE 'TEST_%'`.
- Protocol versions `delta.minReaderVersion = 2` / `minWriterVersion = 5`
  (required by column mapping) were accepted by the Free Edition
  workspace.
- No feature required a fallback or an ADR-documented workaround — the
  modern Databricks practices declared in CLAUDE.md all work on Free
  Edition as written.

---

## Conclusion

**Sprint 1 Definition of Done item 7 is satisfied.** The Bronze DDL and
the modern Delta features it depends on are proven to work on the real
target platform.

This also de-risks Sprint 2: the workspace, catalog, schemas, and table
structures are confirmed, so Sprint 2 validation only needs to exercise
the ingestion *code path* (real Riot matches through `MERGE INTO`), not
the infrastructure.
