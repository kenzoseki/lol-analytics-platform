# Databricks Workspace Setup

One-time runbook to provision the Databricks Free Edition workspace for the
`lol-analytics-platform` project. Re-run any step idempotently if you reset
or recreate the workspace.

---

## Pre-requisites

- A Databricks Free Edition account (sign-up: <https://www.databricks.com/learn/free-edition>).
- Metastore-admin role in the workspace (required for `CREATE CATALOG`).
- Local clone of this repo on a machine with `git`, `uv`, and Python 3.11+.

---

## 1. Connect the repo to the workspace

Databricks → **Workspace** → **Create → Git Folder**.

| Field | Value |
|---|---|
| Git repository URL | `https://github.com/kenzoseki/lol-analytics-platform.git` |
| Git provider | GitHub |
| Branch | `main` |
| Workspace path | `/Workspace/Users/<your-email>/lol-analytics-platform` |

The Git Folder syncs commits between GitHub and the workspace. Pull
manually after `git push` to local; commits made from the workspace are
pushed back via the Git Folder UI.

---

## 2. Run the validation notebook (one-time)

The validation notebook does setup AND validation in one pass:

1. Open [`notebooks/setup/01_validate_bronze.py`](../../notebooks/setup/01_validate_bronze.py) inside the Git Folder.
2. Attach to a serverless compute (Free Edition default).
3. Run the cells top-to-bottom.

What the notebook does:

| Step | Effect |
|---|---|
| 1 | Print workspace context (catalog, schema, user). |
| 2 | `CREATE CATALOG lol_analytics`. |
| 3 | `CREATE SCHEMA` for bronze, silver, gold. |
| 4 | `ALTER CATALOG lol_analytics ENABLE PREDICTIVE OPTIMIZATION`. |
| 5 | `CREATE TABLE` for the five Bronze tables. |
| 6–8 | Verify schema, clustering keys, table properties on `raw_matches`. |
| 9–12 | Insert TEST_ rows, MERGE idempotency, CDF query, deletion vectors. |
| 13 | Final inventory: row counts in all five tables. |

The notebook leaves **two `TEST_*` rows in `bronze.raw_matches`** as a
permanent sanity check. Real Sprint 2 ingestion writes alongside them.
Any downstream Silver query must filter with `WHERE match_id NOT LIKE 'TEST_%'`.

---

## 3. Capture evidence for the Definition of Done

The "Definition of Done" in [CLAUDE.md](../../CLAUDE.md) item 7 requires
evidence that Delta-touching sprints work on real Databricks. After running
the notebook:

1. Screenshot the cell outputs of steps 4, 5 (final `SHOW TABLES`), 7, 8,
   9, 10, 11, 12, 13.
2. Paste them into [`docs/sprint-1-validation.md`](../sprint-1-validation.md)
   under the matching sections.
3. Commit the evidence file via the Git Folder (or locally + push).

---

## 4. Troubleshooting

### `CREATE CATALOG` fails with "USER_NOT_AUTHORIZED"

You are not a metastore admin. Two options:

1. **Recommended:** ask the workspace owner to grant `CREATE CATALOG` on
   the metastore, or to run the catalog creation on your behalf.
2. **Fallback:** scope the project to an existing catalog (e.g. `workspace`
   on Free Edition). This requires re-pointing all `lol_analytics.bronze.*`
   references in DDL and code. Open a new ADR documenting the fallback
   before changing anything.

### `ALTER CATALOG ENABLE PREDICTIVE OPTIMIZATION` fails

Predictive Optimization requires Databricks Runtime 12.2 LTS+ and Unity
Catalog. On Free Edition serverless, this should work. If it does not,
the feature may be region-restricted — manually schedule `OPTIMIZE` /
`VACUUM` jobs instead (Sprint 5 work, document via ADR).

### `CLUSTER BY` syntax error

Liquid Clustering requires Databricks Runtime 13.3 LTS+. Free Edition
serverless is on a newer runtime, so this should work. If it fails on a
specific table, double-check that `PARTITIONED BY` is not also present
in the same CREATE TABLE — they are mutually exclusive.

### `delta.minReaderVersion = 2` is rejected

The workspace may be running a Delta protocol downgrade. Run `DESCRIBE
DETAIL` on any existing table to inspect current versions; raise via
`ALTER TABLE … SET TBLPROPERTIES`.

---

## 5. Sprint 2 prep

After validation passes, Sprint 2 implementation will:

- Add a Python writer (`BronzeWriter`) that consumes the
  primitives in [`src/lol_analytics/bronze/`](../../src/lol_analytics/bronze/)
  (payload_hash, dead_letter, ingestion_log) and writes to the
  validated Bronze tables via `MERGE INTO`.
- Add CLI commands (`pull-matches`, `pull-timelines`, `pull-leagues`)
  that orchestrate ingestion.
- Validate against 50–100 real ranked matches per the Definition of
  Done for Sprint 2.

Nothing in Sprint 2 should require re-running this notebook unless the
DDL changes (in which case re-run with `DROP TABLE` first, or use
`ALTER TABLE` for migration-style updates).
