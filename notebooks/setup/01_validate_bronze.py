# Databricks notebook source
# MAGIC %md
# MAGIC # Sprint 1 Validation — Bronze layer against Databricks Free Edition
# MAGIC
# MAGIC Closes the **Definition of Done item 7** for Sprint 1 (see [CLAUDE.md](../../CLAUDE.md))
# MAGIC by validating that the Bronze DDL ([sql/ddl/01_bronze.sql](../../sql/ddl/01_bronze.sql))
# MAGIC works end-to-end on a real Databricks workspace with all modern features active:
# MAGIC
# MAGIC | Feature | Validated by step |
# MAGIC |---|---|
# MAGIC | Unity Catalog three-level namespacing | 2, 3, 5 |
# MAGIC | Predictive Optimization (catalog-level) | 4 |
# MAGIC | Generated columns | 6, 9 |
# MAGIC | Liquid Clustering (CLUSTER BY) | 7 |
# MAGIC | Column Mapping (mode = name) | 8 |
# MAGIC | Deletion Vectors | 8, 12 |
# MAGIC | Change Data Feed | 8, 11 |
# MAGIC | MERGE INTO idempotency | 10 |
# MAGIC
# MAGIC **Convention:** every row inserted by this notebook has a `match_id` /
# MAGIC `puuid` / `request_id` prefixed with `TEST_` so it is trivially filterable.
# MAGIC The notebook leaves the tables in place as a permanent sanity check —
# MAGIC Sprint 2 will write real ingestion rows alongside the TEST_ rows.
# MAGIC
# MAGIC **Pre-requisites:**
# MAGIC - Databricks Free Edition workspace.
# MAGIC - User is a metastore admin (otherwise `CREATE CATALOG` will fail).
# MAGIC - This notebook attached to a serverless or single-node compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Confirm workspace context
# MAGIC
# MAGIC Sanity print before any DDL. If `current_catalog()` returns `hive_metastore`,
# MAGIC we're not on Unity Catalog — stop and check the workspace before continuing.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   current_catalog() AS catalog,
# MAGIC   current_database() AS schema,
# MAGIC   current_user() AS user,
# MAGIC   current_timestamp() AS now_utc;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create catalog `lol_analytics`
# MAGIC
# MAGIC Idempotent. If you do not have metastore-admin role, this fails — that
# MAGIC means the project needs to be re-scoped to an existing catalog (open an ADR).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS lol_analytics
# MAGIC COMMENT 'LoL meta-game analytics platform (Phase 1: BR1 + KR Master+).';
# MAGIC
# MAGIC SHOW CATALOGS LIKE 'lol_analytics';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Create the three medallion schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS lol_analytics.bronze
# MAGIC COMMENT 'Raw landing zone for Riot API responses. Append-only with MERGE for idempotency.';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS lol_analytics.silver
# MAGIC COMMENT 'Dimensional model: dim_champion (SCD2), dim_patch, facts.';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS lol_analytics.gold
# MAGIC COMMENT 'Business aggregations and dashboards.';
# MAGIC
# MAGIC SHOW SCHEMAS IN lol_analytics;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Enable Predictive Optimization at catalog level
# MAGIC
# MAGIC Replaces manual `OPTIMIZE` / `VACUUM` scheduling. Requires UC + managed
# MAGIC tables. Validate via `DESCRIBE CATALOG EXTENDED`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER CATALOG lol_analytics ENABLE PREDICTIVE OPTIMIZATION;
# MAGIC DESCRIBE CATALOG EXTENDED lol_analytics;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Create all five Bronze tables
# MAGIC
# MAGIC Copy/paste of `sql/ddl/01_bronze.sql` excluding the catalog+schema
# MAGIC creation (already done above). If you re-run this cell, all `CREATE
# MAGIC TABLE IF NOT EXISTS` are no-ops.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- bronze.raw_matches
# MAGIC CREATE TABLE IF NOT EXISTS lol_analytics.bronze.raw_matches (
# MAGIC     match_id              STRING NOT NULL COMMENT 'Riot match ID, e.g. BR1_2987654321',
# MAGIC     platform              STRING NOT NULL COMMENT 'Platform shard (BR1, KR, ...)',
# MAGIC     region                STRING NOT NULL COMMENT 'Routing super-region (americas, asia, ...)',
# MAGIC     payload               STRING NOT NULL COMMENT 'Full match JSON response, unparsed',
# MAGIC     payload_hash          STRING NOT NULL COMMENT 'SHA-256 of payload for change detection',
# MAGIC     ingestion_timestamp   TIMESTAMP NOT NULL COMMENT 'UTC time the row was written',
# MAGIC     ingestion_date        DATE GENERATED ALWAYS AS (CAST(ingestion_timestamp AS DATE)),
# MAGIC     source_endpoint       STRING NOT NULL COMMENT '/lol/match/v5/matches/{matchId}',
# MAGIC     api_key_hash          STRING COMMENT 'Last 4 chars of API key used (audit only)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (ingestion_date, match_id)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.columnMapping.mode'    = 'name',
# MAGIC     'delta.minReaderVersion'      = '2',
# MAGIC     'delta.minWriterVersion'      = '5',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.enableChangeDataFeed'  = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- bronze.raw_match_timeline
# MAGIC CREATE TABLE IF NOT EXISTS lol_analytics.bronze.raw_match_timeline (
# MAGIC     match_id              STRING NOT NULL,
# MAGIC     platform              STRING NOT NULL,
# MAGIC     region                STRING NOT NULL,
# MAGIC     payload               STRING NOT NULL,
# MAGIC     payload_hash          STRING NOT NULL,
# MAGIC     ingestion_timestamp   TIMESTAMP NOT NULL,
# MAGIC     ingestion_date        DATE GENERATED ALWAYS AS (CAST(ingestion_timestamp AS DATE)),
# MAGIC     source_endpoint       STRING NOT NULL,
# MAGIC     api_key_hash          STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (ingestion_date, match_id)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.columnMapping.mode'    = 'name',
# MAGIC     'delta.minReaderVersion'      = '2',
# MAGIC     'delta.minWriterVersion'      = '5',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.enableChangeDataFeed'  = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- bronze.raw_league_entries
# MAGIC CREATE TABLE IF NOT EXISTS lol_analytics.bronze.raw_league_entries (
# MAGIC     puuid                 STRING NOT NULL,
# MAGIC     summoner_id           STRING,
# MAGIC     platform              STRING NOT NULL,
# MAGIC     queue_type            STRING NOT NULL,
# MAGIC     tier                  STRING NOT NULL,
# MAGIC     rank                  STRING,
# MAGIC     league_points         INT NOT NULL,
# MAGIC     wins                  INT NOT NULL,
# MAGIC     losses                INT NOT NULL,
# MAGIC     payload               STRING NOT NULL,
# MAGIC     ingestion_timestamp   TIMESTAMP NOT NULL,
# MAGIC     ingestion_date        DATE GENERATED ALWAYS AS (CAST(ingestion_timestamp AS DATE)),
# MAGIC     source_endpoint       STRING NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (ingestion_date, tier, platform)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.columnMapping.mode'    = 'name',
# MAGIC     'delta.minReaderVersion'      = '2',
# MAGIC     'delta.minWriterVersion'      = '5',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.enableChangeDataFeed'  = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- bronze.ingestion_dead_letter
# MAGIC CREATE TABLE IF NOT EXISTS lol_analytics.bronze.ingestion_dead_letter (
# MAGIC     request_id            STRING NOT NULL,
# MAGIC     endpoint              STRING NOT NULL,
# MAGIC     url                   STRING NOT NULL,
# MAGIC     http_status           INT,
# MAGIC     error_class           STRING NOT NULL,
# MAGIC     error_message         STRING,
# MAGIC     request_payload       STRING,
# MAGIC     attempt_count         INT NOT NULL,
# MAGIC     failed_at             TIMESTAMP NOT NULL,
# MAGIC     failed_at_date        DATE GENERATED ALWAYS AS (CAST(failed_at AS DATE))
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (failed_at_date, error_class)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.columnMapping.mode'    = 'name',
# MAGIC     'delta.minReaderVersion'      = '2',
# MAGIC     'delta.minWriterVersion'      = '5',
# MAGIC     'delta.enableDeletionVectors' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- bronze.ingestion_log
# MAGIC CREATE TABLE IF NOT EXISTS lol_analytics.bronze.ingestion_log (
# MAGIC     event_id              STRING NOT NULL,
# MAGIC     run_id                STRING NOT NULL,
# MAGIC     runner_name           STRING NOT NULL,
# MAGIC     action                STRING NOT NULL,
# MAGIC     platform              STRING,
# MAGIC     target_table          STRING,
# MAGIC     rows_affected         BIGINT,
# MAGIC     error_class           STRING,
# MAGIC     error_message         STRING,
# MAGIC     duration_ms           BIGINT,
# MAGIC     emitted_at            TIMESTAMP NOT NULL,
# MAGIC     emitted_at_date       DATE GENERATED ALWAYS AS (CAST(emitted_at AS DATE))
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (emitted_at_date, runner_name)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.columnMapping.mode'    = 'name',
# MAGIC     'delta.minReaderVersion'      = '2',
# MAGIC     'delta.minWriterVersion'      = '5',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.enableChangeDataFeed'  = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All five tables should be listed
# MAGIC SHOW TABLES IN lol_analytics.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Confirm schema and generated columns
# MAGIC
# MAGIC `DESCRIBE EXTENDED` should show `ingestion_date` as `GENERATED ALWAYS AS (CAST(ingestion_timestamp AS DATE))`.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED lol_analytics.bronze.raw_matches;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Confirm Liquid Clustering keys
# MAGIC
# MAGIC `DESCRIBE DETAIL` returns a `clusteringColumns` array. Expected for
# MAGIC `raw_matches`: `["ingestion_date", "match_id"]`.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL lol_analytics.bronze.raw_matches;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Confirm Delta table properties
# MAGIC
# MAGIC Expect to see `delta.columnMapping.mode = name`,
# MAGIC `delta.enableDeletionVectors = true`, `delta.enableChangeDataFeed = true`,
# MAGIC `delta.minReaderVersion = 2`, `delta.minWriterVersion = 5`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES lol_analytics.bronze.raw_matches;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Insert two TEST_ rows and verify generated columns populate
# MAGIC
# MAGIC `ingestion_date` is **not** provided in the INSERT — Delta computes it
# MAGIC from `ingestion_timestamp` automatically.
# MAGIC
# MAGIC > **Note:** these rows have `match_id = 'TEST_*'` and a clearly synthetic
# MAGIC > payload. Filter them out in any real Silver query with
# MAGIC > `WHERE match_id NOT LIKE 'TEST_%'`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- fixture: synthetic data — TEST_ rows for validation only
# MAGIC INSERT INTO lol_analytics.bronze.raw_matches
# MAGIC   (match_id, platform, region, payload, payload_hash, ingestion_timestamp, source_endpoint, api_key_hash)
# MAGIC VALUES
# MAGIC   ('TEST_BR1_1', 'BR1', 'americas',
# MAGIC    '{"test": true, "match_id": "TEST_BR1_1"}',
# MAGIC    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
# MAGIC    current_timestamp(),
# MAGIC    '/lol/match/v5/matches/TEST_BR1_1',
# MAGIC    'TEST'),
# MAGIC   ('TEST_KR_1', 'KR', 'asia',
# MAGIC    '{"test": true, "match_id": "TEST_KR_1"}',
# MAGIC    '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824',
# MAGIC    current_timestamp(),
# MAGIC    '/lol/match/v5/matches/TEST_KR_1',
# MAGIC    'TEST');
# MAGIC
# MAGIC SELECT match_id, platform, ingestion_timestamp, ingestion_date
# MAGIC FROM lol_analytics.bronze.raw_matches
# MAGIC WHERE match_id LIKE 'TEST_%'
# MAGIC ORDER BY match_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — MERGE INTO idempotency
# MAGIC
# MAGIC Re-merge the same two rows + one new row. Expectation:
# MAGIC - Two existing rows: unchanged (WHEN MATCHED — do nothing in this test).
# MAGIC - One new row inserted.
# MAGIC - Final count: 3 TEST_ rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- fixture: synthetic data
# MAGIC MERGE INTO lol_analytics.bronze.raw_matches AS target
# MAGIC USING (
# MAGIC   SELECT * FROM VALUES
# MAGIC     ('TEST_BR1_1', 'BR1', 'americas',
# MAGIC      '{"test": true, "match_id": "TEST_BR1_1"}',
# MAGIC      'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
# MAGIC      current_timestamp(), '/lol/match/v5/matches/TEST_BR1_1', 'TEST'),
# MAGIC     ('TEST_KR_1', 'KR', 'asia',
# MAGIC      '{"test": true, "match_id": "TEST_KR_1"}',
# MAGIC      '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824',
# MAGIC      current_timestamp(), '/lol/match/v5/matches/TEST_KR_1', 'TEST'),
# MAGIC     ('TEST_BR1_2', 'BR1', 'americas',
# MAGIC      '{"test": true, "match_id": "TEST_BR1_2"}',
# MAGIC      '486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7',
# MAGIC      current_timestamp(), '/lol/match/v5/matches/TEST_BR1_2', 'TEST')
# MAGIC   AS t(match_id, platform, region, payload, payload_hash,
# MAGIC        ingestion_timestamp, source_endpoint, api_key_hash)
# MAGIC ) AS source
# MAGIC ON  target.match_id = source.match_id
# MAGIC AND target.platform = source.platform
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC SELECT COUNT(*) AS test_row_count
# MAGIC FROM lol_analytics.bronze.raw_matches
# MAGIC WHERE match_id LIKE 'TEST_%';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 — Change Data Feed
# MAGIC
# MAGIC Read all changes since version 0. We should see the `insert` events
# MAGIC from steps 9 and 10 with `_change_type = 'insert'`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   _change_type,
# MAGIC   _commit_version,
# MAGIC   match_id,
# MAGIC   platform
# MAGIC FROM table_changes('lol_analytics.bronze.raw_matches', 0)
# MAGIC WHERE match_id LIKE 'TEST_%'
# MAGIC ORDER BY _commit_version, match_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 — Deletion Vectors in action
# MAGIC
# MAGIC Delete one TEST_ row. With deletion vectors enabled, the underlying
# MAGIC Parquet file is **not rewritten** — the delete is recorded via a
# MAGIC deletion vector file alongside the data.
# MAGIC
# MAGIC We do NOT re-insert. Sprint 2 ingestion will write real rows alongside
# MAGIC the remaining TEST_ rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM lol_analytics.bronze.raw_matches
# MAGIC WHERE match_id = 'TEST_BR1_2';
# MAGIC
# MAGIC SELECT COUNT(*) AS remaining_test_rows
# MAGIC FROM lol_analytics.bronze.raw_matches
# MAGIC WHERE match_id LIKE 'TEST_%';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 — Final inventory
# MAGIC
# MAGIC Confirm all 5 tables exist and report row counts. Sprint 1 validation
# MAGIC is complete when:
# MAGIC
# MAGIC - `raw_matches` has 2 TEST_ rows.
# MAGIC - All other Bronze tables have 0 rows (DDL-only validation).
# MAGIC - Predictive Optimization shows as `ENABLE` in step 4.
# MAGIC - `DESCRIBE DETAIL` (step 7) showed `clusteringColumns` populated.
# MAGIC - `SHOW TBLPROPERTIES` (step 8) showed all five Delta properties.
# MAGIC
# MAGIC Take screenshots of cells 4, 5 (final `SHOW TABLES`), 7, 8, 9, 10, 11,
# MAGIC and 12 and paste into `docs/sprint-1-validation.md`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'raw_matches'            AS table_name, COUNT(*) AS row_count FROM lol_analytics.bronze.raw_matches
# MAGIC UNION ALL
# MAGIC SELECT 'raw_match_timeline',    COUNT(*) FROM lol_analytics.bronze.raw_match_timeline
# MAGIC UNION ALL
# MAGIC SELECT 'raw_league_entries',    COUNT(*) FROM lol_analytics.bronze.raw_league_entries
# MAGIC UNION ALL
# MAGIC SELECT 'ingestion_dead_letter', COUNT(*) FROM lol_analytics.bronze.ingestion_dead_letter
# MAGIC UNION ALL
# MAGIC SELECT 'ingestion_log',         COUNT(*) FROM lol_analytics.bronze.ingestion_log
# MAGIC ORDER BY table_name;
