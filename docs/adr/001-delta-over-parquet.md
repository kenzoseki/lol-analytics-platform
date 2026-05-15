# ADR 001 — Delta Lake over Plain Parquet

**Status:** Accepted
**Date:** 2026-05-01
**Deciders:** Christian Kenzo Seki

## Context

The pipeline ingests data from the Riot API into a lakehouse on Databricks.
We need a storage format for Bronze, Silver, and Gold tables that supports
the access patterns we expect:

- **Idempotent ingestion:** if we re-run a Bronze ingestion job that
  already pulled match `BR1_12345`, we must not produce duplicate rows.
- **Schema evolution:** Riot adds fields to match payloads between patches
  (e.g., new champion stats, new event types). We can't redefine Silver
  every time.
- **Concurrent reads while writing:** the dashboard queries Gold tables on
  a schedule that overlaps with the daily ingestion job.
- **Time-travel debugging:** when an analyst asks "why did Champion X's
  winrate jump on April 12?", being able to query the Gold table *as of*
  April 11 is invaluable.
- **Compaction without locking the table:** Bronze accumulates many small
  files (one per micro-batch); we need to compact them without taking the
  table offline.

## Decision

Use **Delta Lake** for all three medallion layers (Bronze, Silver, Gold).

Specifically:
- Bronze: append-mostly with `MERGE INTO` for idempotency on natural keys
  (`match_id`).
- Silver: `MERGE INTO` for SCD Type 2 dimensions and idempotent fact loads.
- Gold: `OVERWRITE` of full aggregations is acceptable (small tables).

## Alternatives considered

### Plain Parquet on object storage

**Pros:** Zero vendor coupling, simplest possible format, readable by anything.
**Cons:**
- No `MERGE` semantics; idempotency requires manual partition-rewrite gymnastics.
- No ACID guarantees; a failed mid-write leaves a corrupt partition.
- No schema evolution — you can add columns, but consumers see nulls without
  any history of when the column was added.
- No time travel; once you overwrite, the old data is gone.
- Concurrent reader during a writer's `INSERT OVERWRITE` may see partial data.

For a pipeline that runs once a day with append-only data, Parquet would
be defensible. For a pipeline that has to handle re-ingestion of the same
match (because Riot updates a match's record after it's played, e.g., when
a participant is later banned), the lack of `MERGE` is a deal-breaker.

### Apache Iceberg

**Pros:** Open table format with similar features to Delta. Better
multi-engine story (Trino, Snowflake, Flink).
**Cons:**
- On Databricks, Delta has first-class support; Iceberg works but is not
  the default path. Liquid Clustering, predictive optimization, and the
  Photon engine are tuned for Delta.
- The portfolio audience is BR + international companies hiring for
  Databricks roles. Showing Delta is on-stack.

If this were a Snowflake or AWS-native pipeline, Iceberg would be the
better choice. For this project's stack (Databricks + Azure/GCP),
Delta is the conventional and operationally simpler answer.

### Apache Hudi

**Pros:** Strong upsert performance, designed for streaming.
**Cons:**
- Complexity beyond what this project needs.
- Smaller ecosystem on Databricks.
- Operational overhead (compaction services, timeline management) without
  payoff for our batch workload.

## Consequences

**Positive:**
- `MERGE INTO` makes Bronze idempotent without partition-level reasoning.
- Time-travel (`VERSION AS OF`, `TIMESTAMP AS OF`) enables analytical
  forensics and easy rollback of bad ingestion runs.
- Schema evolution via `ALTER TABLE ADD COLUMN` and (where safe)
  automatic schema inference on writes.
- Liquid Clustering becomes available on Gold tables, which we plan to
  use for `(patch_version, region, elo_tier)` access patterns
  (see ADR 003).

**Negative:**
- Delta-specific syntax in DDL; the SQL is not 100% portable to other
  warehouses without translation.
- Slightly higher metadata overhead per table (transaction log).
- Time-travel adds storage cost over time; we will set
  `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration`
  to reasonable values (e.g., 30 days) to bound this.

## References

- [Delta Lake documentation](https://docs.delta.io)
- [Databricks: Delta Lake feature comparison](https://docs.databricks.com/delta/index.html)
- ADR 002 — SCD Type 2 on dim_champion (depends on `MERGE INTO`)
- ADR 003 — Partitioning strategy
