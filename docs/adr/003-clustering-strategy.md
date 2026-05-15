# ADR 003 — Clustering Strategy with Liquid Clustering

**Status:** Accepted
**Date:** 2026-05-15
**Deciders:** Christian Kenzo Seki
**Related ADRs:** ADR 001 (Delta over Parquet)

## Context

With Delta Lake chosen as the storage format (ADR 001), we need to decide
how to physically organize data within each table. Three options exist:

1. **No organization** — Delta defaults; relies only on min/max statistics
   per file collected at write time. Acceptable for small tables, poor for
   the volumes this pipeline targets.
2. **Hive partitioning + Z-Order** — the legacy approach. `PARTITIONED BY`
   creates directory hierarchies; `OPTIMIZE ... ZORDER BY` clusters data
   within each partition by space-filling curve.
3. **Liquid Clustering** — the modern approach (GA in May 2024).
   `CLUSTER BY` defines clustering keys; Delta self-tunes file layout via
   range-based segmentation; no directory hierarchy.

The decision matters because Bronze in particular is high-volume and
filter-heavy: queries from Silver hit it with `WHERE ingestion_date >= ...`
or `WHERE match_id = ...`, and getting the layout wrong means either slow
queries (too few files pruned) or expensive metadata operations (too many
small files).

### Initial misconception (worth documenting)

During an earlier design conversation, the heuristic "avoid clustering by
high-cardinality columns (e.g., `match_id`)" was applied. This heuristic
is correct for Hive partitioning and Z-Order — high-cardinality there
generates many small partitions and degrades performance.

However, this heuristic **does not apply to Liquid Clustering**.
Verifying against the official Databricks documentation revealed:

> "The following scenarios particularly benefit from clustering:
> **Queries that filter on high cardinality columns.** Tables with heavy
> data skew. Fast growing tables that require maintenance and tuning effort."
>
> — [Use liquid clustering for tables, Databricks docs](https://docs.databricks.com/aws/en/delta/clustering)

And from the GA announcement:

> "Unlike Hive partitioning, Liquid clustering keys can be chosen purely
> based on query access patterns, **with no need to consider cardinality**,
> key order, file size, potential data skew, and how access patterns
> might change in the future. In the example above, we are using
> **timestamp, a high-cardinality column, as our clustering key**."
>
> — [Announcing General Availability of Liquid Clustering, Databricks blog](https://www.databricks.com/blog/announcing-general-availability-liquid-clustering)

The intuition that worked for Z-Order's space-filling curve does not
transfer to Liquid Clustering's range-based segmentation. This ADR
exists in part to prevent the team from re-litigating the same
misconception later.

## Decision

Use **Liquid Clustering** for all three medallion layers. Specifically:

| Table                            | Clustering keys                              | Rationale                                                                 |
| -------------------------------- | -------------------------------------------- | ------------------------------------------------------------------------- |
| `bronze.raw_matches`             | `(ingestion_date, match_id)`                 | Time-range queries from Silver; point lookups by `match_id` during debug. |
| `bronze.raw_match_timeline`      | `(ingestion_date, match_id)`                 | Same access pattern as `raw_matches`.                                     |
| `bronze.raw_league_entries`      | `(ingestion_date, platform)`                 | Per-platform snapshots queried by date range.                             |
| `bronze.ingestion_dead_letter`   | `(failed_at_date)`                           | Operational queries by date; low volume, single key sufficient.           |
| `bronze.ingestion_log`           | `(emitted_at_date, runner_name)`             | Operational queries by date + runner; observability over ingestion runs.  |
| `silver.dim_champion` (SCD2)     | `(champion_id, valid_from)`                  | Lookup by champion + as-of-date semantics.                                |
| `silver.fact_match_participant`  | `(patch_version, region, champion_id)`       | Exactly the Gold aggregation grain.                                       |
| `silver.fact_match_event`        | `(patch_version, region, match_id)`          | Timeline analytics, joins back to participants.                           |
| `gold.agg_champion_patch_elo`    | `(patch_version, region, elo_tier)`          | Dashboard filter dimensions.                                              |
| `gold.agg_champion_synergy`      | `(patch_version, region, champion_id_a)`     | Sinergias filtradas por patch + champion principal.                       |
| `gold.agg_meta_evolution`        | `(region, patch_version)`                    | Cross-patch comparisons within a region.                                  |

### Rules followed when selecting keys

1. **Prioritize query filter frequency over cardinality.** Each key above
   appears in `WHERE`, `JOIN`, or aggregation clauses of expected
   downstream queries.
2. **Cap at 4 keys per table.** Per docs: more keys can degrade single-column
   filter performance on tables under 10 TB.
3. **Avoid correlated keys.** `platform` and `region` are highly correlated
   (`BR1 → americas`, `KR → asia`); we include only one in each table.
4. **High-cardinality keys are explicitly OK.** `match_id` (semi-sequential
   strings like `BR1_2987654321`) is included where point-lookup matters.

### Caveats and known limitations

- **No purely random IDs as clustering keys.** UUIDv4-style IDs (no
  temporal/logical proximity) would cause Liquid Clustering to spread
  updates across many files. The Riot Games match IDs are
  region-prefixed sequential integers, so they avoid this trap. If we
  ever add an external system with random UUIDs, exclude them from
  clustering keys.
- **`CLUSTER BY AUTO` deferred.** Available in Runtime 15.4 LTS+ and lets
  Databricks select keys based on actual query workload. We defer this
  to Sprint 5 or later, when there's enough query history for the
  automatic selection to have signal. For now, explicit keys make the
  design intent reviewable.
- **Statistics requirement.** Clustering keys must have statistics
  collected. Delta collects on the first 32 columns by default; all our
  chosen keys fall well within that range.

## Alternatives Considered

### Alternative A — Hive partitioning + Z-Order

**Pros:**
- Familiar pattern, works on any Spark distribution (not Databricks-locked).
- Directory hierarchy can be useful for legacy tooling that wants to
  read by partition path.

**Cons:**
- Choosing partition columns is a one-way door — wrong choice forces
  full table rewrite to fix.
- `match_id` as a partition key would generate billions of directories
  (one per match). Not viable.
- `ingestion_date` as a partition key would create 1-2 directories per
  day, but every Z-Order on top adds rewrite cost.
- The Databricks blog explicitly cites that customers using this approach
  often need "complex workarounds, such as using generated columns to
  partition by high-cardinality columns."

Rejected as legacy.

### Alternative B — No clustering, rely on file statistics

**Pros:** simplest possible setup.
**Cons:** file ordering becomes random over time as MERGEs and inserts
land; data skipping degrades; queries do progressively more I/O.

Rejected; works only for tables that stay small.

### Alternative C — Mix `CLUSTER BY` on some tables, `PARTITIONED BY` on others

**Cons:** inconsistent operational model, harder to reason about, and
mixing approaches per table requires team-wide convention. Also,
they're mutually exclusive on a per-table basis — you can't mix on the
same table — so no clear win.

Rejected for cognitive overhead.

## Consequences

### Positive

- **No partition planning required.** New tables just declare
  `CLUSTER BY` with the columns we know will be filtered.
- **Clustering keys are mutable.** If query patterns change between
  Phase 1 and Phase 2, we run `ALTER TABLE ... CLUSTER BY (...)` plus
  one `OPTIMIZE FULL` and we're done. No directory restructure.
- **No small-file problem.** Liquid Clustering produces consistent file
  sizes regardless of cardinality; we don't have to tune
  `optimizeWrite` flags per table.
- **Compatible with `CLUSTER BY AUTO` in the future.** When we switch
  to automatic key selection in Sprint 5+, the schema and DDL don't
  change — only the clause does.
- **Predictive Optimization handles maintenance.** Combined with the
  catalog-level `ENABLE PREDICTIVE OPTIMIZATION`, we don't need to
  schedule `OPTIMIZE` jobs ourselves.

### Negative

- **Requires Databricks Runtime 13.3 LTS or later** (Free Edition
  satisfies this).
- **Databricks-specific feature.** Migrating to a vanilla Spark or
  another lakehouse engine (e.g., Iceberg on Trino) would require
  re-evaluating layout — though Iceberg has its own analogous feature.
- **No directory-level pruning** the way Hive partitioning provides.
  Tools that scan by path (rare in modern stacks) would not benefit.

### Validation plan

The clustering choices above are theoretical until validated against
real workloads. Sprint 4 validation will measure:

1. `numFilesPruned` from `EXPLAIN FORMATTED` on each portfolio query.
   Target: at least 70% pruned for filter-heavy queries.
2. Average file size per table via `DESCRIBE DETAIL`. Target: 64MB–1GB.
3. Query latency before/after a manual `OPTIMIZE FULL`. Should be
   stable (Predictive Optimization keeps tables well-clustered).

If any table shows poor pruning (<50%), revisit keys in a follow-up ADR.

## References

- [Use liquid clustering for tables — Databricks AWS docs](https://docs.databricks.com/aws/en/delta/clustering)
- [Use liquid clustering for tables — Azure Databricks docs](https://learn.microsoft.com/en-us/azure/databricks/delta/clustering)
- [Announcing General Availability of Liquid Clustering — Databricks blog (May 2024)](https://www.databricks.com/blog/announcing-general-availability-liquid-clustering)
- [How does liquid clustering handle high cardinality strings — Databricks Community](https://community.databricks.com/t5/data-engineering/how-does-liquid-clustering-handle-high-cardinality-strings/td-p/114857)
- [When to Use and when Not to Use Liquid Clustering — Databricks Community](https://community.databricks.com/t5/data-engineering/when-to-use-and-when-not-to-use-liquid-clustering/td-p/136190)
