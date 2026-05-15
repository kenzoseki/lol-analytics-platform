# ADR 002 — Slowly Changing Dimension Type 2 on `dim_champion`

**Status:** Proposed (implementation deferred to Sprint 3)
**Date:** 2026-05-15
**Deciders:** Christian Kenzo Seki
**Related ADRs:** ADR 001 (Delta over Parquet), ADR 003 (Clustering Strategy)

## Context

The dimensional model in Silver represents League of Legends champions
as a dimension table `silver.dim_champion`. Each row describes a
champion's attributes: name, primary class (Tank, Fighter, Mage,
Assassin, Marksman, Support, Specialist), secondary class, difficulty
rating, regions of release, lore tags, and similar properties.

The complication is that **these attributes are not stable over time**.
Riot Games rebalances and remixes the champion roster continuously:

- **Reclassification.** A champion can move between classes when Riot
  shifts its kit identity. *Example:* Aatrox has been reclassified
  multiple times between Fighter and Diver across patches.
- **Reworks and mid-scopes.** Major reworks (full kit changes) and
  mid-scope updates (significant ability changes) effectively create
  a new champion under the same `champion_id`. *Example:* Sett's
  mid-scope or Aurelion Sol's full VGU.
- **Attribute drift.** Difficulty ratings, recommended roles, and
  tag lists are updated patch to patch.
- **Releases and removals.** New champions get added; in extreme cases
  champions get temporarily disabled.

This matters for analytics because the project's central business
question — *"Which champions are overpowered, and how has the meta
evolved across patches?"* — is inherently time-aware. Queries like
"average winrate of Tanks in patch 14.5" require the champion's
attributes **as of that patch**, not their current state.

A dimensional model that overwrites attributes on every refresh
makes this impossible. We need a strategy that preserves history.

## Decision

Implement **Slowly Changing Dimension Type 2 (SCD2)** on
`silver.dim_champion`. Each champion change (detected via the Data
Dragon API + match-level `gameVersion`) generates a new row, with
prior rows closed out via `valid_to`.

### Schema (proposed)

```sql
CREATE TABLE silver.dim_champion (
    -- Surrogate key (one per version of a champion)
    champion_sk           BIGINT GENERATED ALWAYS AS IDENTITY,

    -- Natural key (stable across versions)
    champion_id           INT      NOT NULL,    -- Riot's numeric ID
    champion_key          STRING   NOT NULL,    -- Riot's string key (e.g. 'Aatrox')

    -- Slowly changing attributes
    champion_name         STRING   NOT NULL,
    title                 STRING,
    primary_class         STRING   NOT NULL,    -- Tank, Fighter, Mage, ...
    secondary_class       STRING,
    difficulty            INT,                  -- 1..10 from Riot's metadata
    tags                  ARRAY<STRING>,
    release_patch         STRING,               -- patch of original release

    -- SCD2 metadata
    valid_from            TIMESTAMP NOT NULL,   -- when this version became active
    valid_to              TIMESTAMP,            -- NULL = currently active
    is_current            BOOLEAN  NOT NULL,    -- redundant but enables fast filter
    source_patch          STRING   NOT NULL,    -- patch that introduced this version
    record_hash           STRING   NOT NULL     -- hash of attributes for change detection
)
USING DELTA
CLUSTER BY (champion_id, valid_from)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.enableDeletionVectors' = 'true',
    'delta.columnMapping.mode' = 'name'
);
```

### Loading pattern

Use Delta's `MERGE INTO` to handle the three SCD2 cases atomically:

```sql
-- Pseudocode pattern; real implementation in src/lol_analytics/silver/dim_champion.py
MERGE INTO silver.dim_champion AS target
USING (
    -- Latest snapshot from Data Dragon, hashed for change detection
    SELECT
        champion_id,
        champion_key,
        ...attributes...,
        sha2(concat_ws('|', ...attributes...), 256) AS record_hash,
        current_timestamp() AS effective_ts,
        :current_patch AS source_patch
    FROM silver.staging_champion_snapshot
) AS source
ON target.champion_id = source.champion_id
   AND target.is_current = true

-- Case 1: attributes unchanged → no-op
WHEN MATCHED AND target.record_hash = source.record_hash
    THEN UPDATE SET valid_to = valid_to  -- no-op write to keep audit

-- Case 2: attributes changed → close out current row
WHEN MATCHED AND target.record_hash != source.record_hash
    THEN UPDATE SET
        valid_to = source.effective_ts,
        is_current = false;

-- Case 3 (separate INSERT, since MERGE can't INSERT + UPDATE in one match):
-- new rows for both new champions AND new versions of existing champions
INSERT INTO silver.dim_champion (...)
SELECT ... FROM staging WHERE no current row OR row was just closed;
```

The two-step pattern (MERGE for close-out, INSERT for new version)
is the canonical SCD2 approach on Delta. The alternative (MERGE with
`WHEN NOT MATCHED THEN INSERT`) does not handle the "close old +
insert new for the same natural key" case in a single statement.

### Resolving FK from facts

When `fact_match_participant` ingests a row for `champion_id = 266`
at `game_creation = 2026-04-15`, the join to `dim_champion` is:

```sql
SELECT f.*, c.primary_class, c.difficulty
FROM silver.fact_match_participant f
JOIN silver.dim_champion c
  ON f.champion_id = c.champion_id
 AND f.game_creation BETWEEN c.valid_from AND COALESCE(c.valid_to, '9999-12-31')
```

The `BETWEEN ... COALESCE` is the standard SCD2 "as-of join" pattern.

## Alternatives Considered

### Alternative A — SCD Type 1 (overwrite, no history)

**How it works:** the latest snapshot replaces the previous one. Only
the current state of each champion exists.

**Pros:** simplest possible model; one row per champion; no temporal
joins; no `valid_from`/`valid_to` columns.

**Cons:**
- **Breaks the project's primary business question.** "Average winrate
  of Tanks in patch 14.5" cannot be answered if Aatrox was a Fighter
  in 14.5 but is currently classified as a Tank — SCD1 would mis-attribute
  Aatrox's old games to Tank stats.
- **Reworks become invisible.** "Sett pre-rework vs post-rework
  winrate" requires distinguishing the two versions of the same
  `champion_id`. SCD1 cannot.
- **Audit trail is gone.** No way to answer "when did Riot change
  Aatrox's classification?"

Rejected. The cost of preserving history (one extra row per change)
is dwarfed by the value of being able to answer time-aware questions
correctly. **A portfolio project whose central question is
patch-over-patch evolution cannot afford SCD1.**

### Alternative B — SCD Type 2 (chosen)

Covered above.

### Alternative C — SCD Type 4 (separate history table)

**How it works:** `dim_champion` always holds the current snapshot
(SCD1 style); a parallel `dim_champion_history` table records all
prior versions with `valid_from`/`valid_to`.

**Pros:**
- Queries that only need current state stay simple (single join).
- Historical queries still possible.

**Cons:**
- **Two tables to maintain in sync.** Every champion change writes
  to both. More code, more places for bugs.
- **No benefit on Delta.** The main historical argument for SCD4 was
  performance: on row-store databases, joining a wide history table on
  every analytic query is slow. On Delta with Liquid Clustering by
  `(champion_id, valid_from)`, the as-of join is already fast — and
  `is_current = true` lets us filter to current state with a single
  predicate.
- **No mature tooling support.** SCD2 is the dominant pattern; BI
  tools, dbt-style frameworks, and Delta documentation assume SCD2.

Rejected as complexity without payoff in our stack.

### Alternative D — Snapshot per patch (no SCD pattern)

**How it works:** every patch, dump a full copy of `dim_champion`
tagged with that patch version. The table has `(champion_id,
patch_version)` as the natural key. ~170 champions × 13 patches per
year ≈ 2,200 rows per year.

**Pros:**
- Simpler than SCD2 — no MERGE logic, no `valid_to` to maintain.
- Joins from facts are trivial: `JOIN dim_champion ON match.patch = dim.patch AND match.champion_id = dim.champion_id`.
- Naturally handles "no champion change between patches" — same row, different `patch_version`.

**Cons:**
- **Storage waste.** Most champions don't change between patches.
  Snapshotting copies unchanged data ~170× per patch.
- **No "when did this change?" semantics.** With a snapshot, you know
  the state at patch N and patch N+1; you don't know whether the
  change happened on day 1 or day 14 of the patch cycle. SCD2's
  `valid_from` timestamp captures that.
- **Misses sub-patch changes.** Hotfixes within a patch are invisible
  in a patch-grained snapshot. SCD2 captures them via timestamps.

Rejected as the "fast and dirty" alternative. Justified only if SCD2's
MERGE complexity were prohibitive — which it isn't, given Delta's
first-class MERGE support.

## Consequences

### Positive

- **The project's central question becomes answerable correctly.**
  Patch-over-patch meta evolution queries get the right champion
  attributes at the right time.
- **Audit trail by construction.** "When did Riot reclassify
  Aatrox?" is a single query: `SELECT * FROM dim_champion WHERE
  champion_key = 'Aatrox' ORDER BY valid_from`.
- **Reworks become first-class.** Pre-rework and post-rework analyses
  are distinct rows; the join to facts naturally picks the right one
  based on game date.
- **Portfolio value.** SCD2 in Delta with MERGE is exactly the kind of
  dimensional modeling work that mid-senior DE interviews dig into.
  Documenting the decision makes the choice defensible in conversation.
- **Liquid Clustering on `(champion_id, valid_from)`** (see ADR 003)
  makes the as-of join performant even on large fact tables.

### Negative

- **Implementation complexity.** SCD2 with MERGE requires careful
  handling of the close-out + insert pattern. Unit tests must cover
  unchanged-attributes (no-op), changed-attributes (close-out + insert),
  brand-new-champion (insert only), and idempotency (re-running the
  load produces no duplicates).
- **As-of joins everywhere.** Every fact-to-dim join must use the
  `BETWEEN valid_from AND COALESCE(valid_to, ...)` pattern. Easy to
  forget; will codify in a SQL view or PySpark helper to enforce.
- **Storage growth.** Modest — ~170 champions × maybe 5-10 changes per
  champion per year = ~1,000-2,000 new rows per year. Trivial in
  absolute terms; mentioning for completeness.
- **Change detection requires hashing.** We use `sha2(concat_ws(...))`
  over the SCD2-tracked columns. If a column is added to the dimension
  later, the hash function must be updated, or backfill becomes
  ambiguous.

### Validation plan

Validation in Sprint 3 will verify:

1. **Idempotency:** running the dim_champion load twice with the same
   Data Dragon snapshot produces exactly zero new rows.
2. **Change detection:** modifying one attribute in staging produces
   exactly one closed-out row + one new row, both with the correct
   `valid_from`/`valid_to` timestamps.
3. **As-of join correctness:** a fact row dated April 1 joined against
   a dimension with versions at March 15 and April 15 returns the
   March 15 version.
4. **No orphan facts:** every `(champion_id, game_creation)` from
   `fact_match_participant` resolves to exactly one `dim_champion` row.

## References

- [Slowly Changing Dimensions — Kimball Group](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/)
- [SCD Type 2 with Delta MERGE — Databricks blog](https://www.databricks.com/blog/2022/06/24/simplifying-change-data-capture-with-databricks-delta-live-tables.html)
- [Data Dragon — Riot's champion metadata distribution](https://developer.riotgames.com/docs/lol#data-dragon)
- ADR 001 — Delta over Parquet (the MERGE pattern depends on Delta)
- ADR 003 — Clustering Strategy (`dim_champion` clustered by `(champion_id, valid_from)`)
