# ADR 004 — Databricks-first: simplify the Bronze ingestion architecture

**Status:** Accepted
**Date:** 2026-05-19
**Deciders:** Christian Kenzo Seki
**Related ADRs:** ADR 001 (Delta over Parquet)

## Context

Sprint 2 delivered the Bronze ingestion code as a layered Python
application: ~2,000 lines across 20 modules, with eight layers of
indirection between the CLI and the Delta tables —

```
CLI → bootstrap (IngestionStack) → runners → RiotApiClient
                                           → BronzeWriter → MERGE
      DeadLetterSink (Protocol) ─┬─ DeltaDeadLetterSink (prod)
                                 └─ InMemoryDeadLetterSink (test)
      IngestionLogSink (Protocol) ─┬─ DeltaIngestionLogSink (prod)
                                   └─ InMemoryIngestionLogSink (test)
```

Two problems surfaced when validating against a real workspace:

1. **The architecture is heavier than the problem.** The job is
   "fetch JSON from an API, hash it, `MERGE INTO` a Delta table".
   Dependency-injected `Protocol`s with dual implementations, a
   `bootstrap` module assembling an `IngestionStack`, and orchestration
   runners are the shape of a long-lived service, not a data pipeline.
   The `Protocol` + `InMemory*Sink` pair existed almost entirely to let
   the code be unit-tested *without Spark* — indirection in service of
   the test harness, not the problem.

2. **The feedback loop fights the platform.** Spark does not run on a
   plain Windows dev box (no `winutils.exe`). So the layered design
   tried to make everything runnable and testable off-Databricks. The
   result: edit → commit → push → pull Git Folder → run notebook → see
   error → repeat. Minutes per cycle, for a workload that ultimately
   only ever runs on Databricks.

CLAUDE.md's own guidance ("prefer simple, boring solutions over clever
ones") was being contradicted by the codebase.

## Decision

Adopt a **Databricks-first** structure. Databricks is the real
execution environment; stop building scaffolding to run Spark elsewhere.

Two clear homes for code:

| Layer | Holds | Runs on |
|---|---|---|
| `src/lol_analytics/` | **Pure, testable logic only** — rate limiter, Riot API client, payload hashing, payload→record transformation | VSCode (fast unit tests) and importable inside Databricks |
| `notebooks/` | **Databricks orchestration** — Bronze ingestion (fetch → build → `MERGE INTO`), with validation cells inline | Databricks |

Hard rule: **`src/` never imports `pyspark`.** Spark lives only in
notebooks. This is what makes the `src/` half genuinely fast to test
and free of the winutils problem.

Concretely, this ADR removes:

- `ingestion/bootstrap.py`, `ingestion/runners.py`,
  `bronze/writer.py`, `bronze/delta_sinks.py` — the orchestration and
  Spark-write layers move into notebooks.
- `bronze/dead_letter.py`, `bronze/ingestion_log.py` — the `Protocol`s,
  the `InMemory*Sink`s, and their dataclasses. Dead-letter and
  ingestion-log events are short-lived (created → written as a Delta
  row → done); they do not circulate through the logic. `spark.create
  DataFrame` accepts `list[dict]` natively, so a frozen dataclass only
  adds an `asdict()` of ceremony. These events become plain `dict`s
  built inline in the notebook.
- The `pull-leagues` / `pull-matches` / `pull-timelines` CLI commands.
  Ingestion runs as a notebook/job, not a local CLI.
- The CI `spark-tests` job — with no local Spark tests there is
  nothing for it to run.

And keeps:

- `ingestion/riot_client.py` — the async client, with the dead-letter
  simplified: instead of an injected `DeadLetterSink`, the client
  accumulates terminal failures in a `list[dict]` the notebook reads
  and writes to Delta.
- `ingestion/rate_limiter.py` — non-trivial concurrency logic, worth
  its tests.
- `bronze/records.py`, `bronze/payload_hash.py` — pure transformation
  with real test value (canonical JSON, hashing, builders). Dataclasses
  stay **here**, where they earn their keep.
- `ingestion/smoke_test.py` and the `smoke-test` CLI command — runs
  without Spark, useful for validating the API key locally.

Test suite shrinks from ~95 to ~30: drop the tests that exercised the
deleted layers, and drop trivial tests (a frozen dataclass is frozen,
a UUID is unique) that test Python rather than our logic.

## Alternatives considered

### Alternative A — keep the layered architecture

**Argument for:** dependency injection, `Protocol`s, and layering read
as "senior" in an interview; the project's audience is DE recruiters.

**Why rejected:** a senior data engineer reading the code sees the
opposite — a corporate pattern applied to a JSON-ingestion script
without asking whether it was needed. What actually signals competence
in a Databricks project is clean notebooks, correct use of Delta
features (MERGE, Liquid Clustering, CDF), well-written SQL, and
documented decisions (ADRs). None of that needs eight layers of Python.
Keeping the layers would also keep the slow feedback loop.

### Alternative B — fully notebook-based (no `src/` package)

**Argument for:** maximum simplicity; everything is a notebook.

**Why rejected:** the rate limiter and the payload-hashing/transform
logic genuinely benefit from fast, isolated unit tests. Inlining them
into notebooks would lose that. A thin `src/` of pure logic is the
right amount of structure — testable in milliseconds, importable by
the notebooks.

### Alternative C — keep `BronzeWriter`, drop only the Protocols

**Why rejected:** `BronzeWriter` is a class wrapping a `MERGE INTO`
SQL string. In a notebook, that MERGE is three readable cells next to
the data it operates on. Wrapping it in a DI'd class adds a file to
navigate and a SparkSession to inject, for no gain once the CLI (its
only non-test caller) is gone.

## Consequences

### Positive

- **~600–800 lines instead of ~2,000.** Less code to read, navigate,
  and maintain.
- **Fast feedback loop.** Pure logic is edited and tested in the IDE
  in milliseconds. Orchestration is iterated cell-by-cell in a
  Databricks notebook — the platform's native loop.
- **Honest architecture.** The structure matches the workload: a data
  pipeline, not a service. Aligns with CLAUDE.md's "simple and boring".
- **`src/` is winutils-free.** No pyspark import means no Windows Spark
  problem, and the fast test suite runs identically everywhere.

### Negative

- **The `MERGE`/idempotency logic is no longer unit-tested in Python.**
  It now lives in notebooks and is validated by running them on
  Databricks against real data. This is acceptable: `MERGE INTO`,
  Liquid Clustering, and CDF only behave identically to Databricks
  Runtime on a real workspace anyway (CLAUDE.md already said local
  Spark is not a faithful substitute). The validation moves from a
  CI job to the Sprint Definition-of-Done notebook run.
- **The Sprint 2 commits (`36d2ea6`, `9fa2384`, `e53ff17`, `9b31775`)
  now describe code that has been removed.** They remain in git
  history; this ADR is the record of why the approach changed. No
  history rewrite.
- **Less "enterprise Python" on display.** Accepted — see Alternative A.

### Validation plan

Unchanged in spirit from CLAUDE.md's Definition of Done item 7: the
Bronze ingestion notebooks are run on Databricks Free Edition against
50–100 real matches, and the evidence (idempotency, row counts,
dead-letter behaviour) is recorded in `docs/sprint-2-validation.md`.

## References

- ADR 001 — Delta over Parquet.
- ADR 003 — Clustering Strategy (Bronze table layout unchanged here).
- CLAUDE.md — "Em Caso de Dúvida": prefer simple, boring solutions.
