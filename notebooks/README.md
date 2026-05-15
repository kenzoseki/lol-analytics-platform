# Notebooks

Exploratory analysis and one-off investigations.

These notebooks are **not** part of the production pipeline. Anything
that becomes load-bearing should be promoted to a tested module under
`src/lol_analytics/` and a SQL file under `sql/`.

## Conventions

- Name notebooks `NN_short_topic.ipynb` (e.g. `01_meta_volatility.ipynb`)
  so they sort chronologically.
- Top cell of every notebook: a markdown block with **purpose**,
  **inputs** (which Bronze/Silver/Gold tables it reads), and
  **conclusion** (what was learned).
- Clear all outputs before committing — diff noise from cell outputs
  drowns out real review signal. (`jupyter nbconvert --clear-output --inplace`)
- If the notebook depends on a dataset that isn't reproducible from the
  pipeline, say so explicitly in the top cell.

This directory is a placeholder until Sprint 4, when the first
exploratory analyses will land alongside the analytical SQL queries.
