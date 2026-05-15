# LoL Analytics Platform

> End-to-end data engineering pipeline ingesting League of Legends ranked match data from the Riot Games API, transforming it through a medallion architecture on Databricks, and exposing meta-game analytics for champion balance analysis.

[![CI](https://github.com/kenzoseki/lol-analytics-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/kenzoseki/lol-analytics-platform/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

---

## Business Context

League of Legends is played by ~150M monthly active users worldwide. Riot Games balances ~170 champions across 13+ patches per year, and the resulting "meta-game" — which champions are statistically dominant — drives player behavior, content creation, and competitive play.

**The question this pipeline answers:** *Which champions are currently overpowered, and how has the meta evolved across recent patches?*

This kind of analysis powers products like [op.gg](https://op.gg), [u.gg](https://u.gg), and [Mobalytics](https://mobalytics.gg). Building it from raw API data requires solving real data engineering problems: rate-limited ingestion, multi-region routing, slowly-changing champion attributes, time-series patch comparisons, and statistically valid sampling across skill tiers.

## What This Project Demonstrates

| Skill | Where it shows up |
|---|---|
| **API ingestion at scale** | Token-bucket rate limiting, exponential backoff, dead-letter queue for failed requests |
| **Medallion architecture** | Bronze (raw JSON) → Silver (dimensional model) → Gold (business aggregations) |
| **Delta Lake operations** | Idempotent MERGE, partitioning by patch, Liquid Clustering on Gold |
| **Dimensional modeling** | SCD Type 2 on champions (attributes change per patch), star schema on facts |
| **Data quality** | Expectation checks, freshness tracking, lineage metadata |
| **PySpark** | Bronze→Silver transforms, window functions, complex joins |
| **SQL analytics** | 10 production-grade queries with window functions, CTEs, statistical aggregations |
| **Observability** | Structured logging, pipeline metadata table, freshness SLAs |

## Architecture

```
┌─────────────────────┐
│   Riot Games API    │  100 req/2min (dev) → token-bucket limited
│  - match-v5         │
│  - league-v4        │
│  - summoner-v4      │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│                    BRONZE (raw, append-only)                │
│  raw_matches | raw_match_timeline | raw_league_entries      │
│  Partitioned by ingestion_date | nested JSON preserved      │
└──────────┬──────────────────────────────────────────────────┘
           │ PySpark + Delta MERGE
           ▼
┌─────────────────────────────────────────────────────────────┐
│              SILVER (dimensional, conformed)                │
│  dim_champion (SCD2) | dim_patch | dim_summoner             │
│  fact_match_participant | fact_match_event                  │
│  Partitioned by patch_version | quality-gated               │
└──────────┬──────────────────────────────────────────────────┘
           │ Business aggregations
           ▼
┌─────────────────────────────────────────────────────────────┐
│                   GOLD (analytics-ready)                    │
│  agg_champion_patch_elo | agg_champion_synergy              │
│  agg_meta_evolution | dim_pipeline_metadata                 │
│  Liquid Clustered on (patch, region, elo_tier)              │
└──────────┬──────────────────────────────────────────────────┘
           │
           ▼
   Databricks SQL Dashboard + 10 analytical queries
```

For deeper architectural reasoning, see [`docs/adr/`](docs/adr/).

## Scope

**Phase 1 (MVP, current):** BR1 + KR regions, Master+ tier, last 3 patches.
Rationale: enough volume for statistical validity (~50k matches), avoids low-elo noise, fits within development API rate limits.

**Phase 2 (planned):** Multi-region (BR1 + KR + NA1 + EUW1), all tiers with stratified sampling.
The expansion itself becomes a case study in scaling a pipeline — see `docs/adr/004-phase-2-scaleup.md` (TBD).

## Tech Stack

- **Compute:** Databricks Free Edition (single-node)
- **Storage:** Delta Lake on DBFS
- **Transform:** PySpark 3.5+
- **Orchestration:** Databricks Workflows
- **Ingestion:** Python 3.11, `httpx`, `tenacity`
- **Quality:** Custom expectation framework (lightweight alternative to Great Expectations)
- **Dashboard:** Databricks SQL
- **Dependency management:** [uv](https://github.com/astral-sh/uv)
- **CI:** GitHub Actions (ruff + pytest)

## Repository Structure

```
lol-analytics-platform/
├── README.md                       # this file
├── pyproject.toml                  # uv-managed dependencies
├── docs/
│   ├── architecture.md             # system overview
│   ├── data_dictionary.md          # every table, every column
│   └── adr/                        # Architecture Decision Records
│       ├── 001-delta-over-parquet.md
│       ├── 002-scd2-on-dim-champion.md
│       └── 003-partitioning-strategy.md
├── src/
│   └── lol_analytics/
│       ├── ingestion/              # Riot API client + rate limiter
│       ├── bronze/                 # raw landing
│       ├── silver/                 # dimensional transforms
│       ├── gold/                   # aggregations
│       └── utils/                  # logging, config, helpers
├── sql/
│   ├── ddl/                        # CREATE TABLE statements
│   └── analyses/                   # 10 business queries
├── tests/unit/                     # pytest + chispa
├── notebooks/                      # exploratory work
└── .github/workflows/ci.yml        # lint + test pipeline
```

## Quickstart

```bash
# Clone and enter the repo
git clone https://github.com/kenzoseki/lol-analytics-platform.git
cd lol-analytics-platform

# Install uv if you don't have it
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Set your Riot API key
cp .env.example .env
# Edit .env with your RIOT_API_KEY (get one at https://developer.riotgames.com)

# Run a smoke test of the ingestion client
uv run python -m lol_analytics.ingestion.smoke_test
```

## Roadmap

- [x] Sprint 1: Foundation — repo scaffolding, Riot API client, first Bronze table
- [ ] Sprint 2: Robust Bronze — incremental ingestion, timeline events, dead-letter queue
- [ ] Sprint 3: Silver — dimensional model, SCD2, quality checks
- [ ] Sprint 4: Gold + the 10 SQL analyses
- [ ] Sprint 5: Dashboard + scheduled Workflow
- [ ] Sprint 6: Polish — README insights, demo video, CI hardening

## Insights (TBD — populated during Sprint 6)

> Three to five highlighted findings from the analytical queries will go here, with charts. Examples of the format:
>
> - *"Champion X has a 56% winrate in Diamond+ but only 48% in Bronze, suggesting a high mechanical skill ceiling."*
> - *"After the patch 14.3 nerf, Champion Y's pickrate dropped 40% within two weeks while winrate stabilized."*

## Limitations & Honest Caveats

- **Selection bias:** Only matches from sampled players' recent history are ingested. Players who don't play frequently are underrepresented.
- **Region coverage:** Phase 1 is BR1 + KR only. Western metas (NA1, EUW1) may differ.
- **Patch boundaries are fuzzy:** Riot deploys patches at slightly different times per region. Patch attribution uses match `gameVersion`, which is reliable but not millisecond-precise.
- **Champion mastery / skill of player not modeled.** A 60% winrate on a champion played mostly by one-tricks is not the same as a 60% winrate spread across the population.

## License

MIT — see [LICENSE](LICENSE).

## Author

**Christian Kenzo Seki** — Data Engineer / Analytics Engineer
[LinkedIn](https://linkedin.com/in/kenzoseki) · [GitHub](https://github.com/kenzoseki)
