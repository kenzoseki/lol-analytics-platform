"""Command-line interface for the ingestion layer.

Exposed as `lol-ingest` via the project script entry point in pyproject.toml.

The CLI is intentionally small: it carries only `smoke-test`, which
validates the API key + rate limiter + routing against the live Riot
API and needs no Spark. Bronze ingestion itself is a Databricks
workload — it runs as a notebook under `notebooks/bronze/`, not a local
command (see ADR 004).
"""

from __future__ import annotations

import asyncio

import typer

from lol_analytics.ingestion.smoke_test import run_smoke_test

app = typer.Typer(
    help="LoL analytics ingestion CLI.",
    no_args_is_help=True,
    add_completion=False,
)


@app.callback()
def _root() -> None:
    """Force multi-command mode so a single-command app does not collapse.

    Typer auto-flattens a Typer app with exactly one command into a
    single-command CLI, which would make `lol-ingest smoke-test` reject
    `smoke-test` as an unexpected argument. A no-op callback keeps the
    subcommand structure stable.
    """


@app.command("smoke-test")
def smoke_test() -> None:
    """Validate API key, rate limiter, and routing against the live Riot API.

    Pulls top Challenger players on BR1, fetches one player's recent match
    list, then loads one full match payload. Exits non-zero on failure.
    """
    asyncio.run(run_smoke_test())


if __name__ == "__main__":
    app()
