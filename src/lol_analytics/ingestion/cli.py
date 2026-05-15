"""Command-line interface for the ingestion layer.

Exposed as `lol-ingest` via the project script entry point in pyproject.toml.

Sprint 1 ships only the `smoke-test` command. Subsequent sprints will add
`pull-leagues`, `pull-matches`, and `pull-timelines` commands as the
corresponding ingestion jobs land.
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


@app.command("smoke-test")
def smoke_test() -> None:
    """Validate API key, rate limiter, and routing against the live Riot API.

    Pulls top Challenger players on BR1, fetches one player's recent match
    list, then loads one full match payload. Exits non-zero on failure.
    """
    asyncio.run(run_smoke_test())


if __name__ == "__main__":
    app()
