"""Bronze row records — pure transformation of Riot payloads to typed rows.

This module is the **Spark-free** half of Bronze ingestion. It takes a
raw Riot API response (a `dict` or `list` straight from `RiotApiClient`)
plus ingestion metadata, and produces an immutable dataclass that mirrors
a Bronze table row column-for-column, with `payload_hash` already computed.

The companion module `bronze.writer` is the Spark half: it turns these
records into a Spark DataFrame and `MERGE INTO`s them. Keeping the two
apart means all the field-extraction and hashing logic is unit-testable
without a SparkSession — which matters because Spark does not run cleanly
on every dev machine (see CLAUDE.md, "Desenvolvimento PySpark Local").

Design notes:
- `payload` is stored as the **exact JSON string** the caller serialized,
  not re-serialized here — the hash must match the bytes we persist.
- Records are frozen dataclasses; once built they do not change.
- Field names match the DDL in `sql/ddl/01_bronze.sql` exactly.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from lol_analytics.bronze.payload_hash import sha256_hex


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


def canonical_json(payload: Any) -> str:
    """Serialize a Riot payload to a stable JSON string.

    `sort_keys=True` makes the output deterministic regardless of dict
    insertion order, so the same logical payload always hashes the same.
    `separators` strips incidental whitespace. `ensure_ascii=False` keeps
    non-ASCII summoner names readable rather than `\\uXXXX`-escaped.

    Args:
        payload: Any JSON-serializable object (dict or list from the API).

    Returns:
        A compact, key-sorted JSON string.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True, slots=True)
class BronzeMatchRecord:
    """One row for `bronze.raw_matches` or `bronze.raw_match_timeline`.

    Both tables share the same shape, so one record type serves both —
    the target table is chosen by the writer, not the record.

    Attributes mirror the DDL. `ingestion_date` is intentionally absent:
    it is a Delta generated column, computed from `ingestion_timestamp`
    by the engine on write.
    """

    match_id: str
    platform: str
    region: str
    payload: str
    payload_hash: str
    ingestion_timestamp: datetime
    source_endpoint: str
    api_key_hash: str | None = None


@dataclass(frozen=True, slots=True)
class BronzeLeagueEntryRecord:
    """One row for `bronze.raw_league_entries`."""

    puuid: str
    summoner_id: str | None
    platform: str
    queue_type: str
    tier: str
    rank: str | None
    league_points: int
    wins: int
    losses: int
    payload: str
    ingestion_timestamp: datetime
    source_endpoint: str


def build_match_record(
    *,
    match_id: str,
    platform: str,
    region: str,
    payload: dict[str, Any],
    source_endpoint: str,
    api_key_hash: str | None = None,
    ingestion_timestamp: datetime | None = None,
) -> BronzeMatchRecord:
    """Build a `BronzeMatchRecord` from a raw match (or timeline) payload.

    Args:
        match_id: Riot match ID (e.g. `BR1_2987654321`).
        platform: Platform shard the match belongs to (`BR1`, `KR`).
        region: Routing super-region (`americas`, `asia`).
        payload: The raw JSON object returned by `match-v5`.
        source_endpoint: The Riot endpoint path that produced `payload`.
        api_key_hash: Last 4 chars of the API key, for audit. Optional.
        ingestion_timestamp: Override the write time. Defaults to now (UTC).

    Returns:
        A frozen `BronzeMatchRecord` with `payload` serialized canonically
        and `payload_hash` computed over that exact string.
    """
    payload_str = canonical_json(payload)
    return BronzeMatchRecord(
        match_id=match_id,
        platform=platform,
        region=region,
        payload=payload_str,
        payload_hash=sha256_hex(payload_str),
        ingestion_timestamp=ingestion_timestamp or _now_utc(),
        source_endpoint=source_endpoint,
        api_key_hash=api_key_hash,
    )


def build_league_entry_records(
    *,
    league_payload: dict[str, Any],
    platform: str,
    tier: str,
    source_endpoint: str,
    ingestion_timestamp: datetime | None = None,
) -> list[BronzeLeagueEntryRecord]:
    """Explode a league-v4 response into one record per entry.

    The `challengerleagues` / `grandmasterleagues` / `masterleagues`
    endpoints return a single league object with an `entries` array
    (a Riot quirk — see CLAUDE.md). Each entry becomes one Bronze row.

    Args:
        league_payload: The single league object from league-v4.
        platform: Platform shard the league belongs to.
        tier: `CHALLENGER`, `GRANDMASTER`, or `MASTER`. Passed explicitly
            because the apex-tier endpoints embed it inconsistently.
        source_endpoint: The Riot endpoint path that produced the payload.
        ingestion_timestamp: Override the write time. Defaults to now (UTC).

    Returns:
        A list of `BronzeLeagueEntryRecord`, one per entry. Empty if the
        payload has no `entries`.
    """
    ts = ingestion_timestamp or _now_utc()
    queue_type = league_payload.get("queue", "RANKED_SOLO_5x5")
    entries = league_payload.get("entries", [])

    records: list[BronzeLeagueEntryRecord] = []
    for entry in entries:
        records.append(
            BronzeLeagueEntryRecord(
                puuid=entry["puuid"],
                summoner_id=entry.get("summonerId"),
                platform=platform,
                queue_type=queue_type,
                tier=tier,
                rank=entry.get("rank"),
                league_points=entry.get("leaguePoints", 0),
                wins=entry.get("wins", 0),
                losses=entry.get("losses", 0),
                payload=canonical_json(entry),
                ingestion_timestamp=ts,
                source_endpoint=source_endpoint,
            )
        )
    return records
