"""SHA-256 hashing for Bronze payloads.

Every Bronze table stores `payload_hash`: the SHA-256 of the JSON
payload string. Two reasons:

1. **Change detection.** Match data is immutable post-game, but the
   Riot API occasionally updates fields (e.g. a participant marked
   `banned` after the fact). When we re-ingest the same match, we can
   tell "same match, different payload" vs "same match, same payload"
   by comparing the hash — without parsing the JSON.

2. **Audit trail.** If a downstream consumer of Silver claims the data
   changed under them, we can prove via Bronze whether the source
   payload actually changed.

The hash is deterministic and content-addressed: identical input bytes
always produce the same hex digest, independent of process, machine,
or Python version.
"""

from __future__ import annotations

import hashlib


def sha256_hex(payload: str) -> str:
    """Return the SHA-256 hex digest of `payload`.

    Args:
        payload: The JSON string (or any text) to hash. Encoded as UTF-8
            before hashing so the output is byte-deterministic across
            platforms.

    Returns:
        Lowercase hex digest, 64 chars.

    Raises:
        TypeError: If `payload` is not a `str`. We deliberately do not
            accept bytes — Bronze payloads are always JSON text, and
            silently accepting bytes would mask a bug upstream.
    """
    if not isinstance(payload, str):
        raise TypeError(f"payload must be str, got {type(payload).__name__}")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
