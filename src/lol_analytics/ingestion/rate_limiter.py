"""Token-bucket rate limiter for the Riot Games API.

Riot enforces TWO concurrent windows on every endpoint:
  - Per-second:  20 requests / 1 second   (development key)
  - Per-2-min:   100 requests / 120 seconds (development key)

Both must be respected simultaneously. This module implements a multi-window
token bucket and exposes a single `acquire()` call that blocks until
*all* windows have a free slot.

Why not just `time.sleep(0.05)`?
  - That handles the per-second window only.
  - It wastes burst capacity (we could fire 20 in <1s and then wait).
  - It doesn't model the per-2min ceiling at all, so we'd hit 429s.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field


@dataclass
class RateWindow:
    """A single sliding-window rate limit (e.g. 100 requests per 120 seconds)."""

    max_requests: int
    period_seconds: float
    _timestamps: deque[float] = field(default_factory=deque)

    def time_until_slot(self, now: float) -> float:
        """Seconds to wait before a new request fits in this window. 0 if available now."""
        # Drop timestamps that fell out of the window
        cutoff = now - self.period_seconds
        while self._timestamps and self._timestamps[0] <= cutoff:
            self._timestamps.popleft()

        if len(self._timestamps) < self.max_requests:
            return 0.0

        # Oldest timestamp leaves the window at: oldest + period
        oldest = self._timestamps[0]
        return max(0.0, (oldest + self.period_seconds) - now)

    def record(self, now: float) -> None:
        """Record a request that was just made."""
        self._timestamps.append(now)


class RiotRateLimiter:
    """Multi-window async rate limiter.

    Usage:
        limiter = RiotRateLimiter([(20, 1), (100, 120)])
        async with limiter:
            response = await client.get(url)
    """

    def __init__(self, windows: list[tuple[int, float]]):
        """
        Args:
            windows: list of (max_requests, period_seconds) tuples.
        """
        if not windows:
            raise ValueError("At least one rate window is required")
        self.windows = [RateWindow(max_requests=n, period_seconds=p) for n, p in windows]
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Block until a slot is free in every window, then record the request."""
        async with self._lock:
            while True:
                now = time.monotonic()
                wait_times = [w.time_until_slot(now) for w in self.windows]
                worst_wait = max(wait_times)

                if worst_wait <= 0:
                    # Slot available in every window — claim it
                    for w in self.windows:
                        w.record(now)
                    return

                # Sleep just enough to clear the most constrained window,
                # then re-check (another caller may have used the slot meanwhile).
                await asyncio.sleep(worst_wait)

    async def __aenter__(self) -> RiotRateLimiter:
        await self.acquire()
        return self

    async def __aexit__(self, *args: object) -> None:
        # No-op: we don't release on exit, the request already happened.
        return None
