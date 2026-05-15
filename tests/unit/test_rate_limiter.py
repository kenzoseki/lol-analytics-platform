"""Unit tests for the rate limiter.

We test in simulated time semantics: the limiter uses time.monotonic, so
we drive it through real (very short) sleeps with small windows. This keeps
tests fast and deterministic without monkey-patching time.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from lol_analytics.ingestion.rate_limiter import RateWindow, RiotRateLimiter


class TestRateWindow:
    def test_empty_window_has_no_wait(self) -> None:
        window = RateWindow(max_requests=5, period_seconds=1.0)
        assert window.time_until_slot(now=time.monotonic()) == 0.0

    def test_slot_available_below_limit(self) -> None:
        window = RateWindow(max_requests=5, period_seconds=1.0)
        now = time.monotonic()
        for _ in range(4):
            window.record(now)
        assert window.time_until_slot(now) == 0.0

    def test_full_window_requires_wait(self) -> None:
        window = RateWindow(max_requests=2, period_seconds=10.0)
        t0 = time.monotonic()
        window.record(t0)
        window.record(t0 + 1.0)

        wait = window.time_until_slot(now=t0 + 2.0)
        # Oldest entry is at t0; it leaves the window at t0 + 10
        # We're now at t0 + 2, so we wait 8 seconds
        assert wait == pytest.approx(8.0, abs=0.001)

    def test_old_timestamps_are_evicted(self) -> None:
        window = RateWindow(max_requests=2, period_seconds=1.0)
        t0 = time.monotonic()
        window.record(t0)
        window.record(t0 + 0.1)

        # 2 seconds later, both timestamps should be evicted
        assert window.time_until_slot(now=t0 + 2.0) == 0.0


class TestRiotRateLimiter:
    @pytest.mark.asyncio
    async def test_rejects_no_windows(self) -> None:
        with pytest.raises(ValueError, match="At least one"):
            RiotRateLimiter(windows=[])

    @pytest.mark.asyncio
    async def test_under_limit_does_not_block(self) -> None:
        limiter = RiotRateLimiter(windows=[(10, 1.0)])
        start = time.monotonic()
        for _ in range(5):
            await limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.05  # Should be near-instant

    @pytest.mark.asyncio
    async def test_blocks_when_window_full(self) -> None:
        # 2 requests per 0.2s window
        limiter = RiotRateLimiter(windows=[(2, 0.2)])

        start = time.monotonic()
        await limiter.acquire()
        await limiter.acquire()
        # Third call must wait until first timestamp falls out (~0.2s after start)
        await limiter.acquire()
        elapsed = time.monotonic() - start

        assert elapsed >= 0.18  # Allow tiny scheduling jitter
        assert elapsed < 0.35

    @pytest.mark.asyncio
    async def test_respects_most_constrained_window(self) -> None:
        # 100/sec but only 3 per 0.3s — the second window is binding
        limiter = RiotRateLimiter(windows=[(100, 1.0), (3, 0.3)])

        start = time.monotonic()
        for _ in range(4):
            await limiter.acquire()
        elapsed = time.monotonic() - start

        # The 4th request must wait for the 0.3s window to open up
        assert elapsed >= 0.28

    @pytest.mark.asyncio
    async def test_concurrent_acquires_are_serialized(self) -> None:
        """Two coroutines competing for the same limiter must not exceed it."""
        limiter = RiotRateLimiter(windows=[(2, 0.2)])

        async def acquire_at(delay: float) -> float:
            await asyncio.sleep(delay)
            await limiter.acquire()
            return time.monotonic()

        start = time.monotonic()
        # Fire 4 concurrent acquires; only 2 can land in any 0.2s window
        timestamps = await asyncio.gather(
            acquire_at(0),
            acquire_at(0),
            acquire_at(0),
            acquire_at(0),
        )

        relative = sorted(t - start for t in timestamps)
        # First two: immediate. Third and fourth: must wait ~0.2s.
        assert relative[0] < 0.05
        assert relative[1] < 0.05
        assert relative[2] >= 0.18
