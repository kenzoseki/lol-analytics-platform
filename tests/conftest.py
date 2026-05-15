"""Pytest configuration shared by all tests.

`asyncio_mode = "auto"` (set in pyproject.toml) means async tests run
under pytest-asyncio without an explicit `@pytest.mark.asyncio` marker.

Add shared fixtures here as they're needed by more than one test module.
"""

from __future__ import annotations
