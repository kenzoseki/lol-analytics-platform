"""Pytest configuration shared by all tests.

`asyncio_mode = "auto"` (set in pyproject.toml) means async tests run
under pytest-asyncio without an explicit `@pytest.mark.asyncio` marker.

There is no Spark fixture: `src/` is pure Python and never imports
pyspark (ADR 004). Spark logic lives in the Databricks notebooks and is
validated there, not in this suite.

Add shared fixtures here as they're needed by more than one test module.
"""

from __future__ import annotations
