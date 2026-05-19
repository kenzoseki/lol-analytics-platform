"""Pytest configuration shared by all tests.

`asyncio_mode = "auto"` (set in pyproject.toml) means async tests run
under pytest-asyncio without an explicit `@pytest.mark.asyncio` marker.

This file also provides the session-scoped `spark` fixture used by
tests marked `@pytest.mark.spark`. Spark does not run on every dev
machine — notably a plain Windows box without `winutils.exe` (see
CLAUDE.md, "Desenvolvimento PySpark Local"). When a SparkSession cannot
be created, the fixture skips the test rather than failing it, so the
fast unit suite stays green everywhere and the Spark tests run where
Spark works (Databricks, Linux CI).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Session-scoped local SparkSession with Delta enabled.

    Skips the test if Spark cannot start (e.g. Windows without
    `winutils.exe`, or pyspark/delta-spark not installed). One session
    is reused across all Spark tests to avoid per-test JVM startup.
    """
    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("pyspark/delta-spark not installed (install with --extra spark)")

    builder = (
        SparkSession.builder.appName("lol-analytics-tests")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
    )

    try:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as e:
        pytest.skip(f"SparkSession unavailable in this environment: {e}")

    yield session
    session.stop()
