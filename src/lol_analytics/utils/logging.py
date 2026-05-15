"""Structured JSON logging with structlog.

JSON logs are non-negotiable for any pipeline that runs in Databricks Workflows
or any cloud environment with log aggregation (CloudWatch, Datadog, etc.).
Search by key, not by regex.
"""

from __future__ import annotations

import logging
import sys
from typing import cast

import structlog


def configure_logging(level: str = "INFO") -> None:
    """Configure stdlib logging + structlog to emit JSON.

    Call once at process startup. Idempotent — safe to call again
    in tests or notebook reloads.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a logger bound to a module name."""
    return cast("structlog.stdlib.BoundLogger", structlog.get_logger(name))
