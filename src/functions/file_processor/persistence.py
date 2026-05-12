"""Persistence layer subclass that logs idempotency cache hits.

Powertools 3.24 does not expose a native cache-hit hook usable for our
needs: ``IdempotencyConfig.response_hook`` only fires when a *completed*
prior record exists, and its ``DataRecord`` does not carry the original
input payload (only its hash). We therefore use a Powertools-internal-error-
as-signal pattern: a cache hit surfaces as an ``IdempotencyItemAlreadyExists``
raised by the base class's ``save_inprogress``. We intercept it, emit a
structured log line carrying the source ``bucket``/``key``, and re-raise so
Powertools handles the cached response normally.

The module-level Logger uses ``service="file-processor"`` with
``child=True`` so it inherits the parent Logger's Powertools JSON formatter
(parent is instantiated in ``pipeline.py``/``app.py``). A mismatched service
name would cause Powertools to fall back to a plain stdlib logger, which
silently drops ``extra=`` kwargs from the JSON output that reaches
CloudWatch.

The CloudWatch alarm for cache-hit rate uses DynamoDB's native
ConditionalCheckFailedRequests metric on the idempotency table — no custom
Lambda metric is emitted.
"""

from __future__ import annotations

from typing import Any

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.idempotency import DynamoDBPersistenceLayer
from aws_lambda_powertools.utilities.idempotency.exceptions import (
    IdempotencyItemAlreadyExistsError,
)

logger = Logger(service="file-processor", child=True)


class InstrumentedDynamoDBPersistenceLayer(DynamoDBPersistenceLayer):
    """DynamoDB persistence layer that logs cache hits."""

    def save_inprogress(
        self,
        data: dict[str, Any],
        remaining_time_in_millis: int | None = None,
    ) -> None:
        try:
            return super().save_inprogress(data, remaining_time_in_millis)
        except IdempotencyItemAlreadyExistsError:
            payload = data if isinstance(data, dict) else {}
            logger.info(
                "idempotent_cache_hit",
                extra={
                    "source_bucket": payload.get("bucket"),
                    "source_key": payload.get("key"),
                },
            )
            raise
