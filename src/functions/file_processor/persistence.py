"""Persistence layer subclass that logs idempotency cache hits.

Powertools does not expose a native cache-hit hook. The cache-hit code path
is the IdempotencyItemAlreadyExistsError raised by save_inprogress when a
record already exists. We intercept that exception here, emit a structured
log line, and re-raise so Powertools handles the cached response normally.

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

logger = Logger(service="instrumented-persistence", child=True)


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
