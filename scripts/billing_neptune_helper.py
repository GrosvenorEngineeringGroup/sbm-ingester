"""Shared helper for invoking Neptune Gremlin queries via gemsNeptuneExplorer Lambda.

The gemsNeptuneExplorer Lambda sends raw Gremlin to Neptune's HTTPS endpoint and
returns the full GraphSON v2 response without parsing. This module handles:
1. Lambda invocation via boto3
2. Response structure navigation (data → result → data)
3. GraphSON v2 unwrapping (@type/@value annotations → plain Python values)
"""

from __future__ import annotations

import json
import threading
from typing import Any

import boto3

AWS_PROFILE = "geg"
AWS_REGION = "ap-southeast-2"
NEPTUNE_EXPLORER_FUNCTION = "gemsNeptuneExplorer"

# Thread-local storage for Lambda clients (boto3 clients are not thread-safe)
_thread_local = threading.local()


def get_lambda_client() -> boto3.client:
    """Get or create a thread-local Lambda client."""
    client = getattr(_thread_local, "lambda_client", None)
    if client is None:
        session = boto3.Session(profile_name=AWS_PROFILE)
        client = session.client("lambda", region_name=AWS_REGION)
        _thread_local.lambda_client = client
    return client


def parse_graphson(value: Any) -> Any:
    """Recursively unwrap GraphSON v2 type-annotated values into plain Python values.

    GraphSON v2 wraps values like: {"@type": "g:Int32", "@value": 100}
    Maps use alternating key-value flat arrays: ["k1", v1, "k2", v2]
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, list):
        return [parse_graphson(item) for item in value]

    if not isinstance(value, dict):
        return value

    if "@type" not in value or "@value" not in value:
        return {k: parse_graphson(v) for k, v in value.items()}

    gtype = value["@type"]
    inner = value["@value"]

    if gtype in ("g:Int32", "g:Int64", "g:Float", "g:Double"):
        return inner

    if gtype == "g:T":
        return inner

    if gtype in ("g:List", "g:Set"):
        return [parse_graphson(item) for item in inner]

    if gtype == "g:Map":
        pairs = inner
        result = {}
        for i in range(0, len(pairs), 2):
            key = parse_graphson(pairs[i])
            val = parse_graphson(pairs[i + 1])
            result[str(key)] = val
        return result

    # Default: recurse into @value
    return parse_graphson(inner)


def gremlin_query(query: str) -> Any:
    """Execute a Gremlin query via gemsNeptuneExplorer Lambda.

    Args:
        query: Gremlin query string (e.g. "g.V().limit(5).label().toList()").

    Returns:
        The parsed query result data (GraphSON unwrapped to plain Python values).

    Raises:
        RuntimeError: If the Lambda invocation or Gremlin query fails.
    """
    client = get_lambda_client()
    payload = json.dumps({"gremlin": query}).encode()

    response = client.invoke(
        FunctionName=NEPTUNE_EXPLORER_FUNCTION,
        InvocationType="RequestResponse",
        Payload=payload,
    )

    response_payload = json.loads(response["Payload"].read().decode())

    if "error" in response_payload:
        msg = f"Gremlin query failed: {response_payload['error']}"
        raise RuntimeError(msg)

    # Navigate Neptune response structure:
    # Lambda returns: {"query": "...", "data": {"requestId": "...", "status": {...}, "result": {"data": <GraphSON>}}}
    neptune_response = response_payload.get("data", {})
    graphson_data = neptune_response.get("result", {}).get("data", None)

    if graphson_data is None:
        return neptune_response

    return parse_graphson(graphson_data)
