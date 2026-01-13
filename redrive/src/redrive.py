import sys
import traceback
from typing import Any

import boto3

s3 = boto3.client("s3")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Lambda to re-trigger S3 event notifications by copying files to themselves.

    Event format example:
    {
        "bucket": "my-bucket-name",
        "prefix": "some/path/"
    }
    """

    # Get bucket & prefix from event or environment
    bucket = "sbm-file-ingester"
    prefix = "newTBP/"

    if not bucket:
        raise ValueError("Bucket name must be provided in event['bucket'] or BUCKET env var")

    print(f"Starting re-trigger for bucket={bucket}, prefix={prefix}")

    paginator = s3.get_paginator("list_objects_v2")

    triggered = 0
    errors = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            # Skip directory placeholders
            if key.endswith("/"):
                continue

            try:
                print(f"Copying {key} to itself to trigger event...")

                # Copy to itself, forcing metadata replacement
                s3.copy_object(
                    Bucket=bucket,
                    CopySource={"Bucket": bucket, "Key": key},
                    Key=key,
                    Metadata={},
                    MetadataDirective="REPLACE",
                )

                print(f"Triggered event for {key}")
                triggered += 1

            except Exception as e:
                errors += 1
                print(f"ERROR processing {key}: {e}", file=sys.stderr)
                traceback.print_exc()

    print(f"Finished. Triggered events for {triggered} objects. Errors: {errors}")
    return {"status": "done", "bucket": bucket, "prefix": prefix, "triggered": triggered, "errors": errors}
