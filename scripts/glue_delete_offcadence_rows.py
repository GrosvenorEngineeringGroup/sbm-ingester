"""Generic one-off Glue job: purge off-cadence rows from Hudi sensordata table.

============================================================================
Pattern this addresses
============================================================================

Some sensors are sourced from a single upstream that lands readings at a
known cadence (e.g. BidEnergy "Export Interval Usage Csv" always emits
30-min readings, i.e. timestamps at :00 and :30 only).

When a SECOND upstream accidentally writes to the same sensorIds at
DIFFERENT minute offsets (e.g. legacy NEM12 export emitting 5-min readings
at :05, :10, :15, :20, :25, :35, :40, :45, :50, :55), Hudi cannot dedupe
because record key is (sensorId, ts) and the timestamps disagree.
Downstream SUM(val) etc. double-count.

This job deletes the off-cadence rows for an EXPLICIT whitelist of sensors
that are known to be cadence-strict — preserving the legitimate on-cadence
readings.

============================================================================
Safety rules — DO NOT bypass
============================================================================

1. Sensor list MUST be an explicit whitelist. Never a wildcard predicate
   like `sensorid LIKE 'p:<project>:%'` — that would catch sensors with
   legitimate non-30-min upstreams. Build the list from
   `nem12_mappings.json` Optima_* entries (or equivalent canonical source)
   and verify pre-incident the sensors were ONLY at the expected cadence.

2. ALWAYS backup `.hoodie/` timeline + every touched partition before
   running with DRY_RUN=false. See companion bash runner.

3. ALWAYS run DRY_RUN=true first and confirm the per-partition / per-minute
   breakdown matches expectations from a pre-flight Athena query.

4. Coordinate with the main ETL — `DataImportIntoLake` must NOT be
   committing concurrently. Hudi 0.12 has no built-in lock provider in
   this deployment.

============================================================================
Parameters (via Glue --arguments)
============================================================================

    --JOB_NAME              (auto-provided)
    --HUDI_DB_NAME          "default"
    --HUDI_TABLE_NAME       "sensordata_default"
    --SENSOR_LIST_S3_URI    e.g. s3://sbm-file-ingester/cleanup/
                                  racv_interval_only_sensors.txt
                            Newline-delimited file with one sensor ID per
                            line. Lines starting with '#' are ignored.
    --LEGIT_MINUTES         Comma-separated, default "0,30".
                            Rows with minute(ts) NOT IN this set are
                            considered off-cadence and will be deleted.
    --PARTITION_ITS_LIST    Comma-separated Hudi partition values to scope
                            the scan, e.g. "2023,2024,2025,2026".
                            REQUIRED — list every year that may contain
                            off-cadence rows for this sensor set.
    --START_TS              Optional ISO ts inclusive lower bound,
                            or empty string for no lower bound.
    --END_TS                Optional ISO ts exclusive upper bound,
                            or empty string for no upper bound.
    --DRY_RUN               "true" (default) or "false".
    --datalake-formats      "hudi"   (Glue 4.0 native Hudi 0.12.1)

============================================================================
Implementation notes
============================================================================

* DataSource API, NOT Spark SQL DELETE — the table uses
  `CustomKeyGenerator + partitionpath.field=its:TIMESTAMP`. Spark SQL
  DELETE wraps the keygen with `SqlKeyGenerator` which fails to propagate
  the timebased-keygen config (HoodieKeyException). DataSource write with
  operation=delete + matching keygen configs works correctly.

* Hive/meta sync OFF — DELETE doesn't change schema or partition layout.
  With sync enabled, Hudi crashed in post-commit with cryptic
  `IllegalArgumentException: None` after the data write committed
  successfully, triggering a rollback. Skipping sync avoids that path.

* Verification — the in-session SELECT COUNT(*) after the Hudi write can
  return STALE results (Spark catalog cache). Always verify post-state
  externally (Athena) before declaring success. This script LOGS the
  predicate so an operator can run the same query independently.
"""

from __future__ import annotations

import sys
from urllib.parse import urlparse

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.session import SparkSession

# ================================
# Argument parsing
# ================================
REQUIRED_ARGS = [
    "JOB_NAME",
    "HUDI_DB_NAME",
    "HUDI_TABLE_NAME",
    "SENSOR_LIST_S3_URI",
    "LEGIT_MINUTES",
    "PARTITION_ITS_LIST",
    "START_TS",
    "END_TS",
    "DRY_RUN",
]

args = getResolvedOptions(sys.argv, REQUIRED_ARGS)

DB = args["HUDI_DB_NAME"].lower()
TABLE = args["HUDI_TABLE_NAME"].lower()
FQTN = f"{DB}.{TABLE}"
SENSOR_LIST_S3_URI = args["SENSOR_LIST_S3_URI"]
LEGIT_MINUTES = [int(m.strip()) for m in args["LEGIT_MINUTES"].split(",") if m.strip()]
PARTITIONS = [p.strip() for p in args["PARTITION_ITS_LIST"].split(",") if p.strip()]


# Sentinel "_NONE_" represents "no bound" — Glue's getResolvedOptions
# rejects empty-string arguments, so the bash runner sends "_NONE_"
# instead. Both are normalised to None here.
def _opt(arg_value: str) -> str:
    v = arg_value.strip()
    return "" if v == "_NONE_" else v


START_TS = _opt(args["START_TS"])
END_TS = _opt(args["END_TS"])
DRY_RUN = args["DRY_RUN"].lower() == "true"


# ================================
# Spark / Hudi setup
# ================================
spark = (
    SparkSession.builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config(
        "spark.sql.extensions",
        "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
    )
    .config("spark.sql.hive.convertMetastoreParquet", "false")
    .config("spark.sql.legacy.pathOptionBehavior.enabled", "true")
    .getOrCreate()
)

sc = spark.sparkContext
glue_context = GlueContext(sc)
job = Job(glue_context)
logger = glue_context.get_logger()
job.init(args["JOB_NAME"], args)


# ================================
# Load sensor whitelist from S3
# ================================
parsed = urlparse(SENSOR_LIST_S3_URI)
if parsed.scheme != "s3":
    raise ValueError(f"SENSOR_LIST_S3_URI must be s3:// URI, got {SENSOR_LIST_S3_URI}")

s3 = boto3.client("s3")
body = s3.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))["Body"].read().decode("utf-8")

sensor_ids = [line.strip() for line in body.splitlines() if line.strip() and not line.strip().startswith("#")]

if not sensor_ids:
    raise ValueError(f"No sensor IDs loaded from {SENSOR_LIST_S3_URI}")

logger.info(f"Loaded {len(sensor_ids)} sensor IDs from {SENSOR_LIST_S3_URI}")


# ================================
# Build predicate
# ================================
sensor_in = ",".join(f"'{sid}'" for sid in sensor_ids)
partition_in = ",".join(f"'{p}'" for p in PARTITIONS)
legit_in = ",".join(str(m) for m in LEGIT_MINUTES)

predicate_parts = [
    f"sensorid IN ({sensor_in})",
    f"its IN ({partition_in})",
    f"minute(ts) NOT IN ({legit_in})",
]
if START_TS:
    predicate_parts.append(f"ts >= TIMESTAMP '{START_TS}'")
if END_TS:
    predicate_parts.append(f"ts <  TIMESTAMP '{END_TS}'")
PREDICATE = " AND ".join(predicate_parts)

logger.info("=" * 70)
logger.info("Hudi DELETE — off-cadence row purge")
logger.info("=" * 70)
logger.info(f"Target table     : {FQTN}")
logger.info(f"Sensor list      : {SENSOR_LIST_S3_URI}")
logger.info(f"Sensors loaded   : {len(sensor_ids)}")
logger.info(f"Partitions       : {PARTITIONS}")
logger.info(f"Legit minutes    : {LEGIT_MINUTES} (other minutes -> DELETE)")
logger.info(f"Time window      : [{START_TS or 'unbounded'}, {END_TS or 'unbounded'})")
logger.info(f"Dry-run          : {DRY_RUN}")
logger.info("=" * 70)


# ================================
# Step 1: Preview
# ================================
logger.info("[Step 1/2] Previewing matching rows ...")
preview_sql = f"""
    SELECT its,
           minute(ts) AS off_minute,
           COUNT(*)                 AS rows,
           COUNT(DISTINCT sensorid) AS sensors,
           MIN(ts)                  AS first_ts,
           MAX(ts)                  AS last_ts
    FROM {FQTN}
    WHERE {PREDICATE}
    GROUP BY its, minute(ts)
    ORDER BY its, off_minute
"""
preview_df = spark.sql(preview_sql)
preview_rows = preview_df.collect()
preview_df.show(60, truncate=False)

total = sum(r["rows"] for r in preview_rows) if preview_rows else 0
logger.info(f"TOTAL rows that match predicate: {total:,d}")


# ================================
# Step 2: Delete via Hudi DataSource API (skipped on dry-run or 0 match)
# ================================
if DRY_RUN:
    logger.info("[Step 2/2] DRY_RUN=true — skipping DELETE.")
elif total == 0:
    logger.info("[Step 2/2] Nothing matches — DELETE skipped.")
else:
    logger.info(f"[Step 2/2] Materializing {total:,d} record keys ...")
    # Reconstruct `its` from `ts` to match the keygen's expected input
    # date format (yyyy-MM-dd H:mm:ss). The Hive view's `its` value is
    # the already-projected partition string ("2025" etc.) which would
    # fail the keygen's date parser.
    delete_keys_df = spark.sql(
        f"""
        SELECT sensorid AS sensorId,
               ts,
               date_format(ts, 'yyyy-MM-dd H:mm:ss') AS its
        FROM {FQTN}
        WHERE {PREDICATE}
        """
    )
    delete_count = delete_keys_df.count()
    logger.info(f"  → materialized {delete_count:,d} record keys")

    if delete_count != total:
        raise RuntimeError(f"Sanity check failed: preview ({total}) != materialised ({delete_count}). Aborting.")

    table_path = "s3://318396632821sydneyhudibucketsrc/sensordata_default"
    hudi_delete_opts = {
        "hoodie.table.name": TABLE,
        # Write semantics
        "hoodie.datasource.write.operation": "delete",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.recordkey.field": "sensorId, ts",
        "hoodie.datasource.write.partitionpath.field": "its:TIMESTAMP",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.CustomKeyGenerator",
        # Timebased keygen — partitionpath.field uses :TIMESTAMP
        "hoodie.deltastreamer.keygen.timebased.timestamp.type": "DATE_STRING",
        "hoodie.deltastreamer.keygen.timebased.input.dateformat": "yyyy-MM-dd H:mm:ss",
        "hoodie.deltastreamer.keygen.timebased.output.dateformat": "yyyy",
        "hoodie.deltastreamer.keygen.timebased.timezone": "UTC",
        # Hive / meta sync OFF (see module docstring "Implementation notes")
        "hoodie.datasource.hive_sync.enable": "false",
        "hoodie.datasource.meta.sync.enable": "false",
    }

    logger.info(f"[Step 2/2] Submitting DELETE to {table_path} ...")
    (delete_keys_df.write.format("hudi").options(**hudi_delete_opts).mode("append").save(table_path))
    logger.info("=" * 70)
    logger.info(f"DELETE commit submitted for ~{total:,d} rows.")
    logger.info("Verify externally (Athena) using the same predicate:")
    logger.info(f"  SELECT COUNT(*) FROM {FQTN} WHERE {PREDICATE}")
    logger.info("Expected post-state count: 0")
    logger.info("=" * 70)

job.commit()
