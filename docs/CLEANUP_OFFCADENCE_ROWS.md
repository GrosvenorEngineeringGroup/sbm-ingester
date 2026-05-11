# SOP: Purging off-cadence rows from the Hudi sensordata table

## Scope

This SOP applies whenever an upstream data source has injected readings at
**off-cadence timestamps** for sensors that are expected to be served by a
single, cadence-strict upstream.

Concretely: BidEnergy "Export Interval Usage Csv" (used by
`optima-interval-exporter`) lands readings every 30 minutes — at `:00` and
`:30` only. If the legacy `optima-nem12-exporter` (5-minute NEM12 cadence)
also writes to the same sensorIds, those extra 5-minute readings appear
at `:05`, `:10`, `:15`, `:20`, `:25`, `:35`, `:40`, `:45`, `:50`, `:55` and
cause double-counting in any `SUM(val)` aggregation.

This SOP **only removes the off-cadence rows**; it does NOT re-ingest data.

---

## Files involved

| Path | Purpose |
|------|---------|
| `scripts/glue_delete_offcadence_rows.py` | Glue PySpark job (project-agnostic) |
| `scripts/glue_delete_offcadence_rows.sh` | Runner / orchestration |
| `scripts/cleanup_configs/<project>.conf` | Per-project config (sensor filter, partitions, cadence) |
| `s3://sbm-file-ingester/cleanup/<project>_interval_only_sensors.txt` | Staged sensor whitelist read by the Glue job |
| `s3://318396632821sydneyhudibucketsrc/_backup/sensordata_default_<project>_<ts>/` | Pre-purge backup |

---

## When to use this SOP

A project qualifies for this cleanup IF AND ONLY IF:

1. **All "interval-only" sensors are explicitly enumerable** — typically
   `Optima_*-E1` / `Optima_*-B1` mappings in
   `s3://sbm-file-ingester/nem12_mappings.json` whose target sensor ID is
   under the project's `p:<project>:` prefix.
2. **Legitimate readings for these sensors are ONLY at the known cadence**
   (e.g. `:00` and `:30` for 30-min). Verify via Athena across multiple
   years (see "Pre-flight checks" below).
3. **No other legitimate upstream** writes to these sensorIds at
   off-cadence timestamps.

⚠️ **Counter-example — Bunnings**: Bunnings has ~916 sensors with
continuous `quality='A'` (NEM12) rows BEFORE the suspect window, indicating
a legitimate persistent NEM12 source. Running this SOP against the
Bunnings Optima list without further investigation would delete legitimate
data. **Investigate before extending this SOP to Bunnings.**

---

## Pre-flight checks (mandatory)

Before running for a new project, validate the scope.

### 1. Identify the sensor whitelist

The `<project>.conf` defines `SENSOR_MAPPING_FILTER`, a jq expression that
extracts sensor IDs from `nem12_mappings.json`. Verify it matches the
intended set:

```bash
aws s3 cp s3://sbm-file-ingester/nem12_mappings.json - --region ap-southeast-2 \
  | jq -r "$SENSOR_MAPPING_FILTER" | wc -l
```

Compare against an external source (e.g. a per-project NMI list) before
trusting the count.

### 2. Confirm the cadence is consistent across the legit upstream

Run this Athena query, replacing `<sensor_in_list>` with a few sample
sensor IDs (one per project — sampling is sufficient at this stage):

```sql
SELECT minute(ts) AS m, COUNT(*) AS rows
FROM sensordata_default
WHERE sensorid IN (<sensor_in_list>)
  AND ts >= TIMESTAMP '<known-clean-window-start>'
  AND ts <  TIMESTAMP '<known-clean-window-end>'
GROUP BY minute(ts)
ORDER BY m;
```

In a known-clean period, only `LEGIT_MINUTES` (e.g. `0, 30`) should appear.
If any other minute has rows, **stop**: legitimate off-cadence data exists
and this SOP is unsafe.

### 3. Count the cleanup scope

```sql
SELECT its, minute(ts) AS m, COUNT(*) AS rows, COUNT(DISTINCT sensorid) AS sensors
FROM sensordata_default
WHERE sensorid IN (<full sensor list>)
  AND minute(ts) NOT IN (0, 30)
GROUP BY its, minute(ts)
ORDER BY its, m;
```

Note the total. The dry-run preview must match this number within ±2%
(slight variance is OK if data is still arriving; large variance = stop).

---

## Execution

```bash
PROJECT=racv   # or bunnings, etc.

# 1. Build / refresh the sensor list and upload to S3.
./scripts/glue_delete_offcadence_rows.sh $PROJECT prepare-sensors

# 2. Deploy / update the Glue job (job name: sbm-purge-offcadence-<project>).
./scripts/glue_delete_offcadence_rows.sh $PROJECT deploy

# 3. Backup .hoodie/ + every affected partition.
./scripts/glue_delete_offcadence_rows.sh $PROJECT backup

# 4. Dry-run. Preview must match pre-flight count.
./scripts/glue_delete_offcadence_rows.sh $PROJECT dry-run
./scripts/glue_delete_offcadence_rows.sh $PROJECT status   # wait for SUCCEEDED
./scripts/glue_delete_offcadence_rows.sh $PROJECT logs     # confirm preview

# 5. If preview matches expectation, apply.
./scripts/glue_delete_offcadence_rows.sh $PROJECT apply
./scripts/glue_delete_offcadence_rows.sh $PROJECT status   # wait for SUCCEEDED

# 6. Verify externally via Athena (DO NOT skip — Spark catalog cache
#    inside the Glue session can return stale results).
./scripts/glue_delete_offcadence_rows.sh $PROJECT verify
# Expected: empty result set.

# 7. (Optional, after a few days of observation) teardown.
./scripts/glue_delete_offcadence_rows.sh $PROJECT teardown
```

---

## Known issues / gotchas

### Glue job marks FAILED even when DELETE succeeded

Symptom: `Error: RuntimeError: Post-DELETE verification failed: N rows
still match predicate`, but Athena says 0 rows match. Cause: the script's
in-session `SELECT COUNT(*)` reads stale Spark catalog metadata from
before the Hudi commit. The Athena verify is authoritative. **The current
script no longer runs the in-session verify** — verification is operator-
driven via the `verify` action.

### Hive sync must be OFF

Earlier attempts with `hive_sync.enable=true` failed in post-commit with
`IllegalArgumentException: None` AFTER the Hudi commit had finalised,
triggering an automatic rollback. The current job uses
`hive_sync.enable=false` + `meta.sync.enable=false`. DELETE does not
change schema or partition layout, so no sync is needed. The next
`DataImportIntoLake` run will refresh the catalog naturally.

### Must use DataSource API, not Spark SQL DELETE

The table uses `CustomKeyGenerator` + `partitionpath.field=its:TIMESTAMP`.
Spark SQL `DELETE FROM` wraps the keygen with `SqlKeyGenerator` which
fails to propagate the timebased-keygen config:
`HoodieKeyException: Unable to find field names for partition path in
proper format`. The job materialises record keys via `SELECT`, then
submits a Hudi write with `operation=delete` and matching keygen configs.

### Reconstructing `its` from `ts`

The Hive-registered `its` column returns the projected partition value
(e.g. `"2026"`), but the keygen's `TimestampBasedAvroKeyGenerator`
expects to re-parse a full `yyyy-MM-dd H:mm:ss` string. The script
re-synthesises `its` via `date_format(ts, 'yyyy-MM-dd H:mm:ss')` for the
delete write.

### Concurrency with `DataImportIntoLake`

Hudi 0.12 has no lock provider configured in this deployment. The bash
runner's `ensure_no_main_etl_running` checks for active runs of
`DataImportIntoLake` before starting `backup`, `dry-run`, or `apply`.
If you bypass the runner, do this check manually.

### COW rewrite is slow

`sensordata_default` is COPY_ON_WRITE. Even deleting ~340k rows takes
~15–25 minutes because every file group containing a matching record is
fully rewritten. Plan accordingly.

---

## Rollback

If a purge needs to be undone:

1. Stop the main ETL: temporarily disable the EventBridge rule for
   `sbm-glue-trigger` (so no new commits during restore).
2. Identify the backup directory:
   `s3://318396632821sydneyhudibucketsrc/_backup/sensordata_default_<project>_<ts>/`
3. Sync `.hoodie/` and affected partitions back to the live table:
   ```bash
   aws s3 sync s3://.../_backup/<dir>/.hoodie/      s3://318396632821sydneyhudibucketsrc/sensordata_default/.hoodie/
   aws s3 sync s3://.../_backup/<dir>/its=2026/     s3://318396632821sydneyhudibucketsrc/sensordata_default/its=2026/
   # (repeat for each affected partition)
   ```
4. Re-enable the EventBridge rule.

Verify via Athena that the pre-purge row counts are restored, then
investigate the cause before re-attempting.

---

## Adding a new project

1. Build a `scripts/cleanup_configs/<project>.conf` analogous to
   `racv.conf`. The `SENSOR_MAPPING_FILTER` is a jq expression that
   selects the project's interval-only sensor IDs from
   `nem12_mappings.json`.
2. Run the "Pre-flight checks" above. **Do not skip.**
3. Follow "Execution".
