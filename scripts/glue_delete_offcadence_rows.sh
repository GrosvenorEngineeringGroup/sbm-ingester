#!/usr/bin/env bash
# Generic runner: purge off-cadence Hudi rows for a specified project.
#
# Usage:
#   ./scripts/glue_delete_offcadence_rows.sh <project> <action>
#
# Actions:
#   prepare-sensors  — Re-extract project sensor list from nem12_mappings.json,
#                      upload to S3 at SENSOR_LIST_S3_URI.
#   deploy           — Upload Glue script to S3 + create/update Glue job.
#   backup           — Snapshot .hoodie + every PARTITION_ITS_LIST partition
#                      to s3://<hudi-bucket>/_backup/<table>_<timestamp>/
#   dry-run          — Start Glue job with DRY_RUN=true (preview only).
#   apply            — Start Glue job with DRY_RUN=false (real delete).
#   status           — Last 3 Glue runs.
#   logs             — Tail CloudWatch logs of most recent run.
#   verify           — Athena query: matching-row count must be 0.
#   teardown         — Delete Glue job + remove S3 script.
#
# Example:
#   ./scripts/glue_delete_offcadence_rows.sh racv prepare-sensors
#   ./scripts/glue_delete_offcadence_rows.sh racv deploy
#   ./scripts/glue_delete_offcadence_rows.sh racv backup
#   ./scripts/glue_delete_offcadence_rows.sh racv dry-run
#   ./scripts/glue_delete_offcadence_rows.sh racv apply
#   ./scripts/glue_delete_offcadence_rows.sh racv verify
#   ./scripts/glue_delete_offcadence_rows.sh racv teardown

set -euo pipefail

# ============================================================================
# Argument parsing
# ============================================================================
PROJECT_ARG="${1:-}"
ACTION="${2:-}"

if [[ -z "$PROJECT_ARG" || -z "$ACTION" ]]; then
  echo "Usage: $0 <project> <action>"
  echo "  project = racv | bunnings | ..."
  echo "  action  = prepare-sensors | deploy | backup | dry-run | apply | status | logs | verify | teardown"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/cleanup_configs/${PROJECT_ARG}.conf"
if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "⛔ Config not found: $CONFIG_FILE"
  exit 1
fi
# shellcheck disable=SC1090
source "$CONFIG_FILE"

# ============================================================================
# Constants
# ============================================================================
REGION="ap-southeast-2"
HUDI_DB="default"
HUDI_TABLE="sensordata_default"
HUDI_TABLE_S3="s3://318396632821sydneyhudibucketsrc/sensordata_default"
BACKUP_ROOT="s3://318396632821sydneyhudibucketsrc/_backup"
GLUE_ROLE="arn:aws:iam::318396632821:role/hudiTrial-2-GlueRoleStack-1VGL72HA871CN-myGlueRole-D7MCD81KE1FV"

JOB_NAME="sbm-purge-offcadence-${PROJECT}"
SCRIPT_LOCAL="${SCRIPT_DIR}/glue_delete_offcadence_rows.py"
SCRIPT_S3_BUCKET="aws-glue-assets-318396632821-ap-southeast-2"
SCRIPT_S3="s3://${SCRIPT_S3_BUCKET}/scripts/glue_delete_offcadence_rows.py"

NEM12_MAPPINGS_S3="s3://sbm-file-ingester/nem12_mappings.json"

MAIN_ETL_JOB="DataImportIntoLake"

# ============================================================================
# Helpers
# ============================================================================
ensure_no_main_etl_running() {
  echo "→ Pre-flight: checking $MAIN_ETL_JOB is not currently running ..."
  local count
  count=$(aws glue get-job-runs --job-name "$MAIN_ETL_JOB" --region "$REGION" \
      --no-paginate \
      --query "length(JobRuns[?JobRunState=='RUNNING' || JobRunState=='STARTING' || JobRunState=='STOPPING'])" \
      --output text 2>/dev/null || echo "0")
  count="${count//[[:space:]]/}"
  if [[ "$count" != "0" && "$count" != "None" && -n "$count" ]]; then
    echo "⛔ ABORT: $MAIN_ETL_JOB has $count active run(s). Aborting."
    aws glue get-job-runs --job-name "$MAIN_ETL_JOB" --region "$REGION" \
      --no-paginate \
      --query "JobRuns[?JobRunState=='RUNNING' || JobRunState=='STARTING' || JobRunState=='STOPPING'].{Id:Id,State:JobRunState}" \
      --output table
    exit 1
  fi
  echo "  ✓ No active $MAIN_ETL_JOB run."
}

build_default_args_json() {
  local dry_run="$1"
  local tmp
  tmp=$(mktemp -t glue-args.XXXXXX.json)
  # Glue's getResolvedOptions rejects empty-string values, so we send
  # "_NONE_" sentinel when a bound is unset. The Python script normalises
  # it back to None.
  local start_ts="${START_TS:-_NONE_}"
  local end_ts="${END_TS:-_NONE_}"
  [[ -z "$start_ts" ]] && start_ts="_NONE_"
  [[ -z "$end_ts" ]] && end_ts="_NONE_"
  cat > "$tmp" <<EOF
{
  "--job-language": "python",
  "--job-bookmark-option": "job-bookmark-disable",
  "--enable-metrics": "true",
  "--enable-spark-ui": "true",
  "--enable-continuous-cloudwatch-log": "true",
  "--enable-glue-datacatalog": "true",
  "--enable-auto-scaling": "true",
  "--enable-job-insights": "false",
  "--datalake-formats": "hudi",
  "--TempDir": "s3://${SCRIPT_S3_BUCKET}/temporary/",
  "--spark-event-logs-path": "s3://${SCRIPT_S3_BUCKET}/sparkHistoryLogs/",
  "--additional-python-modules": "boto3",
  "--HUDI_DB_NAME": "${HUDI_DB}",
  "--HUDI_TABLE_NAME": "${HUDI_TABLE}",
  "--SENSOR_LIST_S3_URI": "${SENSOR_LIST_S3_URI}",
  "--LEGIT_MINUTES": "${LEGIT_MINUTES}",
  "--PARTITION_ITS_LIST": "${PARTITION_ITS_LIST}",
  "--START_TS": "${start_ts}",
  "--END_TS": "${end_ts}",
  "--DRY_RUN": "${dry_run}"
}
EOF
  echo "$tmp"
}

# ============================================================================
# Actions
# ============================================================================
case "$ACTION" in

prepare-sensors)
  echo "→ Extracting RACV-style sensor list from $NEM12_MAPPINGS_S3 ..."
  TMP_MAP=$(mktemp -t mappings.XXXXXX.json)
  TMP_LIST=$(mktemp -t sensors.XXXXXX.txt)
  trap 'rm -f "$TMP_MAP" "$TMP_LIST"' EXIT

  aws s3 cp "$NEM12_MAPPINGS_S3" "$TMP_MAP" --region "$REGION" --quiet
  jq -r "$SENSOR_MAPPING_FILTER" "$TMP_MAP" > "$TMP_LIST"

  COUNT=$(wc -l < "$TMP_LIST" | tr -d ' ')
  echo "  Found ${COUNT} sensors for project '$PROJECT'."
  echo "  First 3:"
  head -3 "$TMP_LIST" | sed 's/^/    /'
  echo "  Last 3:"
  tail -3 "$TMP_LIST" | sed 's/^/    /'
  echo ""

  read -r -p "Upload to ${SENSOR_LIST_S3_URI}? (yes/no): " CONFIRM
  if [[ "$CONFIRM" != "yes" ]]; then
    echo "Aborted."
    exit 1
  fi

  aws s3 cp "$TMP_LIST" "$SENSOR_LIST_S3_URI" --region "$REGION" --quiet
  echo "✅ Uploaded ${COUNT} sensors to ${SENSOR_LIST_S3_URI}"
  ;;

deploy)
  echo "→ Uploading Glue script to S3 ..."
  aws s3 cp "$SCRIPT_LOCAL" "$SCRIPT_S3" --region "$REGION" --quiet
  echo "  ✓ uploaded"

  local_args_file=$(build_default_args_json "true")
  trap 'rm -f "$local_args_file"' EXIT

  echo "→ Creating/updating Glue job '$JOB_NAME' ..."
  if aws glue get-job --job-name "$JOB_NAME" --region "$REGION" >/dev/null 2>&1; then
    aws glue update-job --job-name "$JOB_NAME" --region "$REGION" \
      --job-update "{
        \"Role\": \"$GLUE_ROLE\",
        \"Command\": {
          \"Name\": \"glueetl\",
          \"ScriptLocation\": \"$SCRIPT_S3\",
          \"PythonVersion\": \"3\"
        },
        \"GlueVersion\": \"4.0\",
        \"WorkerType\": \"G.2X\",
        \"NumberOfWorkers\": 5,
        \"Timeout\": 120,
        \"MaxRetries\": 0,
        \"ExecutionProperty\": {\"MaxConcurrentRuns\": 1},
        \"DefaultArguments\": $(cat "$local_args_file")
      }" >/dev/null
  else
    aws glue create-job --name "$JOB_NAME" --region "$REGION" \
      --role "$GLUE_ROLE" \
      --command "Name=glueetl,ScriptLocation=$SCRIPT_S3,PythonVersion=3" \
      --glue-version "4.0" \
      --worker-type "G.2X" \
      --number-of-workers 5 \
      --timeout 120 \
      --max-retries 0 \
      --execution-property MaxConcurrentRuns=1 \
      --default-arguments "file://$local_args_file" >/dev/null
  fi
  echo "✅ Deployed. Next: $0 $PROJECT backup, then dry-run."
  ;;

backup)
  ensure_no_main_etl_running
  STAMP=$(date -u +%Y%m%dT%H%M%SZ)
  BACKUP_DIR="${BACKUP_ROOT}/sensordata_default_${PROJECT}_${STAMP}"
  echo "→ Backing up to ${BACKUP_DIR}"
  echo "  Partitions: $PARTITION_ITS_LIST"
  echo "  Plus .hoodie/ timeline"
  echo ""

  echo "[1/$((1 + $(echo "$PARTITION_ITS_LIST" | tr ',' '\n' | wc -l | tr -d ' ')))] Syncing .hoodie/ ..."
  aws s3 sync "${HUDI_TABLE_S3}/.hoodie/" "${BACKUP_DIR}/.hoodie/" \
      --region "$REGION" --only-show-errors

  IFS=',' read -ra PARTS <<< "$PARTITION_ITS_LIST"
  for i in "${!PARTS[@]}"; do
    P="${PARTS[$i]}"
    echo "[$((i + 2))/...] Syncing its=${P}/ ..."
    aws s3 sync "${HUDI_TABLE_S3}/its=${P}/" "${BACKUP_DIR}/its=${P}/" \
        --region "$REGION" --only-show-errors
  done

  MARKER=$(mktemp -t backup-marker.XXXXXX.json)
  cat > "$MARKER" <<EOF
{
  "project": "${PROJECT}",
  "table": "${HUDI_DB}.${HUDI_TABLE}",
  "source_path": "${HUDI_TABLE_S3}",
  "backup_time": "${STAMP}",
  "backed_up": ["/.hoodie/", "/its=${PARTITION_ITS_LIST//,/, /its=}/"],
  "purpose": "Pre-purge snapshot for off-cadence row cleanup",
  "scope": "Sensors listed in ${SENSOR_LIST_S3_URI}, legit minutes [${LEGIT_MINUTES}]"
}
EOF
  aws s3 cp "$MARKER" "${BACKUP_DIR}/BACKUP_METADATA.json" \
      --region "$REGION" --quiet
  rm -f "$MARKER"

  echo "✅ Backup complete: ${BACKUP_DIR}"
  ;;

dry-run)
  ensure_no_main_etl_running
  echo "→ Starting DRY-RUN ..."
  RUN_ID=$(aws glue start-job-run --job-name "$JOB_NAME" --region "$REGION" \
    --arguments '{"--DRY_RUN":"true"}' --query 'JobRunId' --output text)
  echo "JobRunId: $RUN_ID"
  echo "Watch:  $0 $PROJECT status  /  $0 $PROJECT logs"
  ;;

apply)
  ensure_no_main_etl_running
  echo ""
  echo "⚠️  REAL DELETE on ${HUDI_DB}.${HUDI_TABLE}"
  echo "    Project        : ${PROJECT}"
  echo "    Sensor list    : ${SENSOR_LIST_S3_URI}"
  echo "    Legit minutes  : ${LEGIT_MINUTES}"
  echo "    Partitions     : ${PARTITION_ITS_LIST}"
  echo "    Expected total : ${EXPECTED_TOTAL_HINT:-(check dry-run output)}"
  echo ""
  read -r -p "Type 'yes' to proceed: " CONFIRM
  if [[ "$CONFIRM" != "yes" ]]; then
    echo "Aborted."
    exit 1
  fi
  RUN_ID=$(aws glue start-job-run --job-name "$JOB_NAME" --region "$REGION" \
    --arguments '{"--DRY_RUN":"false"}' --query 'JobRunId' --output text)
  echo "JobRunId: $RUN_ID"
  echo "Watch:  $0 $PROJECT status  /  $0 $PROJECT logs"
  ;;

status)
  aws glue get-job-runs --job-name "$JOB_NAME" --region "$REGION" --max-items 3 \
    --query 'JobRuns[*].{Id:Id,State:JobRunState,Started:StartedOn,Duration:ExecutionTime,Error:ErrorMessage}' \
    --output table
  ;;

logs)
  RUN_ID=$(aws glue get-job-runs --job-name "$JOB_NAME" --region "$REGION" --max-items 1 \
    --query 'JobRuns[0].Id' --output text)
  if [[ -z "$RUN_ID" || "$RUN_ID" == "None" ]]; then
    echo "No runs yet."
    exit 1
  fi
  echo "→ Tailing CloudWatch logs for $RUN_ID"
  aws logs tail "/aws-glue/jobs/output" --region "$REGION" --follow \
    --log-stream-name-prefix "$RUN_ID"
  ;;

verify)
  echo "→ Downloading sensor list to build Athena query ..."
  TMP_LIST=$(mktemp -t sensors.XXXXXX.txt)
  trap 'rm -f "$TMP_LIST"' EXIT
  aws s3 cp "$SENSOR_LIST_S3_URI" "$TMP_LIST" --region "$REGION" --quiet
  IN_LIST=$(awk '{printf "%s'\''%s'\''", sep, $0; sep=","}' "$TMP_LIST")
  PARTITION_IN_LIST=$(echo "$PARTITION_ITS_LIST" | awk -F, '{for (i=1;i<=NF;i++) printf "%s'\''%s'\''", (i>1?",":""), $i}')
  SQL="SELECT minute(ts) AS off_minute, COUNT(*) AS rows FROM ${HUDI_TABLE} WHERE sensorid IN (${IN_LIST}) AND its IN (${PARTITION_IN_LIST}) AND minute(ts) NOT IN (${LEGIT_MINUTES}) GROUP BY minute(ts) ORDER BY off_minute"
  echo "→ Running Athena verify query ..."
  QID=$(aws athena start-query-execution --query-string "$SQL" \
      --query-execution-context "{\"Database\":\"${HUDI_DB}\"}" \
      --result-configuration '{"OutputLocation":"s3://sbm-file-ingester/athena-results/"}' \
      --region "$REGION" --query 'QueryExecutionId' --output text)
  until S=$(aws athena get-query-execution --query-execution-id "$QID" --region "$REGION" --query 'QueryExecution.Status.State' --output text) && [[ "$S" == "SUCCEEDED" || "$S" == "FAILED" ]]; do sleep 2; done
  echo "Athena status: $S"
  aws athena get-query-results --query-execution-id "$QID" --region "$REGION" \
      --query 'ResultSet.Rows[*].Data[*].VarCharValue' --output text | column -t -s $'\t'
  echo ""
  echo "(Expected post-purge state: empty result set / 0 rows)"
  ;;

teardown)
  read -r -p "Remove Glue job '$JOB_NAME' + S3 script? (yes/no): " CONFIRM
  if [[ "$CONFIRM" != "yes" ]]; then
    echo "Aborted."
    exit 1
  fi
  aws glue delete-job --job-name "$JOB_NAME" --region "$REGION" || true
  # Note: SCRIPT_S3 is shared across projects — only remove if no other
  # project uses it. We leave it in place by default.
  echo "✅ Removed job ${JOB_NAME}. (S3 script + sensor list + backup preserved.)"
  ;;

*)
  echo "Unknown action: $ACTION"
  exit 1
  ;;
esac
