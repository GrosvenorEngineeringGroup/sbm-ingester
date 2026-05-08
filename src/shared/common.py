# S3 and CloudWatch constants for SBM Ingester

PARSE_ERROR_LOG_GROUP = "sbm-ingester-parse-error-log"
RUNTIME_ERROR_LOG_GROUP = "sbm-ingester-runtime-error-log"
ERROR_LOG_GROUP = "sbm-ingester-error-log"
EXECUTION_LOG_GROUP = "sbm-ingester-execution-log"
METRICS_LOG_GROUP = "sbm-ingester-metrics-log"

# The S3 bucket where source files arrive under newTBP/ and are routed by
# disposition to newP/ / newIrrevFiles/ / newParseErr/. The system also
# touches two other buckets (hudibucketsrc, gegoptimareports); use the
# specifically-named constant below for them.
INPUT_BUCKET = "sbm-file-ingester"

# Disposition prefixes inside INPUT_BUCKET.
PARSE_ERR_DIR = "newParseErr/"
UNMAPPED_DIR = "newIrrevFiles/"  # historical name kept on S3; logical name is "unmapped"
PROCESSED_DIR = "newP/"

# Hudi data lake source bucket — CSV objects under HUDI_FINAL_PREFIX are
# consumed by the DataImportIntoLake Glue job into the Hudi table.
HUDI_BUCKET = "hudibucketsrc"
HUDI_FINAL_PREFIX = "sensorDataFiles"
HUDI_STAGING_PREFIX = "sensorDataFilesStaging"
