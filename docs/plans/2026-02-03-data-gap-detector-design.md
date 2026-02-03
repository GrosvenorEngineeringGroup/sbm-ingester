# Data Gap Detector Design

Date: 2026-02-03

## Overview

Create a Lambda function to detect missing sensor data in the Hudi data lake for bunnings/racv projects.

## Input Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `project` | string | Yes | Project name: `bunnings` or `racv` |
| `start_date` | string | No | Start date (YYYY-MM-DD), defaults to sensor's earliest data date |
| `end_date` | string | No | End date (YYYY-MM-DD), defaults to sensor's latest data date |

## Data Flow

```
nem12_mappings.json
        |
        v
Filter sensors by project (p:{project}:xxx pattern)
        |
        v
AWS SDK for pandas query Hudi table (via Glue Catalog)
        |
        v
Analyze date coverage for each sensor
        |
        v
Generate CSV report -> sbm-ingester/output/
```

## Detection Logic

### Issue Types

| issue_type | Description | Condition |
|------------|-------------|-----------|
| `no_data` | No data at all | sensorId has no records in Hudi table |
| `missing_dates` | Some days missing | Some dates within range have no records |

### Date Range Determination

```
If user specifies start_date and end_date:
    Use user-specified range
Else:
    For each sensor individually:
    - start = MIN(DATE(ts)) for that sensor
    - end = MAX(DATE(ts)) for that sensor
```

### Detection Algorithm

```python
expected_dates = set(date_range(start, end))  # All expected dates
actual_dates = set(sensor_data['date'])        # Dates with data
missing_dates = expected_dates - actual_dates  # Missing dates

if len(actual_dates) == 0:
    issue_type = "no_data"
elif len(missing_dates) > 0:
    issue_type = "missing_dates"
else:
    # Data complete, not included in report
```

## Query Optimization Strategy

### Batch + Concurrent Processing

```
Step 1: Get target sensors list (from nem12_mappings.json)
        |
        v
Step 2: Split into batches (e.g., 50 sensorIds per batch)
        |
        v
Step 3: Concurrent query each batch (ThreadPoolExecutor, max_workers=5)
        Each batch executes aggregate query:
        SELECT sensorId, DATE(ts) as data_date, COUNT(*) as cnt
        FROM table
        WHERE sensorId IN (batch_ids)
        GROUP BY sensorId, DATE(ts)
        |
        v
Step 4: Merge results, analyze missing dates in memory
        |
        v
Step 5: Generate CSV report
```

### Configuration

```python
BATCH_SIZE = 50          # Sensors per batch
MAX_WORKERS = 5          # Concurrent query threads
PARTITION_FILTER = True  # Enable partition filtering (its year)
```

### Query Example

```python
import awswrangler as wr
from concurrent.futures import ThreadPoolExecutor

def query_batch(sensor_ids: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """Query date distribution for a batch of sensors"""

    query = f"""
        SELECT sensorId, DATE(ts) as data_date, COUNT(*) as record_count
        FROM default.sensordata_default
        WHERE sensorId IN ({','.join(f"'{id}'" for id in sensor_ids)})
          AND ts >= TIMESTAMP '{start_date}'
          AND ts <= TIMESTAMP '{end_date}'
        GROUP BY sensorId, DATE(ts)
    """
    return wr.athena.read_sql_query(query, database="default")

def query_all_batches(all_sensor_ids: list[str], ...) -> pd.DataFrame:
    """Concurrent query all batches"""
    batches = chunk_list(all_sensor_ids, BATCH_SIZE)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(query_batch, batch, ...) for batch in batches]
        results = [f.result() for f in futures]

    return pd.concat(results)
```

## CSV Output Format

### Output File Path

```
sbm-ingester/output/data_gap_report_{project}_{timestamp}.csv
```

Example: `data_gap_report_bunnings_20240203_143052.csv`

### CSV Fields

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `nmi_channel` | string | NMI-channel identifier | `Optima_4103381203-E1` |
| `point_id` | string | Neptune point ID | `p:bunnings:19be872dd09-319e1ac1` |
| `issue_type` | string | Issue type | `no_data` / `missing_dates` |
| `missing_dates` | string | Comma-separated missing dates | `2024-01-05,2024-01-06,2024-01-07` |
| `missing_count` | int | Number of missing days | `3` |
| `data_start` | string | Sensor data start date | `2024-01-01` |
| `data_end` | string | Sensor data end date | `2024-01-31` |
| `total_expected_days` | int | Total expected days | `31` |

### Example Output

```csv
nmi_channel,point_id,issue_type,missing_dates,missing_count,data_start,data_end,total_expected_days
Optima_4103381203-E1,p:bunnings:19be872dd09-319e1ac1,missing_dates,"2024-01-05,2024-01-06",2,2024-01-01,2024-01-31,31
SEM0002058-K1,p:bunnings:19bbb227caf-be52d94d,no_data,,0,,,0
```

## Code Structure

### Directory Structure

```
sbm-ingester/
├── src/functions/
│   └── data_gap_detector/
│       ├── __init__.py
│       ├── app.py              # Lambda handler + local entry point
│       ├── detector.py         # Core detection logic
│       ├── mappings.py         # nem12_mappings.json read and filter
│       └── report.py           # CSV report generation
├── output/                     # Local test output directory (gitignore)
└── docs/
    └── nem12_mappings.json     # Existing mappings file
```

### Module Responsibilities

| Module | Responsibility |
|--------|----------------|
| `app.py` | Lambda handler entry, argument parsing, local run support |
| `mappings.py` | Read nem12_mappings.json, filter sensors by project |
| `detector.py` | Connect to Hudi table, query data, analyze missing dates |
| `report.py` | Generate CSV report, write to file |

### Local Run

```bash
cd sbm-ingester
uv run python -m src.functions.data_gap_detector.app \
    --project bunnings \
    --start-date 2024-01-01 \
    --end-date 2024-01-31
```

## Dependencies

```bash
# Add runtime dependency
uv add awswrangler

# Add dev dependency (for progress bar)
uv add --optional dev tqdm
```

## Error Handling

| Scenario | Handling |
|----------|----------|
| nem12_mappings.json not found | Raise FileNotFoundError with clear message |
| No sensors for specified project | Log warning, generate empty report |
| Single batch query fails | Log error, continue with other batches, mark failed sensors in report |
| Athena query timeout | Retry 2 times, mark batch as failed if still fails |
| AWS credentials issue | Raise clear error, prompt to configure AWS credentials |

## Log Output

```
[INFO] Loading mappings for project: bunnings
[INFO] Found 45 sensors for bunnings
[INFO] Processing in 1 batches (batch_size=50, workers=5)
[INFO] Batch 1/1: querying 45 sensors...
[INFO] Analysis complete: 3 sensors with issues
[INFO] Report saved to: output/data_gap_report_bunnings_20240203_143052.csv
```

## Future Enhancements

- [ ] Output to S3 instead of local directory
- [ ] Add all-zero data detection
- [ ] Support more projects (not just bunnings/racv)
- [ ] Add email notification for detected issues
