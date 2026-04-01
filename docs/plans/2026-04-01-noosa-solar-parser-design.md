# Noosa Solar CSV Parser Integration

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest RACV Noosa Solar CSV files from BMS team into the Hudi data lake. The CSV contains 40 Fronius solar inverter data points with SkySpark point IDs (`p:racv:r:xxx`) directly in the headers — no Neptune mapping required.

**Architecture:** The BMS team uploads `RACV_Noosa_Solar.csv` daily to S3 (`sbm-file-ingester/newTBP/`). The file has two types of values: numeric energy readings (kWh) and string inverter operating modes (e.g., "Normal Operation"). A new non-NEM parser handles this format, and the file processor is updated to bypass Neptune ID mapping when the parser returns identifiers that are already SkySpark point IDs (prefix `p:`).

**Key design decisions:**

- **No Neptune mapping:** These point IDs are provided directly by BMS team and do not exist in Neptune. The `p:` prefix check in the file processor skips mapping lookup entirely.
- **Dynamic type detection:** Each column is classified as numeric (kWh) or status (mode) using `pd.to_numeric(errors="coerce")`, rather than hardcoding column types. Columns are consistently typed.
- **Fronius mode mapping:** String status values (e.g., "Normal Operation" → 4) are mapped to numeric codes via a hardcoded dictionary. Unit stored as `mode`.
- **Timezone handling:** Strip timezone suffix (AEST/AEDT) before parsing timestamps. Log a warning if non-AEST timezone is detected (Noosa is in QLD, no DST). Store as timezone-naive local time, consistent with all other parsers.
- **Quality column:** Not applicable. The CSV has no quality data. The pipeline handles this gracefully — `write_row()` defaults `quality=""`, Glue reads as NULL.
- **Duplicate data:** Daily files may overlap. Hudi upsert on `sensorId + ts` handles deduplication.

**Fronius Inverter Mode Dictionary:**

| Code | Status String |
|------|---------------|
| 1 | Off |
| 2 | In Operation, No Feed In |
| 3 | Run Up Phase |
| 4 | Normal Operation |
| 5 | Power Reduction |
| 6 | Switch Off Phase |
| 7 | Error Exists |
| 8 | Standby |
| 9 | No Fronius Solar Net Comm |
| 10 | No Comm with Inverter |
| 11 | Overcurrent detected in Fronius Solar Net |
| 12 | Inverter Update being Processed |
| 13 | AFCI Event |

**Tech Stack:** Python 3.13, pandas, Ruff (ANN, PTH rules enabled)

**Known edge cases:**
- CSV may contain BOM header → use `encoding="utf-8-sig"` in `pd.read_csv()`
- Numeric columns may have `nan` string or empty cells → pandas reads both as `NaN`, dropped by `dropna()`
- First data column (`p:racv:r:31425a11-ef34f44a`) is always `0` → valid numeric value, must be preserved
- Unknown Fronius status strings → logged as warning, mapped to `NaN`, dropped

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `src/shared/noosa_solar_parser.py` | Create | Parser with Fronius mode dictionary, timestamp handling, dynamic type detection |
| `src/shared/non_nem_parsers.py` | Modify | Import and register new parser at top of dispatcher list |
| `src/functions/file_processor/app.py` | Modify | Add `p:` prefix bypass for direct point ID usage (2 lines) |
| `tests/unit/test_noosa_solar_parser.py` | Create | Parser tests following existing patterns |

**Files NOT modified (by design):**
- `src/shared/__init__.py` — new parser is imported internally by `non_nem_parsers.py`, no public export needed
- `src/shared/common.py` — no new constants needed
- `src/glue/hudi_import/script.py` — schema already includes `quality` column, no changes needed
- `.github/workflows/main.yml` — `src/shared/**` path filter already covers the new file

---

## Task 1: Create Noosa Solar Parser

**Files:**
- Create: `src/shared/noosa_solar_parser.py`

- [ ] **Step 1: Create parser module with Fronius mode dictionary and parser function**

Create `src/shared/noosa_solar_parser.py` with:

```python
from pathlib import Path

import pandas as pd
from aws_lambda_powertools import Logger

logger = Logger(service="noosa-solar-parser", child=True)

# Type alias — defined locally to avoid circular import with non_nem_parsers.py
ParserResult = list[tuple[str, pd.DataFrame]]

# Fronius inverter operating mode → numeric code
FRONIUS_MODE_MAP: dict[str, int] = {
    "Off": 1,
    "In Operation, No Feed In": 2,
    "Run Up Phase": 3,
    "Normal Operation": 4,
    "Power Reduction": 5,
    "Switch Off Phase": 6,
    "Error Exists": 7,
    "Standby": 8,
    "No Fronius Solar Net Comm": 9,
    "No Comm with Inverter": 10,
    "Overcurrent detected in Fronius Solar Net": 11,
    "Inverter Update being Processed": 12,
    "AFCI Event": 13,
}


def noosa_solar_parser(file_name: str, error_file_path: str) -> ParserResult:
    """Parse RACV Noosa Solar CSV with SkySpark point IDs as column headers."""
    if "RACV_Noosa_Solar" not in Path(file_name).name:
        raise Exception("Not a Noosa Solar file")

    df = pd.read_csv(file_name, encoding="utf-8-sig")

    # Validate expected format: first column must be 'timestamp'
    if df.columns[0] != "timestamp":
        raise Exception("Missing timestamp column in Noosa Solar file")

    # Strip timezone suffix (AEST/AEDT) and parse timestamps
    tz_values = df["timestamp"].dropna().str.extract(r'\s+([A-Z]{3,4})$')[0].dropna().unique()
    unexpected_tz = [tz for tz in tz_values if tz != "AEST"]
    if len(unexpected_tz) > 0:
        logger.warning(
            "Unexpected timezone in Noosa Solar file",
            extra={"timezones": unexpected_tz},
        )

    df["timestamp"] = df["timestamp"].str.replace(r'\s+[A-Z]{3,4}$', '', regex=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%d-%b-%y %I:%M %p")

    sensor_columns = [col for col in df.columns if col.startswith("p:")]

    results: ParserResult = []
    for sensor_id in sensor_columns:
        series = df[sensor_id]

        # Dynamic type detection: try numeric conversion
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null_count = series.dropna().shape[0]
        numeric_count = numeric_series.dropna().shape[0]

        if non_null_count == 0:
            continue  # Skip all-NaN columns

        if numeric_count >= non_null_count * 0.5:
            # Numeric column (kWh energy readings)
            col_name = "E1_kWh"
            out_df = pd.DataFrame({
                "t_start": df["timestamp"],
                col_name: numeric_series,
            })
        else:
            # Status column — map strings to Fronius mode codes
            col_name = "E1_mode"
            mapped = series.map(FRONIUS_MODE_MAP)
            unmapped = series.dropna()[~series.dropna().isin(FRONIUS_MODE_MAP)].unique()
            if len(unmapped) > 0:
                logger.warning(
                    "Unknown Fronius mode values",
                    extra={"sensor_id": sensor_id, "values": unmapped.tolist()},
                )
            out_df = pd.DataFrame({
                "t_start": df["timestamp"],
                col_name: mapped.astype(float),
            })

        out_df = out_df.dropna(subset=[col_name])
        out_df = out_df.set_index("t_start")

        if not out_df.empty:
            results.append((sensor_id, out_df))

    if not results:
        raise Exception(f"No valid data in Noosa Solar file: {file_name}")

    return results
```

**Key behaviors:**
- File name check: rejects non-matching files immediately
- Timezone: strips AEST/AEDT, warns on unexpected timezone
- Type detection: `pd.to_numeric(errors="coerce")` — if ≥50% values are numeric, treat as kWh; otherwise map via `FRONIUS_MODE_MAP`
- Unknown status strings: logged as warning, mapped to NaN, dropped by `dropna()`
- Returns `(sensor_id, DataFrame)` where `sensor_id` is the full SkySpark point ID (`p:racv:r:xxx`)

---

## Task 2: Register Parser in Dispatcher

**Files:**
- Modify: `src/shared/non_nem_parsers.py`

- [ ] **Step 1: Import and register the Noosa Solar parser**

Add import at top of file:

```python
from shared.noosa_solar_parser import noosa_solar_parser
```

Add to the `parsers` list in `get_non_nem_df()` as the **first entry** (fast file name rejection):

```python
parsers = [
    noosa_solar_parser,  # Must be first — checks filename, fast rejection
    envizi_vertical_parser_water,
    envizi_vertical_parser_electricity,
    racv_elec_parser,
    optima_usage_and_spend_to_s3,
    optima_parser,
    envizi_vertical_parser_water_bulk,
    green_square_private_wire_schneider_comx_parser,
]
```

---

## Task 3: Add `p:` Prefix Bypass in File Processor

**Files:**
- Modify: `src/functions/file_processor/app.py`

- [ ] **Step 1: Skip Neptune mapping for direct point IDs**

In `parse_and_write_data()`, replace lines 462-466:

```python
# Before:
monitor_point_name = f"{nmi}-{suffix}"
neptune_id = nem12_mappings.get(monitor_point_name)

if neptune_id is None:
    continue

# After:
if nmi.startswith("p:"):
    neptune_id = nmi
else:
    monitor_point_name = f"{nmi}-{suffix}"
    neptune_id = nem12_mappings.get(monitor_point_name)

if neptune_id is None:
    continue
```

**Why this order:** These point IDs do not exist in Neptune. The `p:` prefix is the contract with the BMS team — any identifier starting with `p:` is already a SkySpark point ID and should be written directly to the data lake without mapping lookup.

---

## Task 4: Write Tests

**Files:**
- Create: `tests/unit/test_noosa_solar_parser.py`

- [ ] **Step 1: Create test file with comprehensive test cases**

Follow existing test patterns from `test_non_nem_parsers.py`:
- Import under test inside test method with `patch("shared.non_nem_parsers.logger")`
- Use `tmp_path` or `temp_directory` fixture for temp files

**Test cases to cover:**

| Test | Description |
|------|-------------|
| `test_parse_numeric_columns` | Numeric kWh values parsed correctly, column name is `E1_kWh` |
| `test_parse_status_columns` | Status strings mapped to codes via FRONIUS_MODE_MAP, column name is `E1_mode` |
| `test_rejects_non_matching_file` | Non-`RACV_Noosa_Solar` files raise exception |
| `test_timestamp_parsing` | `31-Mar-26 8:00 AM AEST` parsed correctly |
| `test_timezone_warning` | Non-AEST timezone logs warning but still parses |
| `test_nan_values_dropped` | NaN and empty values filtered out |
| `test_mixed_empty_and_nan_values` | Both empty cells (`,,`) and `nan` strings are handled identically (dropped) |
| `test_unknown_status_warning` | Unknown status strings logged as warning and dropped |
| `test_all_columns_return_t_start` | All returned DataFrames have `t_start` as index |
| `test_sensor_id_format` | All returned identifiers start with `p:racv:r:` |
| `test_empty_file` | File with only headers raises exception |
| `test_all_nan_column_skipped` | Column where every value is NaN is excluded from results |
| `test_all_zero_column_preserved` | Column with all `0` values is correctly classified as numeric and preserved |
| `test_multiple_status_values` | Column with mixed status strings (Normal Operation, Error Exists, etc.) all mapped correctly |
| `test_dispatcher_integration` | `get_non_nem_df()` correctly routes `RACV_Noosa_Solar` files to the Noosa parser |
| `test_p_prefix_bypass_in_file_processor` | Integration: `p:` prefix sensors bypass Neptune mapping |

---

## Verification

After implementation:

```bash
# Lint
uv run ruff check src/shared/noosa_solar_parser.py
uv run ruff format --check src/shared/noosa_solar_parser.py

# Tests
uv run pytest tests/unit/test_noosa_solar_parser.py -v

# Full test suite (ensure no regressions)
uv run pytest --cov=src

# Coverage must remain ≥ 90%
```
