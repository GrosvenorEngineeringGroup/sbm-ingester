# Billing CSV Import Design

## Overview

Import Bunnings "Usage and Spend Report" CSV into the `sbm-aurora` PostgreSQL database (`bills`, `meters`, `sites` tables). The script is reusable for monthly imports with upsert logic to avoid duplicates.

## CSV Source

- **Format:** UTF-16-LE encoded CSV, 7-row metadata header, then column header + data rows
- **Content:** Monthly billing data per NMI (electricity meter), including usage breakdown (Peak/OffPeak/Shoulder), estimated usage, charges, and spend
- **Scope:** ~413 unique NMIs, ~410 sites, 12 months (Mar 2025 – Feb 2026)

## Data Flow

```
CSV (UTF-16-LE)
  → Python: read + parse (skip first 7 metadata rows)
  → Extract unique sites → UPSERT sites table (key: building_id)
  → Extract unique meters → UPSERT meters table (key: identifier)
  → UPSERT bills table (key: meter_id + bill_date)
```

## Field Mappings

### sites

| DB Column    | CSV Column       | Example                       |
|-------------|------------------|-------------------------------|
| `name`       | Site Name        | `BUN AUS Cairns (Portsmith)`  |
| `address`    | Address          | `71-83 Kenny Street`          |
| `building_id`| Site Reference 3 | `8471`                        |
| `client_id`  | BuyerShortName   | `Bunnings`                    |
| `country`    | Country          | `AU`                          |
| `state`      | State            | `AU:QLD`                      |
| `neptune_id` | NULL             | Linked later                  |

**Upsert key:** `building_id` (unique index)
**On conflict:** update `name`, `address`, `state`, `updated_at`

### meters

| DB Column        | CSV Column      | Example        |
|-----------------|-----------------|----------------|
| `identifier`     | Identifier      | `3052218678`   |
| `identifier_type`| IdentifierType  | `NMI`          |
| `site_id`        | FK → sites.id   | Via building_id lookup |
| `neptune_id`     | NULL            | Linked later   |

**Upsert key:** `identifier` (unique constraint)
**On conflict:** update `site_id`, `updated_at`

### bills

| DB Column                          | CSV Column                    |
|------------------------------------|-------------------------------|
| `meter_id`                         | FK → meters.id (via identifier) |
| `bill_date`                        | Date (`Feb 2026` → `2026-02-01`) |
| `retailer`                         | Retailer                      |
| `peak_usage`                       | Peak                          |
| `off_peak_usage`                   | OffPeak                       |
| `shoulder_usage`                   | Shoulder                      |
| `total_usage`                      | Total Usage                   |
| `total_greenpower_usage`           | Total GreenPower              |
| `estimated_peak_usage`             | Estimated Peak                |
| `estimated_off_peak_usage`         | Estimated OffPeak             |
| `estimated_shoulder_usage`         | Estimated Shoulder            |
| `total_estimated_usage`            | Total Estimated Usage         |
| `total_estimated_greenpower_usage` | Total Estimated GreenPower    |
| `usage_unit`                       | Usage Measurement Unit        |
| `energy_charge`                    | Energy Charge                 |
| `network_charge`                   | Total Network Charge          |
| `environmental_charge`             | Environmental Charge          |
| `metering_charge`                  | Metering Charge               |
| `other_charge`                     | Other Charge                  |
| `total_spend`                      | Total Spend                   |
| `greenpower_spend`                 | GreenPower Spend              |
| `estimated_energy_charge`          | Estimated Energy Charge       |
| `estimated_network_charge`         | Estimated Network Charge      |
| `estimated_environmental_charge`   | Estimated Environmental Charge|
| `estimated_metering_charge`        | Estimated Metering Charge     |
| `estimated_other_charge`           | Estimated Other Charge        |
| `total_estimated_spend`            | Total Estimated Spend         |
| `spend_currency`                   | Spend Currency                |

**Upsert key:** `(meter_id, bill_date)` (primary key)
**On conflict:** update all usage/charge/spend fields

## Schema Changes

Add 3 columns to `bills` table:

```sql
ALTER TABLE bills ADD COLUMN total_greenpower_usage numeric(14,2) NOT NULL DEFAULT 0;
ALTER TABLE bills ADD COLUMN total_estimated_greenpower_usage numeric(14,2) NOT NULL DEFAULT 0;
ALTER TABLE bills ADD COLUMN greenpower_spend numeric(14,2) NOT NULL DEFAULT 0;
```

## Database Connection

Read credentials from AWS Secrets Manager (`prod/db/sbm-aurora`). Support `DATABASE_URL` environment variable as override for local development.

## Script Location

`sbm-ingester/scripts/import_billing_csv.py`

## Usage

```bash
uv run python scripts/import_billing_csv.py "/path/to/report.csv"
```

## Dependencies

- `psycopg2-binary` — PostgreSQL driver
- `boto3` — Secrets Manager access (already in project)
