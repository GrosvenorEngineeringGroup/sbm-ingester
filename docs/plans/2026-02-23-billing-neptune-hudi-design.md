# Billing Data → Neptune Points + Hudi Data Lake

## Goal

Read billing data from Aurora PostgreSQL (sites/meters/bills tables), create one Neptune point vertex per billing field per meter identifier (23 fields × 477 meters = ~10,971 points), then write all billing values (including zeros) to the Hudi data lake. Hudi becomes the single source of truth for billing data — Aurora PostgreSQL will be decommissioned after migration.

## Background

Billing data (usage + spend) is already imported into Aurora PostgreSQL via `scripts/import_billing_csv.py`. The manager wants each billing column (peak_usage, energy_charge, etc.) to be a separate Neptune point, so it can be queried via the same Hudi pipeline that handles interval meter data.

## Aurora Data Profile

| Dimension | Value |
|-----------|-------|
| Sites | 409 (AU 355 + NZ 54) |
| Meters | 477 (NMI 413 + ICP 64) |
| Bills | 5,724 (12 months per meter: 2025-03 → 2026-02) |
| Currency | AUD 4,956 rows / NZD 768 rows |
| Usage unit | All `kWh` |
| Identifier types | AU: NMI (e.g. `3052218678`), NZ: ICP (e.g. `0000005438UN02B`) |
| All-zero bill rows | 360 (6%) — still exported, not skipped |
| `neptune_id` on sites/meters | All NULL — not yet populated |

**ICP meters are treated identically to NMI meters.** The identifier (NMI or ICP) is used as-is in labels and nem12Id. No distinction in Neptune or Hudi.

## Neptune Meter Coverage (Verified)

Cross-referenced all 477 Aurora meters against `nem12_mappings.json` (S3). Existing sensor points use `Optima_{identifier}-{channel}` as nem12Id key.

| Category | Count | Neptune Status |
|----------|-------|---------------|
| AU NMI (in mappings as `Optima_{nmi}`) | 404 | Meter exists |
| NZ ICP (in mappings as `Optima_{icp}`) | 64 | Meter exists |
| **Missing — not in mappings anywhere** | **9** | **Meter does NOT exist** |

### 9 Missing Meters

All AU NMI, all `building_id = NULL`:

| NMI | Site Name | State | Usage (12mo) | Spend (12mo) |
|-----|-----------|-------|-------------|-------------|
| 3120787756 | BUN AUS North MacLean DC | QLD | 178,625 kWh | $32,449 |
| 3120898626 | BUN AUS Wacol Trade and CFC | QLD | 403,459 kWh | $66,424 |
| 3120914378 | BUN AUS Oxley WH | QLD | 770,390 kWh | $129,543 |
| 3120987157 | BUN AUS North MacLean DC | QLD | 12,379 kWh | $3,247 |
| 4310030332 | BUN AUS Valley Heights Store | NSW | 66,189 kWh | $17,713 |
| 4001210099 | _(empty)_ | NSW | 361 kWh | $574 |
| 4001213912 | _(empty)_ | NSW | 447 kWh | $536 |
| UEE0005056 | _(empty)_ | NSW | 0 kWh | $3,610 |
| UEE9905432 | _(empty)_ | NSW | 0 kWh | $0 |

**Pre-requisite:** These 9 meters must be created in Neptune via meter-importer before billing points can be attached. CSV at `data/missing_meters.csv`. Note: 4 meters have empty site names — site names in CSV are placeholders (`BUN AUS Bushland Dr - {nmi}`) and need review.

## Execution Steps

### Step 0: Create 9 Missing Meters (meter-importer)

Use the existing meter-importer workflow to create the Neptune hierarchy (State → Region → Site → Level → Meter) for the 9 missing meters.

```bash
cd /Users/zeyu/Desktop/GEG/sbm/meter-importer

# Dry run first
uv run python scripts/import_meters.py --csv ../sbm-ingester/data/missing_meters.csv --proj-id bunnings --dry-run

# Live run after review
uv run python scripts/import_meters.py --csv ../sbm-ingester/data/missing_meters.csv --proj-id bunnings
```

After creation, refresh nem12_mappings:
```bash
aws lambda invoke --function-name sbm-files-ingester-nem12-mappings-to-s3 --region ap-southeast-2 /tmp/response.json
```

### Step 1: Create Billing Points in Neptune

### Step 2: Export Billing Data to Hudi

_(Both steps handled by `scripts/export_billing_to_hudi.py` — see Implementation Script below)_

## Neptune Point Design

### Hierarchy

```
Site → Meter → Point (billing)
               Point (billing)
               Point (billing)
               ...
               Point (sensor, existing)
```

Points are attached directly to the Meter vertex via `equipRef` edge. No intermediate Billing vertex.

### Vertex Properties

| Property | Required | Format | Example (NMI) | Example (ICP) |
|----------|----------|--------|---------------|---------------|
| `id` | Yes | `p:bunnings:{hex}-{hex}` | `p:bunnings:19c50a1b2c-3f8a1d` | `p:bunnings:19c50a1b2c-4e2b0a` |
| `label` | Yes | `{identifier} {Display Name}` | `3052218678 Peak Usage` | `0000005438UN02B Peak Usage` |
| `nem12Id` | Yes | `{identifier}-billing-{field}` | `3052218678-billing-peak-usage` | `0000005438UN02B-billing-peak-usage` |
| `pointCategory` | Yes | `billing` (constant) | `billing` | `billing` |

- `nem12Id` serves as the deduplication key (idempotent point creation)
- `pointCategory` distinguishes billing points from sensor points in queries
- Billing `nem12Id` format (`{id}-billing-{field}`) is distinct from sensor `nem12Id` format (`Optima_{id}-{channel}`) — no collision

### Edges

| Edge | Direction | Target | Purpose |
|------|-----------|--------|---------|
| `equipRef` | Point → Meter | Meter vertex | Attaches point to its meter |

No `siteRef` edge. No `gegPointType`.

### All 23 Billing Fields

| Field Name | nem12Id Suffix | Label Display Name | Unit Source |
|------------|---------------|-------------------|-------------|
| `peak_usage` | `billing-peak-usage` | Peak Usage | `bills.usage_unit` |
| `off_peak_usage` | `billing-off-peak-usage` | Off Peak Usage | `bills.usage_unit` |
| `shoulder_usage` | `billing-shoulder-usage` | Shoulder Usage | `bills.usage_unit` |
| `total_usage` | `billing-total-usage` | Total Usage | `bills.usage_unit` |
| `total_greenpower_usage` | `billing-total-greenpower-usage` | Total Greenpower Usage | `bills.usage_unit` |
| `estimated_peak_usage` | `billing-estimated-peak-usage` | Estimated Peak Usage | `bills.usage_unit` |
| `estimated_off_peak_usage` | `billing-estimated-off-peak-usage` | Estimated Off Peak Usage | `bills.usage_unit` |
| `estimated_shoulder_usage` | `billing-estimated-shoulder-usage` | Estimated Shoulder Usage | `bills.usage_unit` |
| `total_estimated_usage` | `billing-total-estimated-usage` | Total Estimated Usage | `bills.usage_unit` |
| `total_estimated_greenpower_usage` | `billing-total-estimated-greenpower-usage` | Total Estimated Greenpower Usage | `bills.usage_unit` |
| `energy_charge` | `billing-energy-charge` | Energy Charge | `bills.spend_currency` |
| `network_charge` | `billing-network-charge` | Network Charge | `bills.spend_currency` |
| `environmental_charge` | `billing-environmental-charge` | Environmental Charge | `bills.spend_currency` |
| `metering_charge` | `billing-metering-charge` | Metering Charge | `bills.spend_currency` |
| `other_charge` | `billing-other-charge` | Other Charge | `bills.spend_currency` |
| `total_spend` | `billing-total-spend` | Total Spend | `bills.spend_currency` |
| `greenpower_spend` | `billing-greenpower-spend` | Greenpower Spend | `bills.spend_currency` |
| `estimated_energy_charge` | `billing-estimated-energy-charge` | Estimated Energy Charge | `bills.spend_currency` |
| `estimated_network_charge` | `billing-estimated-network-charge` | Estimated Network Charge | `bills.spend_currency` |
| `estimated_environmental_charge` | `billing-estimated-environmental-charge` | Estimated Environmental Charge | `bills.spend_currency` |
| `estimated_metering_charge` | `billing-estimated-metering-charge` | Estimated Metering Charge | `bills.spend_currency` |
| `estimated_other_charge` | `billing-estimated-other-charge` | Estimated Other Charge | `bills.spend_currency` |
| `total_estimated_spend` | `billing-total-estimated-spend` | Total Estimated Spend | `bills.spend_currency` |

**Unit determination:** Each bill row in Aurora stores `usage_unit` (e.g. "kWh") and `spend_currency` (e.g. "AUD", "NZD"). The Hudi CSV `unit` column is read from these fields per bill row (lowercased). Not hardcoded.

### Concrete Examples

**AU NMI meter** `3052218678` (sensor nem12Id: `Optima_3052218678-E1`):

```
Vertex: Peak Usage
  id:            p:bunnings:19c50a1b2c-3f8a01
  label:         3052218678 Peak Usage
  nem12Id:       3052218678-billing-peak-usage
  pointCategory: billing
  Edge:          equipRef → (meter vertex of 3052218678)
```

**NZ ICP meter** `0000005438UN02B` (sensor nem12Id: `Optima_0000005438UN02B-E1`):

```
Vertex: Total Spend
  id:            p:bunnings:19c50a1b2c-4e2b0a
  label:         0000005438UN02B Total Spend
  nem12Id:       0000005438UN02B-billing-total-spend
  pointCategory: billing
  Edge:          equipRef → (meter vertex of 0000005438UN02B)
```

### Neptune Write Method

Write directly via neptune-explorer Lambda HTTP endpoint (Gremlin queries). Each point creation must be atomic — vertex + edge in one request:

```groovy
g.addV('point')
  .property(id, 'p:bunnings:19c50a1b2c-3f8a01')
  .property('label', '3052218678 Peak Usage')
  .property('nem12Id', '3052218678-billing-peak-usage')
  .property('pointCategory', 'billing')
  .as('pt')
  .V('{meter_vertex_id}')
  .addE('equipRef').from('pt')
```

### ID Generation

Generate IDs matching existing convention:
```python
import time, secrets
hex_ts = format(int(time.time() * 1000), 'x')   # e.g. "19c50a1b2c"
hex_rand = secrets.token_hex(3)                    # e.g. "3f8a01"
point_id = f"p:bunnings:{hex_ts}-{hex_rand}"
```

## Hudi Data Lake Design

### CSV Format

Write billing values as standard Hudi CSVs to `s3://hudibucketsrc/sensorDataFiles/`:

```csv
sensorId,ts,val,unit,its
p:bunnings:19c50a1b2c-3f8a01,2026-01-01 00:00:00,1234.56,kwh,2026-01-01 00:00:00
p:bunnings:19c50a1b2c-3f8a02,2026-01-01 00:00:00,5678.90,aud,2026-01-01 00:00:00
p:bunnings:19c50a1b2c-4e2b0a,2026-01-01 00:00:00,999.00,nzd,2026-01-01 00:00:00
```

| Column | Value | Notes |
|--------|-------|-------|
| `sensorId` | Neptune point ID | From nem12_mappings or created ID |
| `ts` | `{bill_date} 00:00:00` | First of month, e.g. `2026-01-01 00:00:00` |
| `val` | Billing field value | Including zeros — all rows exported |
| `unit` | From bill row | Usage fields: `bills.usage_unit` lowercased (e.g. `kwh`). Spend fields: `bills.spend_currency` lowercased (e.g. `aud`, `nzd`). |
| `its` | Same as `ts` | Partition key |

### Row Count

All bill rows are exported, including zeros. No filtering.

- 5,724 bills × 23 fields = **131,652 Hudi rows**

### Upsert Behavior

Hudi record key = `sensorId + ts`. Re-importing the same billing month for the same point will overwrite (idempotent).

### Trigger

After uploading CSVs, trigger Glue ETL:
```bash
aws glue start-job-run --job-name DataImportIntoLake --region ap-southeast-2
```

## Data Flow

```
Step 0 (one-time):
  data/missing_meters.csv → meter-importer → Push API → Neptune (9 missing meters created)

Step 1 + 2 (export_billing_to_hudi.py):
  Aurora PostgreSQL (bills table)
    → Read all 477 meters + 5,724 bills
    → Look up meter vertex IDs from nem12_mappings.json
    → Create 23 billing point vertices per meter in Neptune (via Gremlin)
    → Generate Hudi CSV rows for all bill_date × field (131,652 rows)
    → Upload CSVs to s3://hudibucketsrc/sensorDataFiles/
    → Trigger Glue job
    → Data available in Athena: default.sensordata_default
    → Aurora PostgreSQL can be decommissioned
```

## Finding Meter Vertex IDs

The script uses `nem12_mappings.json` to find meter vertex IDs. Sensor points have nem12Id `Optima_{identifier}-{channel}`, and their meter is the parent via `equipRef`.

Approach:
1. Download `nem12_mappings.json` from S3
2. For each Aurora meter identifier, find any matching key `Optima_{identifier}-*`
3. The mapping value is a sensor point ID (e.g. `p:bunnings:19be872dd09-319e1ac1`)
4. Query Neptune to get the meter vertex: `g.V('{point_id}').out('equipRef').id()`
5. Cache the identifier → meter_vertex_id map

Alternative: batch query all meter vertices from Neptune directly:
```groovy
g.V().hasLabel('meter').valueMap(true, 'label')
```
Then parse identifier from meter label (format: `{site_name} PM {identifier}`).

## Idempotency

| Step | Mechanism |
|------|-----------|
| Neptune point creation | Check `nem12Id` exists before creating (`g.V().has('nem12Id', '{id}-billing-{field}')`) |
| Hudi data write | Upsert on `sensorId + ts` — same month overwrites |

## Scale

| Metric | Count |
|--------|-------|
| Meters | 477 (413 NMI + 64 ICP) |
| Meters already in Neptune | 468 |
| Meters to create first | 9 (via meter-importer) |
| Billing fields per meter | 23 |
| Neptune billing points to create | ~10,971 |
| Bills in Aurora | 5,724 |
| Hudi rows (bills × fields) | 131,652 |

## Known Trade-offs

1. **No gegPointType**: Existing Lambdas that filter by gegPointType won't see billing points. New Lambdas will be built for billing data retrieval.
2. **No siteRef**: Site-level queries traversing siteRef edges won't include billing points. Can traverse Meter → Site if needed.
3. **nem12_mappings bloat**: ~11K billing entries will be added to `nem12_mappings.json` (currently ~2K entries). File processor loads this into memory but won't mis-match billing entries (format `{id}-billing-{field}` is distinct from `Optima_{id}-{channel}`). Acceptable overhead.
4. **Monthly granularity in Hudi**: Hudi is typically used for interval data (15min/30min). Monthly data at `YYYY-MM-01 00:00:00` works technically but is non-standard.
5. **Zero values exported**: All 131,652 rows are written including zeros. This is intentional — Hudi is the single source of truth replacing Aurora.
6. **4 meters have placeholder site names**: NMIs 4001210099, 4001213912, UEE0005056, UEE9905432 had empty site names in billing CSV. Placeholder names used in `data/missing_meters.csv` — needs review before running meter-importer.

## Implementation Script

A single Python script `scripts/export_billing_to_hudi.py` that:

1. Reads all meters + bills from Aurora PostgreSQL (NMI and ICP, no filtering)
2. Loads nem12_mappings.json to resolve meter vertex IDs
3. For each meter: creates 23 billing point vertices if not exists (idempotent via nem12Id check)
4. Generates Hudi CSV rows for all bill_date × field combinations (including zeros)
5. Uploads CSV files to S3
6. Optionally triggers Glue job

### CLI Interface

```bash
# Dry run (no Neptune writes, no S3 uploads)
PYTHONPATH=src uv run scripts/export_billing_to_hudi.py --dry-run

# Live run
PYTHONPATH=src uv run scripts/export_billing_to_hudi.py

# Skip Neptune point creation (points already exist)
PYTHONPATH=src uv run scripts/export_billing_to_hudi.py --skip-neptune

# Skip Hudi export (only create Neptune points)
PYTHONPATH=src uv run scripts/export_billing_to_hudi.py --skip-hudi
```
