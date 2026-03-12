# Billing Points CSV Generation + Neptune Import

## Goal

Generate a CSV file containing all billing point configurations (477 meters × 23 fields = 10,971 rows), then import them into Neptune via gemsNeptuneExplorer Gremlin queries.

## Scripts

| Script | Purpose |
|--------|---------|
| `scripts/generate_billing_points_csv.py` | Read Aurora + nem12_mappings → generate `data/billing_points.csv` |
| `scripts/import_billing_points.py` | Read CSV → create Neptune points via Gremlin |

## CSV Format

File: `data/billing_points.csv` (10,971 rows)

| Column | Description | Example |
|--------|-------------|---------|
| `identifier` | NMI or ICP | `3052218678` |
| `field` | Billing field name | `peak_usage` |
| `nem12_id` | Dedup key for Neptune | `3052218678-billing-peak-usage` |
| `label` | Neptune display name | `3052218678 Peak Usage` |
| `point_category` | Constant `billing` | `billing` |
| `meter_vertex_id` | Target meter vertex ID | `p:bunnings:r:263a82cb-155b9d48` |

### 23 Billing Fields

| Field | nem12_id Suffix | Label Display Name |
|-------|----------------|-------------------|
| `peak_usage` | `billing-peak-usage` | Peak Usage |
| `off_peak_usage` | `billing-off-peak-usage` | Off Peak Usage |
| `shoulder_usage` | `billing-shoulder-usage` | Shoulder Usage |
| `total_usage` | `billing-total-usage` | Total Usage |
| `total_greenpower_usage` | `billing-total-greenpower-usage` | Total Greenpower Usage |
| `estimated_peak_usage` | `billing-estimated-peak-usage` | Estimated Peak Usage |
| `estimated_off_peak_usage` | `billing-estimated-off-peak-usage` | Estimated Off Peak Usage |
| `estimated_shoulder_usage` | `billing-estimated-shoulder-usage` | Estimated Shoulder Usage |
| `total_estimated_usage` | `billing-total-estimated-usage` | Total Estimated Usage |
| `total_estimated_greenpower_usage` | `billing-total-estimated-greenpower-usage` | Total Estimated Greenpower Usage |
| `energy_charge` | `billing-energy-charge` | Energy Charge |
| `network_charge` | `billing-network-charge` | Network Charge |
| `environmental_charge` | `billing-environmental-charge` | Environmental Charge |
| `metering_charge` | `billing-metering-charge` | Metering Charge |
| `other_charge` | `billing-other-charge` | Other Charge |
| `total_spend` | `billing-total-spend` | Total Spend |
| `greenpower_spend` | `billing-greenpower-spend` | Greenpower Spend |
| `estimated_energy_charge` | `billing-estimated-energy-charge` | Estimated Energy Charge |
| `estimated_network_charge` | `billing-estimated-network-charge` | Estimated Network Charge |
| `estimated_environmental_charge` | `billing-estimated-environmental-charge` | Estimated Environmental Charge |
| `estimated_metering_charge` | `billing-estimated-metering-charge` | Estimated Metering Charge |
| `estimated_other_charge` | `billing-estimated-other-charge` | Estimated Other Charge |
| `total_estimated_spend` | `billing-total-estimated-spend` | Total Estimated Spend |

## Script 1: generate_billing_points_csv.py

### Input
- Aurora PostgreSQL: `meters` table (477 rows)
- S3: `nem12_mappings.json` (maps `Optima_{identifier}-{channel}` → point vertex ID)

### Logic
1. Read all meters from Aurora (identifier + identifier_type)
2. Download `nem12_mappings.json` from S3
3. For each meter identifier, find a matching key `Optima_{identifier}-*` in mappings
4. Get the sensor point ID from mappings
5. Query Neptune for the meter vertex: `g.V('{point_id}').out('equipRef').id()`
6. For each of the 23 billing fields, output one CSV row
7. Write to `data/billing_points.csv`

### Meter Vertex ID Resolution

```
nem12_mappings.json: Optima_3052218678-E1 → p:bunnings:r:263a82cc-9205e1cd (sensor point)
Neptune query: g.V('p:bunnings:r:263a82cc-9205e1cd').out('equipRef').id() → p:bunnings:r:263a82cb-155b9d48 (meter)
```

Batch the Neptune queries to minimize Lambda invocations.

### CLI

```bash
PYTHONPATH=src uv run scripts/generate_billing_points_csv.py
```

### Output

```csv
identifier,field,nem12_id,label,point_category,meter_vertex_id
3052218678,peak_usage,3052218678-billing-peak-usage,3052218678 Peak Usage,billing,p:bunnings:r:263a82cb-155b9d48
3052218678,off_peak_usage,3052218678-billing-off-peak-usage,3052218678 Off Peak Usage,billing,p:bunnings:r:263a82cb-155b9d48
...
0000005438UN02B,peak_usage,0000005438UN02B-billing-peak-usage,0000005438UN02B Peak Usage,billing,p:bunnings:19c02b1f893-19c06521
```

## Script 2: import_billing_points.py

### Input
- `data/billing_points.csv` (10,971 rows)

### Logic
1. Read CSV
2. For each row:
   a. Check if `nem12_id` already exists in Neptune: `g.V().has('nem12Id', '{nem12_id}').id()`
   b. If exists → skip (already created)
   c. If not → generate `point_id`, execute atomic Gremlin:
      ```groovy
      g.addV('point')
        .property(id, '{point_id}')
        .property('label', '{label}')
        .property('nem12Id', '{nem12_id}')
        .property('pointCategory', 'billing')
        .as('pt')
        .V('{meter_vertex_id}')
        .addE('equipRef').from('pt')
      ```
3. Print summary: created / skipped / failed

### Point ID Generation
```python
import time, secrets
hex_ts = format(int(time.time() * 1000), 'x')
hex_rand = secrets.token_hex(3)
point_id = f"p:bunnings:{hex_ts}-{hex_rand}"
```

### CLI

```bash
# Dry run
PYTHONPATH=src uv run scripts/import_billing_points.py --csv data/billing_points.csv --dry-run

# Live run
PYTHONPATH=src uv run scripts/import_billing_points.py --csv data/billing_points.csv
```

### Dry Run Output

```
[DRY RUN] Would create 10,971 billing points for 477 meters
  Sample:
    3052218678 Peak Usage → meter p:bunnings:r:263a82cb-155b9d48
    3052218678 Off Peak Usage → meter p:bunnings:r:263a82cb-155b9d48
    ...

  Summary:
    Total:   10,971
    Create:  10,971
    Skip:    0
```

### Live Run Output

```
Creating billing points...
  [1/10971] 3052218678 Peak Usage → p:bunnings:19c884b1234-abc123 ✓
  [2/10971] 3052218678 Off Peak Usage → p:bunnings:19c884b1234-def456 ✓
  ...

Summary:
  Created: 10,971
  Skipped: 0
  Failed:  0
```

### Error Handling
- If a Gremlin query fails, log the error and continue to the next row
- Failed rows are collected and printed at the end
- Re-running is safe (idempotent via nem12Id check)

## Scale

| Metric | Count |
|--------|-------|
| Meters | 477 |
| Fields per meter | 23 |
| CSV rows | 10,971 |
| Neptune API calls (import) | Up to 10,971 (skips existing) |
