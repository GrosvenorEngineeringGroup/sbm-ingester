# Neptune PM Reassignment Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move 2 Bunnings power meter clusters (`SEM0002152` and `SEM0002125`) to their correct store sites in Neptune by dropping and recreating the affected PM + point vertices with user-supplied IDs so the labels update but all vertex IDs stay the same, while also creating the missing `7349 BUN BATEMANS BAY SLR` site and correcting the `8220 BUN CABOOLTURE SLR` geoAddress.

**Architecture:** One Python CLI script at `sbm/sbm-ingester/scripts/migrate_pm_reassignment.py` invoked via `uv run` with a required `--phase` flag. The script reuses the existing `billing_neptune_helper.gremlin_query()` helper which routes Gremlin queries through the `gemsNeptuneExplorer` Lambda (the same pattern used in `import_billing_points.py`). Data-model constants (all 12 known vertex IDs, the property template for new SLR sites, the target label mappings) are defined once at module level so every phase reads from the same source of truth. Phase 0 is a non-destructive backup that dumps the 6 affected vertices plus all their edges to a timestamped JSON file **and** emits a `restore.gremlin` script for emergency rollback. Phase 1a updates the Caboolture SLR address in place. Phase 1b creates the new Batemans Bay SLR site and Main vertex. Phases 2 and 3 perform the PM cluster migrations using a drop-all-then-recreate-all pattern, with each phase's drop and recreate submitted as two consecutive Gremlin HTTP requests (Neptune's per-request atomicity guarantees each request is all-or-nothing). Phase 4 runs a terminal-state verification query and asserts the expected topology.

**Tech Stack:** Python 3.13, uv, boto3 (via existing Lambda client in `billing_neptune_helper`), pytest + pytest-mock for unit tests, Amazon Neptune Gremlin API via `gemsNeptuneExplorer` Lambda.

---

## Pre-Flight Context (read before Task 1)

### Data already verified in Neptune (as of plan creation date)

**6 vertices to migrate (drop + recreate with same ID, new label):**

| Role | Vertex ID | Current Label |
|---|---|---|
| PM | `p:bunnings:19c4ece1aef-b978c70b` | `7213 BUN KIRRAWEE PM SEM0002152` |
| Point B1 | `p:bunnings:19c4ece1d9d-b90deb5e` | `7213 BUN KIRRAWEE PM SEM0002152 Total Energy - Active (Source) B1` |
| Point K1 | `p:bunnings:19c4ece1db1-7c845958` | `7213 BUN KIRRAWEE PM SEM0002152 Total Energy - Reactive (Source) K1` |
| PM | `p:bunnings:19c4c977981-db50da59` | `8220 BUN CABOOLTURE SLR PM SEM0002125` |
| Point B1 | `p:bunnings:19c4ccf75ac-5c09fd71` | `8220 BUN CABOOLTURE SLR PM SEM0002125 Total Energy - Active (Source) B1` |
| Point K1 | `p:bunnings:19c4ccf75c1-8f262f80` | `8220 BUN CABOOLTURE SLR PM SEM0002125 Total Energy - Reactive (Source) K1` |

**Target labels after migration:**

| Vertex ID | New Label |
|---|---|
| `p:bunnings:19c4ece1aef-b978c70b` | `8220 BUN CABOOLTURE SLR PM SEM0002152` |
| `p:bunnings:19c4ece1d9d-b90deb5e` | `8220 BUN CABOOLTURE SLR PM SEM0002152 Total Energy - Active (Source) B1` |
| `p:bunnings:19c4ece1db1-7c845958` | `8220 BUN CABOOLTURE SLR PM SEM0002152 Total Energy - Reactive (Source) K1` |
| `p:bunnings:19c4c977981-db50da59` | `7349 BUN BATEMANS BAY SLR PM SEM0002125` |
| `p:bunnings:19c4ccf75ac-5c09fd71` | `7349 BUN BATEMANS BAY SLR PM SEM0002125 Total Energy - Active (Source) B1` |
| `p:bunnings:19c4ccf75c1-8f262f80` | `7349 BUN BATEMANS BAY SLR PM SEM0002125 Total Energy - Reactive (Source) K1` |

**Reference vertices (NOT dropped, only addressed by ID/label):**

| Purpose | Vertex ID | Label |
|---|---|---|
| Target site for SEM0002152 | `p:bunnings:19c4c96af42-c66e535a` | `8220 BUN CABOOLTURE SLR` |
| Target Main for SEM0002152 | `p:bunnings:19c4c976f53-43d75377` | `8220 BUN CABOOLTURE SLR Main` |
| NSW region (for new SLR site) | `p:bunnings:19ba28300f8-e7faeb43` | `Bunnings - NSW` |
| Pambula weather station | `p:global:19ba28c7587-59100473` | `Pambula, New South Wales, Australia` |
| Existing Batemans Bay (for lat/long reference only) | `p:bunnings:19ba28b99f4-f19a6333` | `7349 BUN BATEMANS BAY` |

**Edge types to preserve across drop/recreate (all edges have zero properties):**

- `siteRef`: PM → site; Point → site
- `equipRef`: Point → PM
- `levelRef`: PM → Main
- `regionRef`: Site → Region
- `weatherStationRef`: Site → WeatherStation

**Vertex property inventory (all single-cardinality, verified):**

- Point properties: `active`, `elec`, `energy`, `gegPointType`, `nem12Id`, `point`, `sensor`, `unit`, `gegDataType`, `export`, `reactive` (some only on K1 points)
- PM properties: `elec`, `equip`, `gateMeter`, `gegEquipType`, `meter`, `gegNabersInclusionPercent`, `dev`
- Site properties: `long`, `lat`, `area`, `armsProj`, `armsProjectId`, `geoCountry`, `site`, `tz`, `geoAddress`, `projId`, `observesHolidays`
- Main properties: `level`

---

## File Structure

**Create:**

- `sbm/sbm-ingester/scripts/migrate_pm_reassignment.py` - Main migration script with phase dispatcher, data model constants, Gremlin query builders, and per-phase execution functions. Single file because this is a one-off migration tightly tied to one specific data-model correction.
- `sbm/sbm-ingester/tests/unit/test_migrate_pm_reassignment.py` - Unit tests for pure query-builder functions and backup-restore roundtrip logic.
- `sbm/sbm-ingester/data/migration-backups/.gitkeep` - Backup JSON and restore Gremlin will land here (gitignored content, keep folder).

**Modify:**

- `sbm/sbm-ingester/.gitignore` - Add `data/migration-backups/*.json` and `data/migration-backups/*.gremlin` (keep `.gitkeep`).

**Not modified (key point — confirms IDs stay stable):**

- `sbm/sbm-ingester/docs/nem12_mappings_latest.json` - Unchanged
- `sbm/meter-importer/data/nem12_mappings_latest.json` - Unchanged

---

## Task 1: Scaffold the migration script with constants

**Files:**
- Create: `sbm/sbm-ingester/scripts/migrate_pm_reassignment.py`

- [ ] **Step 1: Create the module with docstring, imports, and data-model constants**

Write the file with exactly these contents:

```python
"""One-off migration: reassign two Bunnings PM clusters to their correct sites.

This script corrects a data-model misassignment in Neptune where:
- PM SEM0002152 (currently labeled under 7213 BUN KIRRAWEE) physically belongs to 8220 BUN CABOOLTURE SLR.
- PM SEM0002125 (currently labeled under 8220 BUN CABOOLTURE SLR) physically belongs to 7349 BUN BATEMANS BAY SLR.

Key invariants:
- All 6 affected vertex IDs stay identical (user-supplied IDs in addV).
  This means Hudi `sensorid`, nem12_mappings_latest.json values, and any other
  ID-keyed references remain valid.
- Only Gremlin vertex labels and edges change.
- Phase 0 produces a timestamped JSON backup and a restore.gremlin script
  that can re-create the pre-migration topology if anything goes wrong.

Usage:
    PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 0
    PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 1a
    PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 1b
    PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 2
    PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 3
    PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 4
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Allow "from billing_neptune_helper import ..." when run from repo root
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

from billing_neptune_helper import gremlin_query  # noqa: E402

# ---------------------------------------------------------------------------
# Backup output directory
# ---------------------------------------------------------------------------

BACKUP_DIR = SCRIPT_DIR.parent / "data" / "migration-backups"


# ---------------------------------------------------------------------------
# Vertex IDs (verified against Neptune on 2026-04-16)
# ---------------------------------------------------------------------------

# PM cluster A: SEM0002152 currently at 7213 BUN KIRRAWEE, moving to 8220 BUN CABOOLTURE SLR
PM_152_ID = "p:bunnings:19c4ece1aef-b978c70b"
POINT_152_B1_ID = "p:bunnings:19c4ece1d9d-b90deb5e"
POINT_152_K1_ID = "p:bunnings:19c4ece1db1-7c845958"

# PM cluster B: SEM0002125 currently at 8220 BUN CABOOLTURE SLR, moving to 7349 BUN BATEMANS BAY SLR
PM_125_ID = "p:bunnings:19c4c977981-db50da59"
POINT_125_B1_ID = "p:bunnings:19c4ccf75ac-5c09fd71"
POINT_125_K1_ID = "p:bunnings:19c4ccf75c1-8f262f80"

CLUSTER_A_IDS = [PM_152_ID, POINT_152_B1_ID, POINT_152_K1_ID]
CLUSTER_B_IDS = [PM_125_ID, POINT_125_B1_ID, POINT_125_K1_ID]
ALL_AFFECTED_IDS = CLUSTER_A_IDS + CLUSTER_B_IDS

# Existing reference vertices (never dropped)
CABOOLTURE_SLR_SITE_ID = "p:bunnings:19c4c96af42-c66e535a"
CABOOLTURE_SLR_MAIN_ID = "p:bunnings:19c4c976f53-43d75377"
KIRRAWEE_SITE_ID = "p:bunnings:19ba28b9e22-a29b1c01"
KIRRAWEE_MAIN_ID = "p:bunnings:19ba28fb241-c8b2d012"
NSW_REGION_ID = "p:bunnings:19ba28300f8-e7faeb43"
PAMBULA_WEATHER_ID = "p:global:19ba28c7587-59100473"
BATEMANS_BAY_EXISTING_ID = "p:bunnings:19ba28b99f4-f19a6333"  # non-SLR, reference for lat/long

# New vertices created in Phase 1b (client-supplied deterministic IDs so re-runs are idempotent)
BATEMANS_BAY_SLR_SITE_ID = "p:bunnings:migration-20260416-bb-slr-site"
BATEMANS_BAY_SLR_MAIN_ID = "p:bunnings:migration-20260416-bb-slr-main"


# ---------------------------------------------------------------------------
# Target labels (the point of this migration)
# ---------------------------------------------------------------------------

TARGET_LABELS: dict[str, str] = {
    # Cluster A: SEM0002152 → 8220 BUN CABOOLTURE SLR
    PM_152_ID: "8220 BUN CABOOLTURE SLR PM SEM0002152",
    POINT_152_B1_ID: "8220 BUN CABOOLTURE SLR PM SEM0002152 Total Energy - Active (Source) B1",
    POINT_152_K1_ID: "8220 BUN CABOOLTURE SLR PM SEM0002152 Total Energy - Reactive (Source) K1",
    # Cluster B: SEM0002125 → 7349 BUN BATEMANS BAY SLR
    PM_125_ID: "7349 BUN BATEMANS BAY SLR PM SEM0002125",
    POINT_125_B1_ID: "7349 BUN BATEMANS BAY SLR PM SEM0002125 Total Energy - Active (Source) B1",
    POINT_125_K1_ID: "7349 BUN BATEMANS BAY SLR PM SEM0002125 Total Energy - Reactive (Source) K1",
}


# ---------------------------------------------------------------------------
# Post-migration edge targets (what each vertex's outgoing edges should be)
# ---------------------------------------------------------------------------

# For each affected vertex, the (edge_label, target_vertex_id) pairs it should have after migration.
# Edges have no properties (verified in inventory).
TARGET_OUT_EDGES: dict[str, list[tuple[str, str]]] = {
    # Cluster A targets: siteRef/levelRef -> Caboolture SLR Main, equipRef to new PM
    PM_152_ID: [
        ("siteRef", CABOOLTURE_SLR_SITE_ID),
        ("levelRef", CABOOLTURE_SLR_MAIN_ID),
    ],
    POINT_152_B1_ID: [
        ("siteRef", CABOOLTURE_SLR_SITE_ID),
        ("equipRef", PM_152_ID),
    ],
    POINT_152_K1_ID: [
        ("siteRef", CABOOLTURE_SLR_SITE_ID),
        ("equipRef", PM_152_ID),
    ],
    # Cluster B targets: siteRef/levelRef -> new Batemans Bay SLR Main, equipRef to new PM
    PM_125_ID: [
        ("siteRef", BATEMANS_BAY_SLR_SITE_ID),
        ("levelRef", BATEMANS_BAY_SLR_MAIN_ID),
    ],
    POINT_125_B1_ID: [
        ("siteRef", BATEMANS_BAY_SLR_SITE_ID),
        ("equipRef", PM_125_ID),
    ],
    POINT_125_K1_ID: [
        ("siteRef", BATEMANS_BAY_SLR_SITE_ID),
        ("equipRef", PM_125_ID),
    ],
}


# ---------------------------------------------------------------------------
# Property payloads for new vertices created in Phase 1b
# ---------------------------------------------------------------------------

# Copied from 8220 BUN CABOOLTURE SLR template, with Batemans-Bay-specific overrides.
BATEMANS_BAY_SLR_SITE_PROPS: dict[str, Any] = {
    "area": 0,
    "armsProj": "N/A",
    "armsProjectId": 0,
    "geoCountry": "AU",
    "site": True,
    "observesHolidays": True,
    "projId": "bunnings",
    "tz": "Sydney",                                    # NSW
    "long": 149.8645,                                  # from 7349 BUN BATEMANS BAY
    "lat": -36.9446,                                   # from 7349 BUN BATEMANS BAY
    "geoAddress": "32 TO 34 PRINCES HWY  BATEMANS BAY  2536",  # user-provided
}

BATEMANS_BAY_SLR_MAIN_PROPS: dict[str, Any] = {
    "level": True,
}


# ---------------------------------------------------------------------------
# Phase 1a: corrected geoAddress for existing Caboolture SLR site
# ---------------------------------------------------------------------------

CABOOLTURE_SLR_NEW_GEOADDRESS = "459 PUMICESTONE RD  CABOOLTURE  4510"


# ---------------------------------------------------------------------------
# CLI entry point (phase dispatcher implemented in later tasks)
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Neptune PM reassignment migration.")
    parser.add_argument(
        "--phase",
        required=True,
        choices=["0", "1a", "1b", "2", "3", "4"],
        help="Which migration phase to run.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the Gremlin queries that would run without executing them.",
    )
    args = parser.parse_args(argv)

    # Phase functions are added in later tasks.
    raise NotImplementedError(f"Phase {args.phase} not implemented yet")


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Verify the file parses and constants are accessible**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run python -c "from scripts.migrate_pm_reassignment import ALL_AFFECTED_IDS, TARGET_LABELS; print(len(ALL_AFFECTED_IDS), len(TARGET_LABELS))"
```
Expected output: `6 6`

- [ ] **Step 3: Create the migration-backups directory with .gitkeep**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
mkdir -p data/migration-backups
touch data/migration-backups/.gitkeep
```

- [ ] **Step 4: Add backup output to .gitignore**

Append to `sbm/sbm-ingester/.gitignore`:
```
data/migration-backups/*.json
data/migration-backups/*.gremlin
!data/migration-backups/.gitkeep
```

- [ ] **Step 5: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
git add scripts/migrate_pm_reassignment.py data/migration-backups/.gitkeep .gitignore
git commit -m "feat: scaffold neptune PM reassignment migration script"
```

---

## Task 2: Pure-function Gremlin query builders

**Files:**
- Modify: `sbm/sbm-ingester/scripts/migrate_pm_reassignment.py` (add functions below the constants)
- Create: `sbm/sbm-ingester/tests/unit/test_migrate_pm_reassignment.py`

- [ ] **Step 1: Write the failing tests for query builders**

Create `sbm/sbm-ingester/tests/unit/test_migrate_pm_reassignment.py`:

```python
"""Unit tests for migrate_pm_reassignment query builders (no Neptune required)."""

from __future__ import annotations

from scripts.migrate_pm_reassignment import (
    build_addv_query,
    build_drop_query,
    build_gremlin_literal,
    build_property_update_query,
)


class TestBuildGremlinLiteral:
    def test_string_value_gets_quoted_and_escaped(self):
        assert build_gremlin_literal("hello") == "'hello'"
        assert build_gremlin_literal("it's") == "'it\\'s'"

    def test_bool_values(self):
        assert build_gremlin_literal(True) == "true"
        assert build_gremlin_literal(False) == "false"

    def test_int_and_float(self):
        assert build_gremlin_literal(42) == "42"
        assert build_gremlin_literal(-36.9446) == "-36.9446"

    def test_none_raises(self):
        import pytest
        with pytest.raises(ValueError):
            build_gremlin_literal(None)


class TestBuildDropQuery:
    def test_single_id(self):
        q = build_drop_query(["id-1"])
        assert q == "g.V('id-1').drop().iterate()"

    def test_multiple_ids(self):
        q = build_drop_query(["id-1", "id-2", "id-3"])
        assert q == "g.V('id-1','id-2','id-3').drop().iterate()"


class TestBuildAddvQuery:
    def test_vertex_with_props_no_edges(self):
        q = build_addv_query(
            vertex_id="v-1",
            label="MyLabel",
            properties={"key1": "val1", "key2": True, "key3": 42},
            out_edges=[],
        )
        assert q == (
            "g.addV('MyLabel').property(id, 'v-1')"
            ".property(single, 'key1', 'val1')"
            ".property(single, 'key2', true)"
            ".property(single, 'key3', 42)"
            ".iterate()"
        )

    def test_vertex_with_edges(self):
        q = build_addv_query(
            vertex_id="v-1",
            label="MyLabel",
            properties={"k": "v"},
            out_edges=[("siteRef", "site-id")],
        )
        assert q == (
            "g.addV('MyLabel').property(id, 'v-1')"
            ".property(single, 'k', 'v')"
            ".as('new_vertex')"
            ".V('site-id').addE('siteRef').from('new_vertex')"
            ".iterate()"
        )

    def test_multiple_edges(self):
        q = build_addv_query(
            vertex_id="v-1",
            label="L",
            properties={},
            out_edges=[("siteRef", "s"), ("equipRef", "e"), ("levelRef", "m")],
        )
        # Each edge step should reference new_vertex as the source
        assert ".addE('siteRef').from('new_vertex')" in q
        assert ".addE('equipRef').from('new_vertex')" in q
        assert ".addE('levelRef').from('new_vertex')" in q


class TestBuildPropertyUpdateQuery:
    def test_single_property_update(self):
        q = build_property_update_query("v-1", "geoAddress", "459 PUMICESTONE RD")
        assert q == (
            "g.V('v-1').property(single, 'geoAddress', '459 PUMICESTONE RD').iterate()"
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run pytest tests/unit/test_migrate_pm_reassignment.py -v
```
Expected: FAIL with `ImportError: cannot import name 'build_gremlin_literal'` (and the other builders).

- [ ] **Step 3: Add the query builder functions to `migrate_pm_reassignment.py`**

Append below the existing constants and before the `main()` function:

```python
# ---------------------------------------------------------------------------
# Gremlin query builders (pure functions, unit-tested)
# ---------------------------------------------------------------------------

def build_gremlin_literal(value: Any) -> str:
    """Render a Python value as a valid Gremlin-Groovy literal.

    Supports str, bool, int, float. Raises on None (no nullable props in our model).
    """
    if value is None:
        raise ValueError("Null values are not allowed; caller must omit absent props.")
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    raise TypeError(f"Unsupported literal type: {type(value).__name__}")


def build_drop_query(vertex_ids: list[str]) -> str:
    """Build a Gremlin query that drops the given vertices (and all their edges)."""
    id_args = ",".join(build_gremlin_literal(vid) for vid in vertex_ids)
    return f"g.V({id_args}).drop().iterate()"


def build_addv_query(
    vertex_id: str,
    label: str,
    properties: dict[str, Any],
    out_edges: list[tuple[str, str]],
) -> str:
    """Build a Gremlin query that adds a vertex with user-supplied ID, properties, and outgoing edges.

    All property writes use single cardinality to avoid set-append surprises.
    Every edge starts from the just-added vertex (aliased as `new_vertex`).
    """
    label_lit = build_gremlin_literal(label)
    id_lit = build_gremlin_literal(vertex_id)

    parts = [f"g.addV({label_lit}).property(id, {id_lit})"]
    for key, val in properties.items():
        key_lit = build_gremlin_literal(key)
        val_lit = build_gremlin_literal(val)
        parts.append(f".property(single, {key_lit}, {val_lit})")

    if out_edges:
        parts.append(".as('new_vertex')")
        for edge_label, target_id in out_edges:
            edge_label_lit = build_gremlin_literal(edge_label)
            target_lit = build_gremlin_literal(target_id)
            parts.append(
                f".V({target_lit}).addE({edge_label_lit}).from('new_vertex')"
            )

    parts.append(".iterate()")
    return "".join(parts)


def build_property_update_query(vertex_id: str, property_name: str, new_value: Any) -> str:
    """Build a Gremlin query to overwrite a single-cardinality property on an existing vertex."""
    id_lit = build_gremlin_literal(vertex_id)
    name_lit = build_gremlin_literal(property_name)
    val_lit = build_gremlin_literal(new_value)
    return f"g.V({id_lit}).property(single, {name_lit}, {val_lit}).iterate()"
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run pytest tests/unit/test_migrate_pm_reassignment.py -v
```
Expected: 10 tests pass (4 literal + 2 drop + 3 addv + 1 property update).

- [ ] **Step 5: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
git add scripts/migrate_pm_reassignment.py tests/unit/test_migrate_pm_reassignment.py
git commit -m "feat: add gremlin query builders with unit tests for migration script"
```

---

## Task 3: Phase 0 — dump live vertex state to JSON backup

**Files:**
- Modify: `sbm/sbm-ingester/scripts/migrate_pm_reassignment.py` (add `dump_backup()` and wire Phase 0)

- [ ] **Step 1: Add backup-dump helpers below the query builders**

Append below the query builders:

```python
# ---------------------------------------------------------------------------
# Phase 0: live-state backup + restore.gremlin generation
# ---------------------------------------------------------------------------

def _fetch_vertex_full_state(vertex_id: str) -> dict[str, Any]:
    """Query Neptune for a vertex's properties and ALL of its in/out edges.

    Returns a dict shaped like:
      {
        "id": "<vertex_id>",
        "label": "<label>",
        "properties": { "key": [value, ...], ... },   # valueMap() style lists
        "out_edges": [{"label": "...", "target_id": "...", "target_label": "..."}],
        "in_edges":  [{"label": "...", "source_id": "...", "source_label": "..."}],
      }

    Raises RuntimeError if the vertex does not exist.
    """
    id_lit = build_gremlin_literal(vertex_id)
    query = (
        f"g.V({id_lit}).project('id','label','props','outE','inE')"
        ".by(T.id).by(T.label).by(valueMap())"
        ".by(__.outE().project('label','target_id','target_label')"
        ".by(label).by(inV().id()).by(inV().label()).fold())"
        ".by(__.inE().project('label','source_id','source_label')"
        ".by(label).by(outV().id()).by(outV().label()).fold())"
        ".toList()"
    )
    result = gremlin_query(query)
    if not result:
        raise RuntimeError(f"Vertex not found in Neptune: {vertex_id}")
    return result[0]


def dump_backup() -> Path:
    """Dump the 6 affected vertices to a timestamped JSON file.

    Returns the backup file path.
    """
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_path = BACKUP_DIR / f"backup-{timestamp}.json"

    dump: dict[str, Any] = {
        "created_at_utc": timestamp,
        "vertex_count": len(ALL_AFFECTED_IDS),
        "vertices": {},
    }
    for vid in ALL_AFFECTED_IDS:
        print(f"  dumping {vid}...")
        dump["vertices"][vid] = _fetch_vertex_full_state(vid)

    backup_path.write_text(json.dumps(dump, indent=2, default=str))
    print(f"Backup written to: {backup_path}")
    return backup_path
```

- [ ] **Step 2: Wire a partial Phase 0 (backup only) into `main()`**

Replace the current body of `main()` with (note: `write_restore_script` is NOT called yet — that wiring is added in Task 4 so this task compiles and runs on its own):

```python
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Neptune PM reassignment migration.")
    parser.add_argument(
        "--phase",
        required=True,
        choices=["0", "1a", "1b", "2", "3", "4"],
        help="Which migration phase to run.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the Gremlin queries that would run without executing them.",
    )
    args = parser.parse_args(argv)

    if args.phase == "0":
        dump_backup()
        print("(Restore script generation will be added in the next step.)")
        return 0

    raise NotImplementedError(f"Phase {args.phase} not implemented yet")
```

- [ ] **Step 3: Sanity-run Phase 0 backup only**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 0
```
Expected: 6 `dumping ...` lines, one `Backup written to: ...` line, then the note about restore generation being pending.

- [ ] **Step 4: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
git add scripts/migrate_pm_reassignment.py
git commit -m "feat: add phase 0 backup dump helper"
```

---

## Task 4: Phase 0 — generate restore.gremlin from backup

**Files:**
- Modify: `sbm/sbm-ingester/scripts/migrate_pm_reassignment.py`
- Modify: `sbm/sbm-ingester/tests/unit/test_migrate_pm_reassignment.py`

- [ ] **Step 1: Write the failing test**

Append to `test_migrate_pm_reassignment.py`:

```python
class TestWriteRestoreScript:
    def test_restore_script_content(self, tmp_path):
        from scripts.migrate_pm_reassignment import (
            BACKUP_DIR,
            write_restore_script,
        )
        import json
        import scripts.migrate_pm_reassignment as module

        # Point BACKUP_DIR at tmp_path for this test
        monkey_backup = tmp_path
        module.BACKUP_DIR = monkey_backup

        backup_data = {
            "created_at_utc": "20260416T000000Z",
            "vertex_count": 1,
            "vertices": {
                "v-1": {
                    "id": "v-1",
                    "label": "OLD_LABEL",
                    "properties": {"key1": ["val1"], "key2": [True]},
                    "out_edges": [
                        {"label": "siteRef", "target_id": "s-1", "target_label": "Site1"},
                    ],
                    "in_edges": [],
                }
            },
        }
        backup_path = tmp_path / "backup-test.json"
        backup_path.write_text(json.dumps(backup_data))

        restore_path = write_restore_script(backup_path)

        content = restore_path.read_text()
        # Drop comes first
        assert "g.V('v-1').drop().iterate()" in content
        # Then re-create with original label and id
        assert "g.addV('OLD_LABEL').property(id, 'v-1')" in content
        assert ".property(single, 'key1', 'val1')" in content
        assert ".property(single, 'key2', true)" in content
        # Then re-create the outgoing edge
        assert ".V('s-1').addE('siteRef').from('new_vertex')" in content
        # Drop section must appear before recreate section
        assert content.index("drop()") < content.index("addV")
```

- [ ] **Step 2: Run to confirm failure**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run pytest tests/unit/test_migrate_pm_reassignment.py::TestWriteRestoreScript -v
```
Expected: FAIL with `ImportError: cannot import name 'write_restore_script'`.

- [ ] **Step 3: Implement `write_restore_script` in `migrate_pm_reassignment.py`**

Add below `dump_backup()`:

```python
def write_restore_script(backup_path: Path) -> Path:
    """Generate a restore.gremlin script that can rebuild the pre-migration topology.

    The script does two sections:
      1. DROP: drops all backed-up vertex IDs.
      2. RECREATE: re-creates each vertex with its original label, properties,
         and outgoing edges. (Incoming edges are restored implicitly when the
         other endpoints are recreated, because this backup covers the full
         cluster. In-edges whose source is NOT in the backup still work
         because those sources never moved.)

    Returns the restore script path.
    """
    backup = json.loads(backup_path.read_text())
    vertices: dict[str, dict[str, Any]] = backup["vertices"]

    lines: list[str] = [
        f"// Restore script generated from {backup_path.name}",
        f"// Created at UTC: {backup['created_at_utc']}",
        "// Execute each non-comment, non-empty line via gemsNeptuneExplorer.",
        "// See the plan's 'Rollback Procedure' section for the shell loop.",
        "",
        "// --- Section 1: DROP backed-up vertices ---",
    ]
    drop_ids = list(vertices.keys())
    lines.append(build_drop_query(drop_ids))
    lines.append("")
    lines.append("// --- Section 2: RECREATE each vertex with original state ---")

    for vid, v in vertices.items():
        # Flatten valueMap lists (single-cardinality => 1-element list)
        props_flat: dict[str, Any] = {}
        for k, v_list in v["properties"].items():
            if not isinstance(v_list, list) or len(v_list) != 1:
                raise RuntimeError(
                    f"Unexpected multi-value property {k} on {vid}: {v_list!r}"
                )
            props_flat[k] = v_list[0]
        out_edges = [(e["label"], e["target_id"]) for e in v["out_edges"]]
        lines.append(
            build_addv_query(
                vertex_id=vid,
                label=v["label"],
                properties=props_flat,
                out_edges=out_edges,
            )
        )

    restore_path = backup_path.with_suffix(".restore.gremlin")
    restore_path.write_text("\n".join(lines) + "\n")
    return restore_path
```

- [ ] **Step 4: Run tests to verify pass**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run pytest tests/unit/test_migrate_pm_reassignment.py -v
```
Expected: all tests pass (11 total).

- [ ] **Step 5: Wire `write_restore_script` into Phase 0 handler**

Replace the existing phase 0 block in `main()` with:

```python
    if args.phase == "0":
        backup_path = dump_backup()
        restore_path = write_restore_script(backup_path)
        print(f"Restore script written to: {restore_path}")
        return 0
```

- [ ] **Step 6: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
git add scripts/migrate_pm_reassignment.py tests/unit/test_migrate_pm_reassignment.py
git commit -m "feat: generate restore.gremlin script from backup JSON"
```

---

## Task 5: Execute Phase 0 against live Neptune & spot-check output

**Files:** none modified

- [ ] **Step 1: Run Phase 0**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 0
```
Expected stdout:
```
  dumping p:bunnings:19c4ece1aef-b978c70b...
  dumping p:bunnings:19c4ece1d9d-b90deb5e...
  dumping p:bunnings:19c4ece1db1-7c845958...
  dumping p:bunnings:19c4c977981-db50da59...
  dumping p:bunnings:19c4ccf75ac-5c09fd71...
  dumping p:bunnings:19c4ccf75c1-8f262f80...
Backup written to: .../data/migration-backups/backup-<timestamp>.json
Restore script written to: .../data/migration-backups/backup-<timestamp>.restore.gremlin
```

- [ ] **Step 2: Verify the backup JSON has the expected shape**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
LATEST=$(ls -t data/migration-backups/backup-*.json | head -1)
PYTHONPATH=src uv run python - <<PY
import json, sys
d = json.loads(open("$LATEST").read())
assert d["vertex_count"] == 6, d["vertex_count"]
expected_ids = {
    "p:bunnings:19c4ece1aef-b978c70b",
    "p:bunnings:19c4ece1d9d-b90deb5e",
    "p:bunnings:19c4ece1db1-7c845958",
    "p:bunnings:19c4c977981-db50da59",
    "p:bunnings:19c4ccf75ac-5c09fd71",
    "p:bunnings:19c4ccf75c1-8f262f80",
}
assert set(d["vertices"].keys()) == expected_ids
for vid, v in d["vertices"].items():
    assert "label" in v and v["label"]
    assert "properties" in v and v["properties"]
    assert "out_edges" in v and len(v["out_edges"]) >= 2
print("Backup JSON shape OK.")
PY
```
Expected: `Backup JSON shape OK.`

- [ ] **Step 3: Confirm restore script has DROP before RECREATE and covers all 6 vertices**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
LATEST=$(ls -t data/migration-backups/backup-*.restore.gremlin | head -1)
grep -c "g.V(" "$LATEST"
grep -c "g.addV(" "$LATEST"
```
Expected: first count is `1` (one drop line covering all), second count is `6` (one addV per vertex).

- [ ] **Step 4: Human review of the backup**

Open `data/migration-backups/backup-<timestamp>.json` and confirm for each vertex that `label` and `properties` match what Neptune actually contains (cross-check with earlier manual queries). This is the "last chance to spot surprises" checkpoint before destructive phases.

Acceptance criteria (informal):
- 4 point vertices, each with `siteRef` + `equipRef` out-edges
- 2 PM vertices, each with `siteRef` + `levelRef` out-edges and 2 `equipRef` in-edges
- No unexpected edge labels

- [ ] **Step 5: Commit the backup** (NOT the content, only the gitkeep if it changed)

No commit needed — backup JSON and restore.gremlin are gitignored.

---

## Task 6: Phase 1a — correct Caboolture SLR geoAddress in place

**Files:**
- Modify: `sbm/sbm-ingester/scripts/migrate_pm_reassignment.py` (add `run_phase_1a()` and wire it)

- [ ] **Step 1: Add the phase 1a function**

Append below the backup helpers:

```python
# ---------------------------------------------------------------------------
# Phase 1a: correct geoAddress on existing 8220 BUN CABOOLTURE SLR site
# ---------------------------------------------------------------------------

def run_phase_1a(dry_run: bool = False) -> None:
    query = build_property_update_query(
        vertex_id=CABOOLTURE_SLR_SITE_ID,
        property_name="geoAddress",
        new_value=CABOOLTURE_SLR_NEW_GEOADDRESS,
    )
    print(f"[phase 1a] Query: {query}")
    if dry_run:
        print("[phase 1a] DRY RUN: not executing.")
        return
    gremlin_query(query)

    # Verify
    verify_query = (
        f"g.V({build_gremlin_literal(CABOOLTURE_SLR_SITE_ID)})"
        ".values('geoAddress').toList()"
    )
    addrs = gremlin_query(verify_query)
    assert addrs == [CABOOLTURE_SLR_NEW_GEOADDRESS], f"Unexpected addresses: {addrs}"
    print(f"[phase 1a] OK. geoAddress now: {addrs[0]!r}")
```

- [ ] **Step 2: Wire Phase 1a into the dispatcher**

Replace the `if args.phase == "0":` block in `main()` with:

```python
    if args.phase == "0":
        backup_path = dump_backup()
        restore_path = write_restore_script(backup_path)
        print(f"Restore script written to: {restore_path}")
        return 0

    if args.phase == "1a":
        run_phase_1a(dry_run=args.dry_run)
        return 0

    raise NotImplementedError(f"Phase {args.phase} not implemented yet")
```

- [ ] **Step 3: Dry-run Phase 1a**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 1a --dry-run
```
Expected stdout contains:
```
[phase 1a] Query: g.V('p:bunnings:19c4c96af42-c66e535a').property(single, 'geoAddress', '459 PUMICESTONE RD  CABOOLTURE  4510').iterate()
[phase 1a] DRY RUN: not executing.
```

- [ ] **Step 4: Execute Phase 1a**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 1a
```
Expected stdout ends with:
```
[phase 1a] OK. geoAddress now: '459 PUMICESTONE RD  CABOOLTURE  4510'
```

- [ ] **Step 5: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
git add scripts/migrate_pm_reassignment.py
git commit -m "feat: phase 1a - correct caboolture SLR geoAddress"
```

---

## Task 7: Phase 1b — create Batemans Bay SLR site + Main

**Files:**
- Modify: `sbm/sbm-ingester/scripts/migrate_pm_reassignment.py`

- [ ] **Step 1: Add the phase 1b function**

Append below `run_phase_1a`:

```python
# ---------------------------------------------------------------------------
# Phase 1b: create 7349 BUN BATEMANS BAY SLR site and its Main vertex
# ---------------------------------------------------------------------------

def _vertex_exists(vertex_id: str) -> bool:
    id_lit = build_gremlin_literal(vertex_id)
    result = gremlin_query(f"g.V({id_lit}).count().next()")
    # parse_graphson unwraps g:Int64 to int
    if isinstance(result, list):
        count = result[0] if result else 0
    else:
        count = result
    return int(count) > 0


def run_phase_1b(dry_run: bool = False) -> None:
    # Idempotency: if site already exists, skip.
    if _vertex_exists(BATEMANS_BAY_SLR_SITE_ID):
        print(
            f"[phase 1b] site {BATEMANS_BAY_SLR_SITE_ID} already exists, skipping."
        )
    else:
        site_query = build_addv_query(
            vertex_id=BATEMANS_BAY_SLR_SITE_ID,
            label="7349 BUN BATEMANS BAY SLR",
            properties=BATEMANS_BAY_SLR_SITE_PROPS,
            out_edges=[
                ("regionRef", NSW_REGION_ID),
                ("weatherStationRef", PAMBULA_WEATHER_ID),
            ],
        )
        print(f"[phase 1b] creating site:\n  {site_query}")
        if not dry_run:
            gremlin_query(site_query)

    if _vertex_exists(BATEMANS_BAY_SLR_MAIN_ID):
        print(
            f"[phase 1b] main {BATEMANS_BAY_SLR_MAIN_ID} already exists, skipping."
        )
    else:
        main_query = build_addv_query(
            vertex_id=BATEMANS_BAY_SLR_MAIN_ID,
            label="7349 BUN BATEMANS BAY SLR Main",
            properties=BATEMANS_BAY_SLR_MAIN_PROPS,
            out_edges=[("siteRef", BATEMANS_BAY_SLR_SITE_ID)],
        )
        print(f"[phase 1b] creating main:\n  {main_query}")
        if not dry_run:
            gremlin_query(main_query)

    if dry_run:
        print("[phase 1b] DRY RUN: not executing.")
        return

    # Verify
    site_props = gremlin_query(
        f"g.V({build_gremlin_literal(BATEMANS_BAY_SLR_SITE_ID)}).valueMap().toList()"
    )
    assert site_props and site_props[0].get("geoAddress") == [
        "32 TO 34 PRINCES HWY  BATEMANS BAY  2536"
    ], f"Unexpected site state: {site_props}"

    main_count = gremlin_query(
        f"g.V({build_gremlin_literal(BATEMANS_BAY_SLR_MAIN_ID)})"
        ".outE('siteRef').inV()"
        f".hasId({build_gremlin_literal(BATEMANS_BAY_SLR_SITE_ID)}).count().next()"
    )
    count_val = main_count[0] if isinstance(main_count, list) else main_count
    assert int(count_val) == 1, f"Main->Site edge missing, got: {main_count}"
    print("[phase 1b] OK. Site and Main created, edges verified.")
```

- [ ] **Step 2: Wire Phase 1b into the dispatcher**

Add below the `if args.phase == "1a":` block in `main()`:

```python
    if args.phase == "1b":
        run_phase_1b(dry_run=args.dry_run)
        return 0
```

- [ ] **Step 3: Dry-run Phase 1b**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 1b --dry-run
```
Expected stdout contains:
- Site creation query with `7349 BUN BATEMANS BAY SLR` label and `geoAddress`
- Main creation query with `level` property and `siteRef` edge
- `DRY RUN: not executing.`

- [ ] **Step 4: Execute Phase 1b**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 1b
```
Expected stdout ends with:
```
[phase 1b] OK. Site and Main created, edges verified.
```

- [ ] **Step 5: Re-run Phase 1b to verify idempotency**

Run the same command again:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 1b
```
Expected: both site and main log `already exists, skipping.` No creation queries.

- [ ] **Step 6: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
git add scripts/migrate_pm_reassignment.py
git commit -m "feat: phase 1b - create batemans bay SLR site and main"
```

---

## Task 8: Phase 2 — migrate SEM0002152 cluster to Caboolture SLR

**Files:**
- Modify: `sbm/sbm-ingester/scripts/migrate_pm_reassignment.py`

- [ ] **Step 1: Add a shared cluster-migrate helper and Phase 2 wrapper**

Append below `run_phase_1b`:

```python
# ---------------------------------------------------------------------------
# Shared cluster migrator used by Phases 2 and 3
# ---------------------------------------------------------------------------

def _build_recreate_query_for_cluster(
    cluster_ids: list[str],
    property_snapshots: dict[str, dict[str, Any]],
) -> str:
    """Build one Gremlin query that recreates all 3 vertices in a cluster.

    Order: PM first (so points can attach equipRef to it), then each point.
    Each vertex uses user-supplied ID to preserve ID continuity with Hudi.

    property_snapshots maps vertex_id -> flat {key: value} dict.
    """
    parts: list[str] = []
    for vid in cluster_ids:
        label = TARGET_LABELS[vid]
        props = property_snapshots[vid]
        edges = TARGET_OUT_EDGES[vid]
        # Each vertex creation starts with g.addV(...) and ends with .iterate()
        # We concatenate them as sequential statements separated by ";" which
        # Neptune accepts for multi-statement scripts within one request.
        parts.append(build_addv_query(vid, label, props, edges))
    return ";".join(parts)


def _load_property_snapshots_from_backup(cluster_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Load the most recent backup and return flat {vid: {prop: value}} for the cluster."""
    backups = sorted(BACKUP_DIR.glob("backup-*.json"))
    if not backups:
        raise RuntimeError(
            "No backup JSON found. Run Phase 0 first: --phase 0"
        )
    latest = backups[-1]
    print(f"[migrator] using backup: {latest.name}")
    data = json.loads(latest.read_text())
    snapshots: dict[str, dict[str, Any]] = {}
    for vid in cluster_ids:
        v = data["vertices"][vid]
        flat: dict[str, Any] = {}
        for k, v_list in v["properties"].items():
            if len(v_list) != 1:
                raise RuntimeError(f"Multi-value property on {vid}: {k}={v_list}")
            flat[k] = v_list[0]
        snapshots[vid] = flat
    return snapshots


def _verify_cluster_migrated(
    cluster_ids: list[str],
    expected_site_id: str,
) -> None:
    """Assert every vertex in the cluster has the target label, target site, and preserved ID."""
    id_list = ",".join(build_gremlin_literal(v) for v in cluster_ids)
    query = (
        f"g.V({id_list}).project('id','label','site_id').by(T.id).by(T.label)"
        ".by(coalesce(out('siteRef').id(),constant('MISSING'))).toList()"
    )
    rows = gremlin_query(query)
    for row in rows:
        vid = row["id"]
        assert row["label"] == TARGET_LABELS[vid], (
            f"Label mismatch on {vid}: got {row['label']!r}, "
            f"expected {TARGET_LABELS[vid]!r}"
        )
        assert row["site_id"] == expected_site_id, (
            f"siteRef mismatch on {vid}: got {row['site_id']}, "
            f"expected {expected_site_id}"
        )
    assert len(rows) == len(cluster_ids), (
        f"Expected {len(cluster_ids)} vertices, got {len(rows)}"
    )


def _migrate_cluster(
    cluster_ids: list[str],
    expected_site_id: str,
    phase_label: str,
    dry_run: bool,
) -> None:
    snapshots = _load_property_snapshots_from_backup(cluster_ids)
    drop_q = build_drop_query(cluster_ids)
    recreate_q = _build_recreate_query_for_cluster(cluster_ids, snapshots)

    print(f"[{phase_label}] DROP query:\n  {drop_q}\n")
    print(f"[{phase_label}] RECREATE query ({len(cluster_ids)} vertices):")
    # Print each addV on its own line for readability
    for segment in recreate_q.split(";"):
        print(f"  {segment}")

    if dry_run:
        print(f"[{phase_label}] DRY RUN: not executing.")
        return

    print(f"[{phase_label}] executing DROP...")
    gremlin_query(drop_q)
    print(f"[{phase_label}] executing RECREATE...")
    gremlin_query(recreate_q)

    print(f"[{phase_label}] verifying...")
    _verify_cluster_migrated(cluster_ids, expected_site_id)
    print(f"[{phase_label}] OK. Cluster migrated, labels + siteRef verified.")


# ---------------------------------------------------------------------------
# Phase 2: SEM0002152 cluster -> 8220 BUN CABOOLTURE SLR
# ---------------------------------------------------------------------------

def run_phase_2(dry_run: bool = False) -> None:
    _migrate_cluster(
        cluster_ids=CLUSTER_A_IDS,
        expected_site_id=CABOOLTURE_SLR_SITE_ID,
        phase_label="phase 2",
        dry_run=dry_run,
    )
```

- [ ] **Step 2: Wire Phase 2 into dispatcher**

Add below the `if args.phase == "1b":` block:

```python
    if args.phase == "2":
        run_phase_2(dry_run=args.dry_run)
        return 0
```

- [ ] **Step 3: Dry-run Phase 2 and eyeball the generated queries**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 2 --dry-run
```
Expected checks in output:
- DROP query targets the 3 cluster-A IDs
- RECREATE has 3 `g.addV(...)` statements (PM first, then B1, then K1)
- Every addV has `.property(id, 'p:bunnings:...')` with the original ID
- PM's new label is `8220 BUN CABOOLTURE SLR PM SEM0002152`
- Point labels have the new site prefix
- PM's out-edges: `siteRef` → `p:bunnings:19c4c96af42-c66e535a`, `levelRef` → `p:bunnings:19c4c976f53-43d75377`
- Point out-edges: `siteRef` → Caboolture SLR, `equipRef` → `p:bunnings:19c4ece1aef-b978c70b`

- [ ] **Step 4: Execute Phase 2**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 2
```
Expected stdout ends with:
```
[phase 2] OK. Cluster migrated, labels + siteRef verified.
```

- [ ] **Step 5: Independent verification — query Hudi for SEM0002152 historical data by ID**

Run:
```bash
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) AS cnt FROM sensordata_default WHERE sensorid IN ('p:bunnings:19c4ece1d9d-b90deb5e','p:bunnings:19c4ece1db1-7c845958')" \
  --query-execution-context '{"Database":"default"}' \
  --result-configuration '{"OutputLocation":"s3://sbm-file-ingester/athena-results/"}' \
  --region ap-southeast-2
```
Then after a few seconds:
```bash
aws athena get-query-results --query-execution-id <id-from-above> --region ap-southeast-2 --query 'ResultSet.Rows[1].Data[0].VarCharValue'
```
Expected: `157824` (78,912 × 2 from the pre-migration count; unchanged).

- [ ] **Step 6: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
git add scripts/migrate_pm_reassignment.py
git commit -m "feat: phase 2 - migrate SEM0002152 cluster to caboolture SLR"
```

---

## Task 9: Phase 3 — migrate SEM0002125 cluster to Batemans Bay SLR

**Files:**
- Modify: `sbm/sbm-ingester/scripts/migrate_pm_reassignment.py`

- [ ] **Step 1: Add `run_phase_3`**

Append below `run_phase_2`:

```python
# ---------------------------------------------------------------------------
# Phase 3: SEM0002125 cluster -> 7349 BUN BATEMANS BAY SLR (new site)
# ---------------------------------------------------------------------------

def run_phase_3(dry_run: bool = False) -> None:
    _migrate_cluster(
        cluster_ids=CLUSTER_B_IDS,
        expected_site_id=BATEMANS_BAY_SLR_SITE_ID,
        phase_label="phase 3",
        dry_run=dry_run,
    )
```

- [ ] **Step 2: Wire Phase 3 into dispatcher**

Add below the `if args.phase == "2":` block:

```python
    if args.phase == "3":
        run_phase_3(dry_run=args.dry_run)
        return 0
```

- [ ] **Step 3: Dry-run Phase 3**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 3 --dry-run
```
Expected: DROP covers cluster-B IDs; RECREATE has 3 addV with new `7349 BUN BATEMANS BAY SLR` labels, siteRef → `BATEMANS_BAY_SLR_SITE_ID`, PM levelRef → `BATEMANS_BAY_SLR_MAIN_ID`.

- [ ] **Step 4: Execute Phase 3**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 3
```
Expected stdout ends with:
```
[phase 3] OK. Cluster migrated, labels + siteRef verified.
```

- [ ] **Step 5: Independent Hudi verification for SEM0002125 IDs**

```bash
aws athena start-query-execution \
  --query-string "SELECT sensorid, COUNT(*) AS cnt FROM sensordata_default WHERE sensorid IN ('p:bunnings:19c4ccf75ac-5c09fd71','p:bunnings:19c4ccf75c1-8f262f80') GROUP BY sensorid" \
  --query-execution-context '{"Database":"default"}' \
  --result-configuration '{"OutputLocation":"s3://sbm-file-ingester/athena-results/"}' \
  --region ap-southeast-2
```
Expected totals: 25,920 rows per sensorid (unchanged from pre-migration).

- [ ] **Step 6: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
git add scripts/migrate_pm_reassignment.py
git commit -m "feat: phase 3 - migrate SEM0002125 cluster to batemans bay SLR"
```

---

## Task 10: Phase 4 — terminal-state verification

**Files:**
- Modify: `sbm/sbm-ingester/scripts/migrate_pm_reassignment.py`

- [ ] **Step 1: Add `run_phase_4` which asserts every invariant**

Append below `run_phase_3`:

```python
# ---------------------------------------------------------------------------
# Phase 4: full-state terminal verification
# ---------------------------------------------------------------------------

def run_phase_4(dry_run: bool = False) -> None:
    if dry_run:
        print("[phase 4] DRY RUN: phase 4 is read-only, nothing to skip.")

    failures: list[str] = []

    # 1) Every affected vertex has the correct label and preserved ID.
    ids_lit = ",".join(build_gremlin_literal(v) for v in ALL_AFFECTED_IDS)
    rows = gremlin_query(
        f"g.V({ids_lit}).project('id','label').by(T.id).by(T.label).toList()"
    )
    if len(rows) != len(ALL_AFFECTED_IDS):
        failures.append(
            f"Expected {len(ALL_AFFECTED_IDS)} vertices, found {len(rows)}"
        )
    seen_ids = {r["id"] for r in rows}
    if seen_ids != set(ALL_AFFECTED_IDS):
        missing = set(ALL_AFFECTED_IDS) - seen_ids
        extra = seen_ids - set(ALL_AFFECTED_IDS)
        failures.append(f"ID mismatch — missing: {missing}, extra: {extra}")
    for r in rows:
        expected = TARGET_LABELS[r["id"]]
        if r["label"] != expected:
            failures.append(
                f"Label on {r['id']}: got {r['label']!r}, want {expected!r}"
            )

    # 2) Cluster A siteRef points to Caboolture SLR.
    for vid in CLUSTER_A_IDS:
        sites = gremlin_query(
            f"g.V({build_gremlin_literal(vid)}).out('siteRef').id().toList()"
        )
        if sites != [CABOOLTURE_SLR_SITE_ID]:
            failures.append(
                f"Cluster A {vid}: siteRef -> {sites}, want [{CABOOLTURE_SLR_SITE_ID!r}]"
            )

    # 3) Cluster B siteRef points to Batemans Bay SLR.
    for vid in CLUSTER_B_IDS:
        sites = gremlin_query(
            f"g.V({build_gremlin_literal(vid)}).out('siteRef').id().toList()"
        )
        if sites != [BATEMANS_BAY_SLR_SITE_ID]:
            failures.append(
                f"Cluster B {vid}: siteRef -> {sites}, want [{BATEMANS_BAY_SLR_SITE_ID!r}]"
            )

    # 4) PM levelRef correctness.
    level_a = gremlin_query(
        f"g.V({build_gremlin_literal(PM_152_ID)}).out('levelRef').id().toList()"
    )
    if level_a != [CABOOLTURE_SLR_MAIN_ID]:
        failures.append(f"PM_152 levelRef -> {level_a}, want [{CABOOLTURE_SLR_MAIN_ID!r}]")
    level_b = gremlin_query(
        f"g.V({build_gremlin_literal(PM_125_ID)}).out('levelRef').id().toList()"
    )
    if level_b != [BATEMANS_BAY_SLR_MAIN_ID]:
        failures.append(f"PM_125 levelRef -> {level_b}, want [{BATEMANS_BAY_SLR_MAIN_ID!r}]")

    # 5) Points' equipRef points to the recreated PM (IDs preserved).
    for vid in (POINT_152_B1_ID, POINT_152_K1_ID):
        eq = gremlin_query(
            f"g.V({build_gremlin_literal(vid)}).out('equipRef').id().toList()"
        )
        if eq != [PM_152_ID]:
            failures.append(f"{vid} equipRef -> {eq}, want [{PM_152_ID!r}]")
    for vid in (POINT_125_B1_ID, POINT_125_K1_ID):
        eq = gremlin_query(
            f"g.V({build_gremlin_literal(vid)}).out('equipRef').id().toList()"
        )
        if eq != [PM_125_ID]:
            failures.append(f"{vid} equipRef -> {eq}, want [{PM_125_ID!r}]")

    # 6) Caboolture SLR site now has corrected geoAddress.
    addr = gremlin_query(
        f"g.V({build_gremlin_literal(CABOOLTURE_SLR_SITE_ID)}).values('geoAddress').toList()"
    )
    if addr != [CABOOLTURE_SLR_NEW_GEOADDRESS]:
        failures.append(
            f"Caboolture SLR geoAddress -> {addr}, want [{CABOOLTURE_SLR_NEW_GEOADDRESS!r}]"
        )

    # 7) Batemans Bay SLR site + Main both exist with correct state.
    bb_state = gremlin_query(
        f"g.V({build_gremlin_literal(BATEMANS_BAY_SLR_SITE_ID)}).valueMap('geoAddress','tz').toList()"
    )
    if not bb_state or bb_state[0].get("geoAddress") != [
        "32 TO 34 PRINCES HWY  BATEMANS BAY  2536"
    ]:
        failures.append(f"Batemans Bay SLR site state -> {bb_state}")
    if not bb_state or bb_state[0].get("tz") != ["Sydney"]:
        failures.append(f"Batemans Bay SLR tz -> {bb_state}")

    # 8) Kirrawee sanity: SEM0002152 gone; SEM0002108 and 4103751370 PMs still present.
    kirrawee_neighbors = gremlin_query(
        f"g.V({build_gremlin_literal(KIRRAWEE_SITE_ID)}).in('siteRef').label().toList()"
    )
    if any("SEM0002152" in lbl for lbl in kirrawee_neighbors):
        failures.append(
            f"Kirrawee still has SEM0002152 reference: {kirrawee_neighbors}"
        )
    if not any("SEM0002108" in lbl for lbl in kirrawee_neighbors):
        failures.append(
            f"Kirrawee unexpectedly lost SEM0002108: {kirrawee_neighbors}"
        )
    if not any("4103751370" in lbl for lbl in kirrawee_neighbors):
        failures.append(
            f"Kirrawee unexpectedly lost PM 4103751370: {kirrawee_neighbors}"
        )

    if failures:
        print("[phase 4] ❌ FAILED:")
        for f in failures:
            print(f"  - {f}")
        raise SystemExit(1)

    print("[phase 4] ✅ ALL CHECKS PASSED")
    print("  - 6 affected vertex IDs preserved")
    print("  - all 6 labels match target")
    print("  - siteRef / equipRef / levelRef all retargeted correctly")
    print("  - caboolture SLR geoAddress corrected")
    print("  - batemans bay SLR site and main created")
    print("  - kirrawee lost SEM0002152 but kept SEM0002108 and 4103751370")
```

- [ ] **Step 2: Wire Phase 4 into dispatcher**

Add below the `if args.phase == "3":` block:

```python
    if args.phase == "4":
        run_phase_4(dry_run=args.dry_run)
        return 0
```

- [ ] **Step 3: Run Phase 4**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run scripts/migrate_pm_reassignment.py --phase 4
```
Expected stdout ends with:
```
[phase 4] ✅ ALL CHECKS PASSED
  - 6 affected vertex IDs preserved
  - all 6 labels match target
  - siteRef / equipRef / levelRef all retargeted correctly
  - caboolture SLR geoAddress corrected
  - batemans bay SLR site and main created
  - kirrawee lost SEM0002152 but kept SEM0002108 and 4103751370
```

- [ ] **Step 4: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
git add scripts/migrate_pm_reassignment.py
git commit -m "feat: phase 4 - terminal-state verification"
```

- [ ] **Step 5: Final mapping-JSON sanity check (read-only, no commit needed)**

Confirm the two JSON files still have unchanged point IDs:
```bash
grep "SEM0002125\|SEM0002152" /Users/zeyu/Desktop/GEG/sbm/meter-importer/data/nem12_mappings_latest.json
grep "SEM0002125\|SEM0002152" /Users/zeyu/Desktop/GEG/sbm/sbm-ingester/docs/nem12_mappings_latest.json
```
Expected: exactly 4 lines in each file (`SEM0002125-B1`, `SEM0002125-K1`, `SEM0002152-B1`, `SEM0002152-K1`) with their original `p:bunnings:...` IDs.

---

## Task 11: Document the migration in the ingester README

**Files:**
- Modify: `sbm/sbm-ingester/README.md` (append a small section)

- [ ] **Step 1: Append migration reference to README**

Add a new section to the README that points future readers at the script and the backup folder:

```markdown
## One-off Migrations

### `migrate_pm_reassignment.py` (2026-04-16)

Reassigns two Bunnings power meter clusters to their correct sites:
- `SEM0002152` cluster: `7213 BUN KIRRAWEE` → `8220 BUN CABOOLTURE SLR`
- `SEM0002125` cluster: `8220 BUN CABOOLTURE SLR` → `7349 BUN BATEMANS BAY SLR` (site created by the script)

All 6 point/PM vertex IDs are preserved via user-supplied IDs in `addV`, so
Hudi `sensorid` and `nem12_mappings_latest.json` do not require changes.

Run phases in order: 0 (backup) → 1a → 1b → 2 → 3 → 4 (verify).

Backup JSON and restore.gremlin land in `data/migration-backups/` (gitignored).
```

- [ ] **Step 2: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
git add README.md
git commit -m "docs: document PM reassignment migration script"
```

---

## Rollback Procedure (not a task — emergency reference)

If any phase fails mid-execution, the most recent `backup-*.restore.gremlin` file under `data/migration-backups/` contains a complete rollback script:

1. Open the `.restore.gremlin` file.
2. Each line is a standalone Gremlin query.
3. Execute them in order via `gemsNeptuneExplorer`:
   ```bash
   while IFS= read -r line; do
     [[ -z "$line" ]] && continue
     [[ "$line" =~ ^// ]] && continue
     aws lambda invoke --function-name gemsNeptuneExplorer --region ap-southeast-2 \
       --cli-binary-format raw-in-base64-out \
       --payload "$(python3 -c "import json,sys; print(json.dumps({'gremlin': sys.argv[1]}))" "$line")" \
       /tmp/restore-out.json
   done < data/migration-backups/backup-<timestamp>.restore.gremlin
   ```
4. Re-run `--phase 4` to confirm state. (Phase 4 will fail because the restore re-attaches SEM0002152 to Kirrawee etc., which is the pre-migration state — that's expected during rollback.)
