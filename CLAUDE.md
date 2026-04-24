# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **GIS ETL pipeline** for Halifax Regional Municipality (HRM) that synchronizes street asset data between an authoritative source and a working layer (`TRN_STREET_RIVA`) used by the asset accounting system. It runs against Microsoft SQL Server geodatabases via ArcGIS Pro's ArcPy library.

> **Note:** The pipeline was originally built against `SDEADM.TRN_STREET`. Following the transition to a Linear Referencing System (LRS), this has been replaced by `SDEADM.TRNLRS_TRN_STREET_VW`, which is an almost 1-to-1 replacement. All scripts now reference `TRNLRS_TRN_STREET_VW` as the street source.

The pipeline requires **ArcGIS Pro with the Location Referencing Extension** and a valid SDE connection. It cannot run without this environment.

## Running the Pipeline

```bash
python scripts/trn_street_assets.py
```

Before running, configure the environment in `scripts/config.ini` and set the `SDE` variable at the top of `trn_street_assets.py` (line 13) to the correct `.sde` connection file path for your environment (LOCAL workstation or SERVER). The SERVER path should remain consistent across users and not be changed; LOCAL paths are set ad-hoc per developer workstation.

**There is no automated test suite.** All changes must be validated manually against the dev SDE connection before promoting to QA or PROD.

The three ETL steps are **commented out by default** in `__main__` as a safety measure. Uncomment selectively:

```python
step_one_new_hrm_streets()        # Insert new HRM-owned streets into TRN_STREET_RIVA
step_two_update_retired_streets() # Archive retired streets with DATE_RET, DATE_REV, OLD_FDMID
step_three_updating_existing()    # Sync geometry/attribute changes to existing segments
```

There is also a pure SQL equivalent in `scripts/riva_load.sql` that performs the same three steps using T-SQL with spatial methods.

## Architecture

### Data Flow

```
TRNLRS_TRN_STREET_VW (authoritative source, SDEADM)
    │
    ├── Step 1: LEFT JOIN to find new HRM-owned streets → INSERT into TRN_STREET_RIVA
    ├── Step 2: Find retired streets (DATE_RET populated) → UPDATE TRN_STREET_RETIRED
    └── Step 3: Detect geometry/attr changes → UPDATE TRN_STREET_RIVA
                                │
                                ▼
                    TRN_STREET_RIVA (working layer, versioned SDE)
                                │
                                ├── Read-Only Replica (TRN_Rosde) → sync via replicas.py
                                └── ASSET_ACCOUNTING.TRN_STREET_ASSETS (export)
```

### Key Tables

| Table | Schema | Purpose |
|---|---|---|
| `TRNLRS_TRN_STREET_VW` | SDEADM | Source of truth — all street segments (LRS view, replaced `TRN_STREET`) |
| `TRN_STREET_RIVA` | SDEADM | HRM-owned active streets (versioned feature class) |
| `TRN_STREET_RIVA_STAGE` | SDEADM | Staging table for SQL ETL path |
| `TRN_STREET_RETIRED` | SDEADM | Archive of retired/replaced street segments |
| `LND_HRM_PARCEL` | SDEADM | HRM land parcels (for intersection analysis) |
| `TRN_STREET_ASSETS` | ASSET_ACCOUNTING | Asset accounting export layer |

### Scripts

- **`scripts/trn_street_assets.py`** — Python ETL orchestrator using ArcPy cursors and spatial operations
- **`scripts/riva_load.sql`** — T-SQL equivalent of trn_street_assets.py; use when running directly against SQL Server
- **`scripts/replicas.py`** — Manages the `TRN_Rosde` one-way replica (RW→RO sync); run this when adding new feature classes to the replica
- **`scripts/add_fields.py`** — Adds new fields across DEV/QA/PROD environments
- **`scripts/add_feature_to_replica.py`** — Adds a new feature class to the replica dataset
- **`scripts/new_field_PURCHASE.sql`** — Street × parcel midpoint intersection analysis (uses `STIntersects`, `STPointN`)
- **`scripts/land_acq_source.sql`** — Asset accounting view with acquisition source join
- **`scripts/trn_street_riva_update.sql`** — Utility to sync `SYS_DATE` field

### Multi-Environment Configuration

`config.ini` maps environment names to `.sde` connection file paths:

```ini
[LOCAL]   # ArcGIS Pro Favorites on developer workstation
[SERVER]  # Network paths on automated job runner (E:\HRM\Scripts\SDE\...)
```

Environments: `dev_rw`, `qa_rw`, `prod_rw` (read-write SDE connections owned by `sdeadm`).

### Replica Management

The `TRN_Rosde` replica is a **one-way replica** (RW geodatabase → RO geodatabase) containing 51 transportation feature classes (see `TRN_Rosde_updated.txt`). When adding a new feature class:
1. Use `scripts/add_feature_to_replica.py` or manually unregister/recreate via `replicas.py`
2. Sync direction: `FROM_GEODATABASE1_TO_2`
3. `TRN_STREET_RETIRED` was added in the most recent update (required for Step 2 to function in read-only environments)

### Location Referencing System (LRS)

See `claude_lrs.md` for detailed LRS architecture. Key points:
- Event tables are registered against routes with standard fields: `EVENTID`, `ROUTEID`, `FROMDATE`, `TODATE`, `FROMMEASURE`, `TOMEASURE`
- Events behave according to calibration rules: CALIBRATE, RETIRE, EXTEND, REASSIGN, REALIGN, REVERSE, CARTO_REALIGN
- QA checks are required after dynamic segmentation updates

### Final Products

The `final_products/` folder contains example CSV files representing what the client (asset accounting system) ingests as final outputs. Use these as the reference for expected column names, data types, and formatting when making changes to the export views or load scripts.

### Known Outstanding Work
- **Blank `SHORT_DESC`/`LONG_DESC` fields** — this is an unresolved bug from the initial pipeline creation, not a data quality issue in the source. It is unknown whether it was ever fixed. If streets are being inserted with blank description fields, investigate the Step 1 insert logic in `trn_street_assets.py` and the equivalent section of `riva_load.sql`.
