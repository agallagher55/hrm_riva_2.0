# HRM RIVA 2.0 — Street Asset ETL

ETL pipeline for synchronizing Halifax Regional Municipality (HRM) street data between the GIS geodatabase (`TRN_STREET`) and the RIVA asset accounting layer (`ASSET_ACCOUNTING.TRN_STREET_ASSETS`).

---

## Background

### Email: TRN_STREET_RIVA Changes (May 2024)

*Between Alex Gallagher (GIS Systems Analyst) and Dwight Welch, cc Rob Farmer — May 16, 2024*

This thread covered database migration changes related to `TRN_STREET_RIVA`. Dwight Welch noted a key advantage of the SQL Server platform over Oracle:

> "One of the nice features of SQL Server (over Oracle) is that the displayed names of every object in the database are simply aliases – the real object identifiers are GUIDs that are hidden from view. So changing the name of a user or login is simply changing how it is displayed, not its actual identifier. In Oracle that same change would have involved dropping, re-creating and re-provisioning the user."

Alex confirmed the change was completed: *"TOO FAST, thanks Dwight!"*

---

### Email: TRN_STREET_ASSETS — New Field (March 2025)

*Between Alex Gallagher and Rob Farmer — March 20–21, 2025*

This thread concerned adding a new field to `TRN_STREET_ASSETS` to capture acquisition/disposal source information derived from intersecting parcel data.

Alex proposed the field name:
> "Do you care about what the field name is called? **ACQDISPSOURCE**, sound good?"

Rob confirmed:
> "Sounds fine to me. Thank you"

The field `ACQDISPSOURCE` has been added to `ASSET_ACCOUNTING.LND_LAND_ASSETS` as `nvarchar(20), null` and is included in the `ASSET_ACCOUNTING.LAND_ASSETS_EXPORT_VW` view (see `land_acq_source.sql`). Live data confirms it is populated with values such as `"Transaction Summary"` for land assets. The equivalent field still needs to be added to `ASSET_ACCOUNTING.TRN_STREET_ASSETS`.

---

## Repository Structure

```
hrm_riva_2.0/
├── scripts/
│   ├── main.py                          # Main ETL script (3-step process)
│   ├── utils.py                         # Utility functions
│   ├── replicas.py                      # ArcGIS replica management
│   ├── add_fields.py                    # Field addition utility
│   ├── add_feature_to_replica.py        # Add features to replicas
│   ├── config.ini                       # SDE connection paths (DEV/QA/PROD)
│   ├── riva_load.sql                    # SQL mirror of main.py ETL (3-step)
│   ├── trn_street_riva_update.sql       # SYS_DATE sync utility
│   ├── new_field_PURCHASE.sql           # Streets × parcels × acquisition query
│   ├── land_acq_source.sql              # LAND_ASSETS_EXPORT_VW definition
│   ├── TRN_Rosde.txt                    # Original TRN replica feature list (50)
│   ├── TRN_Rosde_updated.txt            # Updated replica list (51, adds TRN_STREET_RETIRED)
│   └── scratch.gdb/                     # Local ArcGIS File Geodatabase (working copy)
├── riva_intersectingParcels.csv         # Output: 11,557 street-parcel intersections
├── RIVA.vsdx                            # Architecture diagram (Visio)
├── RIVA GIS Load - Copy.docx            # GIS load documentation
├── RIVA_GIS Link Information sheet.docx # Link information sheet
├── RE_ TRN_STREET_ASSETS - New Field.msg
└── RE_ TRN_STREET_RIVA changes.msg
```

---

## Key Tables

| Table | Schema | Description |
|---|---|---|
| `TRN_STREET` | `SDEADM` | Source of truth — all HRM street centrelines |
| `TRN_STREET_RIVA` | `SDEADM` | RIVA working layer — HRM-owned active streets |
| `TRN_STREET_RIVA_STAGE` | `SDEADM` | Staging table for SQL-based ETL |
| `TRN_STREET_RETIRED` | `SDEADM` | Archive of retired/replaced street segments |
| `LND_HRM_PARCEL` | `SDEADM` | HRM parcel polygons |
| `LND_HRM_PARCEL_HAS_ACQ_DISP` | `SDEADM` | Parcel → acquisition/disposal link |
| `LND_ACQUISITION_DISPOSAL` | `SDEADM` | Acquisition/disposal records |
| `TRN_STREET_ASSETS` | `ASSET_ACCOUNTING` | Asset accounting export for street assets (see schema below) |
| `LND_LAND_ASSETS` | `ASSET_ACCOUNTING` | Asset accounting export for land assets — `ACQDISPSOURCE` added ✓ |
| `LAND_ASSETS_EXPORT_VW` | `ASSET_ACCOUNTING` | View over `LND_LAND_ASSETS` with `ACQDISPSOURCE` |

### ASSET_ACCOUNTING.LND_LAND_ASSETS — Status

`ACQDISPSOURCE (nvarchar(20), null)` has been added to this table. Field is live in PROD and populates with values such as `"Transaction Summary"` (derived from `LND_ACQUISITION_DISPOSAL`). NULL where no acquisition/disposal record exists for the parcel.

### ASSET_ACCOUNTING.TRN_STREET_ASSETS — Schema

| Column | Type |
|---|---|
| OBJECTID | int, not null |
| STR_CODE | int, not null |
| STR_NAME | nvarchar(40), not null |
| STR_TYPE | nvarchar(4), not null |
| GSA_NAME | nvarchar(40), not null |
| FULL_NAME | nvarchar(40), not null |
| STR_STATUS | nvarchar(4), not null |
| OWN | nvarchar(4), not null |
| DATE_ACCEPT | datetime2(7), null |
| PST_CLASS | nvarchar(40), null |
| SOURCE | nvarchar(12), null |
| FDMID | int, not null |
| FROM_STREET | int, null |
| TO_STREET | int, null |
| SHAPE_LENGTH | numeric(38,8), null |
| SHORT_DESC | nvarchar(120), null |
| LONG_DESC | nvarchar(120), null |
| SURF_MAT | nvarchar(10), null |
| SDI | numeric(38,8), null |
| PAVE_WIDTH | numeric(38,8), null |
| RATE_DATE | datetime2(7), null |
| NUM_CURB | smallint, null |
| WIDTH2 | numeric(38,8), null |
| BASEVAL | numeric(38,8), null |
| SURFVAL | numeric(38,8), null |
| OLD_FDMID | numeric(38,8), null |
| DATE_RET | datetime2(7), null |
| DATE_REV | datetime2(7), null |
| GLOBALID | uniqueidentifier, not null |
| DATE_ACT | datetime2(7), null |
| SYS_DATE | datetime2(7), null |

> Note: `ACQDISPSOURCE` is **not yet present** in `TRN_STREET_ASSETS`. Adding it is the next step (mirrors what was done on `LND_LAND_ASSETS`).

---

## Prerequisites

- **ArcGIS Pro** with ArcPy (Python 3)
- **SQL Server** with SDE geodatabase connections configured
- SDE `.sde` connection files placed according to `config.ini` paths
- Access to DEV / QA / PROD environments
- `pandas` Python package (for `utils.remove_duplicates_from_csv`)

---

## Configuration

Edit `scripts/config.ini` to point to the correct `.sde` connection files for your environment.

```ini
[LOCAL]
dev_rw  = C:\Users\gallaga\AppData\Roaming\Esri\ArcGISPro\Favorites\DEV_RW_SDEADM.sde
qa_rw   = C:\Users\gallaga\AppData\Roaming\Esri\ArcGISPro\Favorites\QA_RW_SDEADM.sde
prod_rw = C:\Users\gallaga\AppData\Roaming\Esri\ArcGISPro\Favorites\PROD_RW_SDEADM.sde
...

[SERVER]
dev_rw  = E:\HRM\Scripts\SDE\SQL\Dev\dev_RW_sdeadm.sde
prod_rw = E:\HRM\Scripts\SDE\SQL\Prod\prod_RW_sdeadm.sde
...
```

The `SDE` path at the top of `main.py` must also be set before running:

```python
SDE = r"E:\HRM\Scripts\SDE\SQL\dev_RW_sdeadm.sde"
```

---

## ETL Process — main.py

The script runs a three-step ETL to keep `TRN_STREET_RIVA` in sync with `TRN_STREET`.

### Step 1 — New HRM Streets (`step_one_new_hrm_streets`)

Identifies streets in `TRN_STREET` (owned by HRM, not yet retired) that do not yet exist in `TRN_STREET_RIVA` and appends them.

1. Exports `TRN_STREET_RIVA` to `scratch.gdb` as a local backup.
2. Filters `TRN_STREET` for `OWN = 'HRM'`.
3. Removes any FDMIDs already present (non-retired) in `TRN_STREET_RIVA`.
4. Exports the remaining new streets to `TBL_new_streets_for_riva`.
5. Appends into the local `TRN_STREET_RIVA` copy using `NO_TEST` schema.

### Step 2 — Retired Streets (`step_two_update_retired_streets`)

Detects RIVA records whose streets have since been retired (moved to `TRN_STREET_RETIRED`) and updates the corresponding fields.

Fields updated:
- `DATE_RET` — from `TRN_STREET_RETIRED`
- `DATE_REV` — set to today
- `OLD_FDMID` — from `TRN_STREET_RETIRED`
- `SHAPE_LENGTH` — recalculated

### Step 3 — Existing Segment Updates (`step_three_updating_existing`)

For all non-retired RIVA streets whose `SHAPE_LENGTH` differs from the current `TRN_STREET` value, updates:

| Field | Calculation |
|---|---|
| `SHAPE_LENGTH` | `TRN_STREET.SHAPE@LENGTH` |
| `SHORT_DESC` | `FULL_NAME (FROM_STR TO TO_STR)` |
| `LONG_DESC` | `FULL_NAME GSA_LEFT` |
| `OLD_FDMID` | from `TRN_STREET` |
| `DATE_REV` | today |
| `DATE_ACT` | from `TRN_STREET` |
| `SYS_DATE` | from `TRN_STREET` |

### Running the Script

```bash
python scripts/main.py
```

Steps 2 and 3 are commented out in `__main__` for safety. Uncomment as needed:

```python
# STEP 1
trn_street_riva_local, local_workspace = step_one_new_hrm_streets()

# STEP 2
step_two_update_retired_streets(trn_street_riva_local, local_workspace)

# STEP 3
step_three_updating_existing(trn_street_riva_local)
```

> **Known QA issue:** After running, review records where `SHORT_DESC`, `LONG_DESC`, or `DATE_REV` are blank — particularly `FDMID: 700013207`.

---

## SQL Scripts

### `riva_load.sql`
SQL Server equivalent of `main.py`. Operates against `TRN_STREET_RIVA_STAGE`:
1. Inserts new HRM streets not yet in RIVA.
2. Updates retired streets with `DATE_RET`, `DATE_REV`, `OLD_FDMID`.
3. Updates existing segments where `SHAPE_LENGTH` has changed.

### `trn_street_riva_update.sql`
Syncs the `SYS_DATE` field from `TRN_STREET` into `TRN_STREET_RIVA` on matching `FDMID`.

### `new_field_PURCHASE.sql`
Queries streets against intersecting HRM parcels. Uses `STIntersects` on the street centreline midpoint to find overlapping parcels, then joins through `LND_HRM_PARCEL_HAS_ACQ_DISP` and `LND_ACQUISITION_DISPOSAL`. Aggregates `ACQDISTYPE` and `TRANS_TYPE` per street/parcel.

### `land_acq_source.sql`
Creates/recreates the `ASSET_ACCOUNTING.LAND_ASSETS_EXPORT_VW` view. Includes the new `ACQDISPSOURCE` field (added per the March 2025 email discussion with Rob Farmer). Only exposes assets with a single record (`ASSET_RECORD_COUNT = 1`).

---

## Replica Management

The `TRN` replica (`TRN_Rosde`) contains 50+ transportation feature classes replicated from the Read-Write (`GISRW01.SDEADM`) to Read-Only geodatabases.

`TRN_Rosde_updated.txt` adds `SDEADM.TRN_STREET_RETIRED` to the replica — required for Step 2 to function in the RO environment (noted in `riva_load.sql`: *"ADD TRN_STREET_RETIRED TO READ_ONLY AND REPLICA"*).

To add a feature to the replica:

```python
# scripts/add_feature_to_replica.py
replicas.add_to_replica(sde_conn, "SDEADM.TRN_STREET_RETIRED", "TRN_Rosde")
```

---

## Populating ASSET_ACCOUNTING.TRN_STREET_ASSETS

### Step 1 — Run the RIVA ETL to sync `TRN_STREET_RIVA`

Execute `main.py` (all 3 steps) against PROD to ensure `TRN_STREET_RIVA` is current before loading into the asset accounting layer.

### Step 2 — Truncate and reload `ASSET_ACCOUNTING.TRN_STREET_ASSETS`

Map fields from `TRN_STREET_RIVA` to `TRN_STREET_ASSETS`:

```sql
TRUNCATE TABLE ASSET_ACCOUNTING.TRN_STREET_ASSETS;

INSERT INTO ASSET_ACCOUNTING.TRN_STREET_ASSETS (
    STR_CODE, STR_NAME, STR_TYPE, GSA_NAME, FULL_NAME, STR_STATUS, OWN,
    DATE_ACCEPT, PST_CLASS, SOURCE, FDMID, SHAPE_LENGTH,
    SHORT_DESC, LONG_DESC, OLD_FDMID, DATE_RET, DATE_REV,
    DATE_ACT, SYS_DATE
)
SELECT
    r.STR_CODE, r.STR_NAME, r.STR_TYPE, r.GSA_NAME, r.FULL_NAME,
    r.STR_STATUS, r.OWN, r.DATE_ACCEPT, r.PST_CLASS, r.SOURCE,
    r.FDMID, r.SHAPE_LENGTH, r.SHORT_DESC, r.LONG_DESC,
    r.OLD_FDMID, r.DATE_RET, r.DATE_REV, r.DATE_ACT, r.SYS_DATE
FROM SDEADM.TRN_STREET_RIVA r;
```
