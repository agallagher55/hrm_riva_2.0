# ASSET_ACCOUNTING — Land Assets

This document covers the three land-related objects in the ASSET_ACCOUNTING schema.

---

## Table of Contents

- [LND\_LAND\_ASSETS](#lnd_land_assets)
- [LAND\_ASSET\_TRANSFORM\_VW](#land_asset_transform_vw)
- [LAND\_ASSETS\_EXPORT\_VW](#land_assets_export_vw)
- [Open Questions](#open-questions)

---

## LND_LAND_ASSETS

**Type:** Physical table (Database Table)

**Purpose:** Asset accounting export layer for HRM-owned land parcels. Parallel to `TRN_STREET_ASSETS` but for land.

**Source data (GIS side):** `SDEADM.LND_HRM_PARCEL` and related tables — accessed via `ASSET_ACCOUNTING.LAND_ASSET_TRANSFORM_VW` (see below).

**Load process:** Not managed in this repository. Per notes in `scripts/land_acq_source.sql`, the load is performed by the ERP/asset accounting team (Sylvie, Rajni, Andrea) via a SQL update script on their side. The `LAND_ASSET_TRANSFORM_VW` view is the GIS team's contribution — it presents the source data in a ready-to-consume format for that script.

**What we know about the schema (from `LAND_ASSETS_EXPORT_VW` column list):**

| Column | Notes |
|---|---|
| ASSET_ID | Links to `LND_HRM_PARCEL.ASSET_ID` |
| GROUP_ID | Links to `LND_LAND_GROUP.GROUP_ID` |
| ACQ_TYPE | Acquisition type |
| PID | Parcel ID |
| ACQ_COST | Acquisition cost |
| ASSET_TYPE | Hardcoded `'LAND'` |
| OWNER | From `LND_HRM_PARCEL.OWNER` |
| DISPOSAL | |
| MAIN_CLASS | From `LND_HRM_PARCEL.ASSETCODE` |
| ACQ_DATE | |
| DISP_DATE | |
| DISP_TYPE | |
| ACQDISPSOURCE | Added April 2025 — from `LND_ACQUISITION_DISPOSAL.SOURCE` |
| LAND_NAME | From `LND_LAND_GROUP.LAND_NAME` |
| HECTARES | From `LND_HRM_PARCEL.HECTARES` |
| SERV_CAT | From `LND_LAND_GROUP.SERV_CAT` |
| PARK_ID | From `LND_HRM_PARK.PARK_ID` |
| PARK_NAME | From `LND_HRM_PARK.PARK_NAME` |
| REPL_COST | From `LND_HRM_PARCEL.REPLCSRA` |
| HRWC_FLAG | Hardcoded NULL |
| HRM_PARCEL_ADDDATE | From `LND_HRM_PARCEL.ADDDATE` |
| HRM_PARCEL_MODDATE | From `LND_HRM_PARCEL.MODDATE` |

**Recent change:** `ACQDISPSOURCE nvarchar(20) null` was added to this table in April 2025 (see email thread RE: current land and street asset exports, TASK0291766, confirmed live in PROD by Rajni Gupta on April 2, 2025).

---

## LAND_ASSET_TRANSFORM_VW

**Type:** View

**Definition:** `sql/land_asset_transform_vw.sql`

**Purpose:** Transformation layer — reads GIS parcel and acquisition data from the SDEADM schema and presents it in a format suitable for loading into `LND_LAND_ASSETS`. This is the GIS team's side of the land asset pipeline.

**Source tables:**

| Table | Schema | Role |
|---|---|---|
| `LND_HRM_PARCEL` | SDEADM | Core parcel geometry and attributes |
| `LND_HRM_PARCEL_HAS_ACQ_DISP` | SDEADM | Junction: parcel → acquisition/disposal |
| `LND_ACQUISITION_DISPOSAL` | SDEADM | Acquisition and disposal transaction records |
| `LND_LAND_GROUP` | SDEADM | Land group names and service category |
| `LND_HRM_PARK` | SDEADM | Park names associated with parcel groups |

**Logic (UNION of two branches):**

**Branch 1 — Active HRM-owned parcels:**
- `RIGHT JOIN` from `LND_HRM_PARCEL` (keeps all parcels even without acq/disp records)
- Filter: `OWNER = 'HRM'`, excludes `ASSETCODE IN ('BSP', 'ROW')`

**Branch 2 — Disposals and non-HRM acquisitions:**
- Disposal records: `TRANS_TYPE = 2` and not BSP/ROW
- Acquisition records where `OWNER <> 'HRM'` and not BSP/ROW (i.e., parcels being acquired into HRM ownership)

**Excluded asset codes:**
- `BSP` — Building site / private?
- `ROW` — Right-of-way (handled separately via the street pipeline)

**Key field mapping:**

| View Column | Source |
|---|---|
| ASSET_ID | `LND_HRM_PARCEL.ASSET_ID` |
| MAIN_CLASS | `LND_HRM_PARCEL.ASSETCODE` |
| ACQDISPSOURCE | `LND_ACQUISITION_DISPOSAL.SOURCE` (added 2025) |
| REPL_COST | `LND_HRM_PARCEL.REPLCSRA` |
| LAND_NAME | `LND_LAND_GROUP.LAND_NAME` |
| PARK_ID / PARK_NAME | `LND_HRM_PARK` joined on `GROUP_ID` |
| ADDDATE / MODDATE | Cast to `date` from `LND_HRM_PARCEL` |

---

## LAND_ASSETS_EXPORT_VW

**Type:** View

**Definition:** `sql/land_assets_export_vw.sql`

**Purpose:** Read-only export interface over `LND_LAND_ASSETS`. Parallel to `STREET_ASSETS_EXPORT_VW` for the land asset side.

**Key filter:** Only returns assets where `ASSET_RECORD_COUNT = 1` — assets with duplicate rows in `LND_LAND_ASSETS` are suppressed. The count is computed via a CTE (`LAND_RECORD_COUNT`).

**Date formatting:** `ACQ_DATE`, `DISP_DATE`, `HRM_PARCEL_ADDDATE`, `HRM_PARCEL_MODDATE` are converted to `varchar(20)`.

**No load process required** — live view over `LND_LAND_ASSETS`.

---

## Open Questions

| Question | Status |
|---|---|
| How exactly is `LND_LAND_ASSETS` loaded? What script does the ERP team run, and on what schedule? | Unknown — owned by ERP delivery team (Sylvie Blanchard, Rajni Gupta, Rob Farmer) |
| Does `LAND_ASSET_TRANSFORM_VW` feed `LND_LAND_ASSETS` directly (i.e., `INSERT INTO ... SELECT FROM LAND_ASSET_TRANSFORM_VW`) or is there intermediate processing? | Unknown |
| Should `HRWC_FLAG` in `LAND_ASSET_TRANSFORM_VW` ever be non-null? Currently hardcoded to NULL. | Unknown |
