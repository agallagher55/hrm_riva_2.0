# ASSET_ACCOUNTING — Street Assets

This document covers the two street-related objects in the ASSET_ACCOUNTING schema.

---

## Table of Contents

- [End-to-End Flow](#end-to-end-flow)
- [TRN\_STREET\_ASSETS](#trn_street_assets)
- [STREET\_ASSETS\_EXPORT\_VW](#street_assets_export_vw)
- [scripts/trn\_street\_assets.py — Detailed Breakdown](#scriptstrn_street_assetspy--detailed-breakdown)
  - [Step 5 — step\_five\_truncate\_load\_asset\_accounting()](#step-5--step_five_truncate_load_asset_accounting)
- [Open Questions](#open-questions)

---

## End-to-End Flow

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                      SDEADM  (SQL Server — versioned GDB)                  ║
║                                                                              ║
║  TRNLRS_TRN_STREET_VW          TRN_STREET_RIVA          E_StreetStatus      ║
║  ─────────────────────         ───────────────          ──────────────────  ║
║  Authoritative source.         Working layer.           LRS event table.    ║
║  All street segments.          HRM-owned active         ROUTEID →           ║
║  OWN, FDMID, geometry,         segments. Versioned      DATE_ACCEPT lookup. ║
║  FULL_NAME, FROM_STR,          feature class.                               ║
║  TO_STR, GSA_LEFT,             DATE_RET flags           TRNLRS_segmented_   ║
║  OLD_FDMID, DATE_ACT,          retired rows.            street_events       ║
║  SYS_DATE.                                              ──────────────────  ║
║                                                         LRS event table.    ║
║                                                         FDMID, TO_DATE,     ║
║                                                         OLD_FDMID,          ║
║                                                         SHAPE@LENGTH,       ║
║                                                         ROUTE_ID.           ║
╚══════════════════════════════════════════════════════════════════════════════╝
         │                           │
         │  OWN = 'HRM' filter       │  export full RIVA copy
         │  FDMID diff vs RIVA       │  collect active FDMIDs
         ▼                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 1  step_one_new_hrm_streets()                                         │
│                                                                             │
│  1. Export TRN_STREET_RIVA → scratch.gdb/TRN_STREET_RIVA  (local backup)   │
│  2. Select HRM-owned streets from TRN_STREET → TRN_street_HRMowned         │
│  3. Remove FDMIDs already in RIVA (DATE_RET IS NULL) → net-new only        │
│  4. Export remainder → TBL_new_streets_for_riva                             │
│  5. Append TBL_new_streets_for_riva into local RIVA copy (NO_TEST)         │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 ▼
              scratch.gdb/TRN_STREET_RIVA  (net-new rows added)
              scratch.gdb/TBL_new_streets_for_riva  (new rows only)
                                 │
         ┌───────────────────────┤
         │                       │
         ▼                       ▼
  TRNLRS_TRN_STREET_VW    TRNLRS_segmented_street_events
  (FDMID presence check    (TO_DATE, OLD_FDMID,
   — active segments only)  SHAPE@LENGTH, ROUTE_ID)
         │                       │
         │                 E_StreetStatus
         │                 (ROUTEID → DATE_ACCEPT
         │                  → DATE_ACT value)
         │                       │
         └───────────────────────┤
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 2  step_two_update_retired_streets()                                  │
│                                                                             │
│  Detect: active RIVA FDMIDs absent from TRN_STREET  (retired in LRS)       │
│  Look up retirement metadata in TRNLRS_segmented_street_events              │
│  Write to RIVA:                                                             │
│    DATE_RET   ← TO_DATE (from LRS)                                          │
│    DATE_REV   ← today                                                       │
│    OLD_FDMID  ← OLD_FDMID (from LRS)                                        │
│    SHAPE_LENGTH ← updated length (from LRS)                                 │
│    DATE_ACT   ← DATE_ACCEPT via ROUTE_ID → E_StreetStatus                  │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 ▼
              scratch.gdb/TRN_STREET_RIVA  (retired rows stamped)
                                 │
         ┌───────────────────────┘
         │
         ▼
  TRNLRS_TRN_STREET_VW
  (SHAPE@LENGTH, FULL_NAME,
   FROM_STR, TO_STR, GSA_LEFT,
   OLD_FDMID, DATE_ACT, SYS_DATE
   — keyed by FDMID)
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 3  step_three_updating_existing()                                     │
│                                                                             │
│  Detect: active RIVA rows (DATE_RET IS NULL) where SHAPE_LENGTH ≠ TRN_STREET│
│  Write to RIVA:                                                             │
│    SHAPE_LENGTH ← TRN_STREET.SHAPE@LENGTH                                   │
│    SHORT_DESC   ← FULL_NAME + " (" + FROM_STR + " TO " + TO_STR + ")"      │
│    LONG_DESC    ← FULL_NAME + " (" + GSA_LEFT + ")"                         │
│    OLD_FDMID    ← TRN_STREET.OLD_FDMID                                      │
│    DATE_REV     ← today                                                     │
│    DATE_ACT     ← TRN_STREET.DATE_ACT                                       │
│    SYS_DATE     ← TRN_STREET.SYS_DATE                                       │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 ▼
              scratch.gdb/TRN_STREET_RIVA  (attributes/lengths synced)
                                 │
         ┌───────────────────────┘
         │
         ▼
  scratch.gdb/TBL_new_streets_for_riva
  (net-new rows from Step 1)
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 4  step_four_validation_review()                   [QA — no rollback] │
│                                                                             │
│  Check: null / blank SHORT_DESC and LONG_DESC in TBL_new_streets_for_riva  │
│  Output: row count + per-field null counts with % (printed to console)     │
│  Action: review blanks manually before proceeding to load                  │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                          QA sign-off
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STEP 5  step_five_truncate_load_asset_accounting()                         │
│                                                                             │
│  Truncate ASSET_ACCOUNTING.TRN_STREET_RIVA                                  │
│  Append all rows from scratch.gdb/TRN_STREET_RIVA (or SDEADM.TRN_STREET_   │
│  RIVA if run standalone).  Row counts must match after load.               │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 ▼
                  ASSET_ACCOUNTING.TRN_STREET_RIVA
                                 │
                                 │  live view — no reload needed
                                 ▼
                  ASSET_ACCOUNTING.STREET_ASSETS_EXPORT_VW
                  (dates cast to varchar(20) for consumer compatibility)
                                 │
                                 ▼
                        Asset Accounting System
```

---

## TRN_STREET_ASSETS

**Type:** Enterprise Geodatabase Table (physical)

**Purpose:** Asset accounting export layer for HRM-owned street segments. This is the table the asset accounting system reads for street data.

**Source:** `SDEADM.TRN_STREET_RIVA`

**Load process:** Run `scripts/trn_street_assets.py` against PROD. Steps 1–3 sync the local RIVA copy; Step 4 validates it; Step 5 (`step_five_truncate_load_asset_accounting`) performs the truncate-and-reload into `ASSET_ACCOUNTING.TRN_STREET_RIVA` automatically.

This is a full truncate-and-reload; there is no incremental update. Row counts should match between the source and `ASSET_ACCOUNTING.TRN_STREET_RIVA` after the load.

**Reload steps documented in:** `steps.txt`

**Schema:** see `README.md` → "ASSET_ACCOUNTING.TRN_STREET_ASSETS — Schema"

**Known issue:** Some records have blank `SHORT_DESC` / `LONG_DESC` (including FDMID 700013207). These should be QA'd before loading. `SURF_MAT`, `SDI`, `PAVE_WIDTH`, `RATE_DATE`, `NUM_CURB`, `WIDTH2`, `BASEVAL`, `SURFVAL`, and `FROM_STREET`/`TO_STREET` are not populated by the current ETL — they may be managed separately.

---

## STREET_ASSETS_EXPORT_VW

**Type:** View

**Definition:** `sql/street_assets_export_vw.sql`

**Purpose:** Read-only export interface over `TRN_STREET_ASSETS`. Converts datetime fields to `varchar(20)` for compatibility with the consuming system.

**No load process required** — it is a live view. It does not need to be refreshed separately; reloading `TRN_STREET_ASSETS` automatically updates what this view returns.

**Fields added vs base table:** None — all columns are passed through, dates are cast.

---

## scripts/trn_street_assets.py — Detailed Breakdown

The ETL orchestrator that keeps `TRN_STREET_RIVA` in sync with `TRNLRS_TRN_STREET_VW`. It runs five functions in sequence against a **local scratch.gdb** copy of RIVA; the edited local copy is then used as the source for the final truncate-and-reload into `ASSET_ACCOUNTING.TRN_STREET_RIVA`.

### Global Configuration

| Variable | Value | Purpose |
|---|---|---|
| `SDE` | `.sde` file path | Active SDEADM connection (dev/qa/prod RW) — edit before running |
| `ASSET_ACCOUNTING_SDE` | `.sde` file path | ASSET_ACCOUNTING connection — edit to match environment |
| `TRNLRS_TRN_STREET_VW` | `SDEADM.TRNLRS_TRN_STREET_VW` | Authoritative street source (LRS view) |
| `TRN_STREET_RIVA` | `SDEADM.TRN_STREET_RIVA` | Working layer being updated |
| `AA_TRN_STREET_RIVA` | `ASSET_ACCOUNTING.TRN_STREET_RIVA` | Target table for the final truncate-and-load |
| `TRNLRS_SEGMENTED` | `SDEADM.TRNLRS_segmented_street_events` | LRS event table for retirement data |
| `E_STREET_STATUS` | `SDEADM.TRNLRS\SDEADM.E_StreetStatus` | LRS event table for `DATE_ACCEPT` lookup |

---

### Step 1 — `step_one_new_hrm_streets()`

**Purpose:** Find streets in `TRNLRS_TRN_STREET_VW` (owned by HRM, not yet retired) that are absent from `TRN_STREET_RIVA`, and insert them.

**Filters applied:**
- `OWN LIKE 'HRM'` — only HRM-owned streets from the source
- `DATE_RET IS NULL` on `TRN_STREET_RIVA` — ignore already-retired RIVA records when building the exclusion set

**Logic:**
1. Create (or reuse) `scratch.gdb` in the scripts directory.
2. Export `TRN_STREET_RIVA` into `scratch.gdb` as a local backup copy.
3. Select all HRM-owned streets from `TRN_STREET` → `TRN_street_HRMowned` in scratch.gdb.
4. Copy that selection → `trn_street_new_streets_riva`.
5. Collect all `FDMID`s currently in `TRN_STREET_RIVA` where `DATE_RET IS NULL`.
6. Delete from `trn_street_new_streets_riva` any row whose `FDMID` is already in RIVA → leaving only genuinely new streets.
7. Export the remainder to `TBL_new_streets_for_riva`.
8. `Append` `TBL_new_streets_for_riva` into the local RIVA copy (`schema_type="NO_TEST"`).

**Returns:** `(trn_street_riva_copy, local_gdb)` — passed to Steps 2–4.

**Fields written to RIVA:** all columns carried over from `TRN_STREET` via `NO_TEST` append (no field mapping).

---

### Step 2 — `step_two_update_retired_streets(new_riva_streets)`

**Purpose:** Detect RIVA records that have disappeared from `TRN_STREET` (i.e., retired in the LRS) and stamp them with retirement metadata.

**Sources queried:**
| Table | Fields read | Why |
|---|---|---|
| `E_StreetStatus` | `ROUTEID`, `DATE_ACCEPT` | Builds a `route_id → date_accept` lookup used to populate `DATE_ACT` |
| `TRN_STREET` | `FDMID` | Full set of active FDMIDs in the authoritative source |
| `TRNLRS_segmented_street_events` | `FDMID`, `TO_DATE`, `OLD_FDMID`, `SHAPE@LENGTH`, `ROUTE_ID` | Retirement date, predecessor FDMID, and updated length |
| local RIVA copy | `FDMID` (`DATE_RET IS NULL`) | Active RIVA rows that are candidates for retirement |

**Logic:**
1. Build `street_status_date_accept` dict: `ROUTEID → DATE_ACCEPT` (first occurrence wins).
2. Collect `trn_street_fdmids` — every FDMID currently in `TRN_STREET`.
3. Walk active RIVA rows; any `FDMID` not in `trn_street_fdmids` is added to `riva_retired_fdmids`.
4. Query `TRNLRS_SEGMENTED` where `TO_DATE IS NOT NULL` to get retirement metadata for each candidate FDMID.
5. Update RIVA rows in `riva_retired_fdmids`:

| RIVA Field | Value set |
|---|---|
| `DATE_RET` | `TO_DATE` from `TRNLRS_SEGMENTED` |
| `DATE_REV` | `datetime.today()` (processing date) |
| `OLD_FDMID` | `OLD_FDMID` from `TRNLRS_SEGMENTED` |
| `SHAPE_LENGTH` | updated length from `TRNLRS_SEGMENTED` |
| `DATE_ACT` | `DATE_ACCEPT` from `E_StreetStatus` via `ROUTE_ID` |

Short-circuits with a message if `riva_retired_fdmids` is empty.

---

### Step 3 — `step_three_updating_existing(trn_street_riva)`

**Purpose:** For active RIVA records that still exist in `TRN_STREET` but whose geometry or attributes have changed, sync the updated values into the local RIVA copy.

**Change detection:** compares `SHAPE_LENGTH` between RIVA and `TRN_STREET`. If lengths match, the row is skipped (geometry is assumed unchanged).

> Note: the script comments acknowledge length equality is rare — in practice nearly every active segment is updated.

**Source query on `TRN_STREET`:** loads all active streets into a dict keyed by `FDMID`, carrying:
`SHAPE@LENGTH`, `FULL_NAME`, `FROM_STR`, `TO_STR`, `GSA_LEFT`, `OLD_FDMID`, `DATE_ACT`, `SYS_DATE`

**Cursor on RIVA:** `DATE_RET IS NULL` — only active segments.

**Fields written per changed row:**

| RIVA Field | Calculation |
|---|---|
| `SHAPE_LENGTH` | `TRN_STREET.SHAPE@LENGTH` |
| `SHORT_DESC` | `FULL_NAME + " (" + FROM_STR + " TO " + TO_STR + ")"` |
| `LONG_DESC` | `FULL_NAME + " (" + GSA_LEFT + ")"` |
| `OLD_FDMID` | `TRN_STREET.OLD_FDMID` |
| `DATE_REV` | `datetime.today()` |
| `DATE_ACT` | `TRN_STREET.DATE_ACT` |
| `SYS_DATE` | `TRN_STREET.SYS_DATE` |

---

### Step 4 — `step_four_validation_review(local_gdb)`

**Purpose:** QA check on the net-new streets inserted by Step 1. Reports null/blank counts for `SHORT_DESC` and `LONG_DESC` — fields that must not be empty before loading to `ASSET_ACCOUNTING`.

**Source:** `TBL_new_streets_for_riva` in `scratch.gdb` (created by Step 1).

**Output:** Printed summary — total row count and per-field null/blank count with percentage. Returns `null_counts` dict.

**Not a hard stop:** Step 4 does not halt or roll back; it reports problems for manual review. Any blanks flagged here should be QA'd before proceeding to Step 5.

---

### Step 5 — `step_five_truncate_load_asset_accounting(source_riva=None)`

**Purpose:** Truncate `ASSET_ACCOUNTING.TRN_STREET_RIVA` and reload it from the local RIVA copy produced by Steps 1–3 (or directly from `SDEADM.TRN_STREET_RIVA` when run standalone).

**Parameters:**

| Parameter | Default | Description |
|---|---|---|
| `source_riva` | `TRN_STREET_RIVA` (`SDEADM`) | Path to the source table/feature class to load from. Pass `new_riva_streets` (the local scratch.gdb copy) when calling from `__main__`. |

**Logic:**
1. `arcpy.TruncateTable_management(AA_TRN_STREET_RIVA)` — removes all existing rows from `ASSET_ACCOUNTING.TRN_STREET_RIVA`.
2. `arcpy.Append_management(inputs=source_riva, target=AA_TRN_STREET_RIVA, schema_type="NO_TEST")` — loads all rows from the source.
3. `arcpy.GetCount_management` — verifies and prints the post-load row count.

**Returns:** path to `AA_TRN_STREET_RIVA`.

**Raises:** `RuntimeError` on any ArcPy failure (wraps the original exception).

**Connection requirement:** `ASSET_ACCOUNTING_SDE` must point to a valid `.sde` connection file with write access to `ASSET_ACCOUNTING.TRN_STREET_RIVA`. Update this constant at the top of the script before running.

---

### Execution Order and Data Flow

```
scratch.gdb/TRN_STREET_RIVA  ←──── Step 1 (INSERT new HRM streets)
         │
         ├── Step 2 (UPDATE DATE_RET / DATE_REV / OLD_FDMID on retired rows)
         │
         ├── Step 3 (UPDATE SHAPE_LENGTH / SHORT_DESC / LONG_DESC / DATE_REV on changed rows)
         │
         ├── Step 4 (QA: report null SHORT_DESC / LONG_DESC in TBL_new_streets_for_riva)
         │
         └── Step 5 (TRUNCATE + LOAD ASSET_ACCOUNTING.TRN_STREET_RIVA)
```

**Important:** Steps 2–5 all receive the same local RIVA copy path (`new_riva_streets`) returned by Step 1. The ETL steps are commented out in `__main__` by default — uncomment selectively before running.

---

## Open Questions

| Question | Status |
|---|---|
| What is the reload frequency for `TRN_STREET_ASSETS`? Is this on a schedule or ad-hoc? | Unknown — appears to be manual / on-demand |
| What populates `SURF_MAT`, `SDI`, `PAVE_WIDTH`, `RATE_DATE`, `NUM_CURB`, `WIDTH2`, `BASEVAL`, `SURFVAL` in `TRN_STREET_ASSETS`? These fields exist in the schema but are not populated by the ETL. | Unknown — likely populated by the asset accounting system separately |
