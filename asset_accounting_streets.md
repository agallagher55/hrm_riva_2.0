# ASSET_ACCOUNTING — Street Assets

This document covers the two street-related objects in the ASSET_ACCOUNTING schema.

---

## Table of Contents

- [TRN\_STREET\_ASSETS](#trn_street_assets)
- [STREET\_ASSETS\_EXPORT\_VW](#street_assets_export_vw)
- [Open Questions](#open-questions)

---

## TRN_STREET_ASSETS

**Type:** Enterprise Geodatabase Table (physical)

**Purpose:** Asset accounting export layer for HRM-owned street segments. This is the table the asset accounting system reads for street data.

**Source:** `SDEADM.TRN_STREET_RIVA`

**Load process (manual, run on demand):**

1. Run `scripts/trn_street_assets.py` against PROD to sync `TRN_STREET_RIVA` with `TRN_STREET` (3-step ETL).
2. Connect to SQL Server PROD and run:

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

This is a full truncate-and-reload; there is no incremental update. Row counts should match between `TRN_STREET_RIVA` and `TRN_STREET_ASSETS` after the load.

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

The ETL orchestrator that keeps `TRN_STREET_RIVA` in sync with `TRNLRS_TRN_STREET_VW`. It runs four functions in sequence against a **local scratch.gdb** copy of RIVA; the edited local copy is then used as the source for the final truncate-and-reload into `ASSET_ACCOUNTING.TRN_STREET_ASSETS`.

### Global Configuration

| Variable | Value | Purpose |
|---|---|---|
| `SDE` | `.sde` file path | Active connection (dev/qa/prod RW) — edit before running |
| `TRN_STREET` | `SDEADM.TRNLRS_TRN_STREET_VW` | Authoritative street source (LRS view) |
| `TRN_STREET_RIVA` | `SDEADM.TRN_STREET_RIVA` | Working layer being updated |
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

### Step 2 — `step_two_update_retired_streets(trn_street_riva, local_gdb)`

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
| `LONG_DESC` | `FULL_NAME + " " + GSA_LEFT` |
| `OLD_FDMID` | `TRN_STREET.OLD_FDMID` |
| `DATE_REV` | `datetime.today()` |
| `DATE_ACT` | `TRN_STREET.DATE_ACT` |
| `SYS_DATE` | `TRN_STREET.SYS_DATE` |

---

### Step 4 — `step_four_validation_review(local_gdb)`

**Purpose:** QA check on the net-new streets inserted by Step 1. Reports null/blank counts for `SHORT_DESC` and `LONG_DESC` — fields that must not be empty before loading to `ASSET_ACCOUNTING`.

**Source:** `TBL_new_streets_for_riva` in `scratch.gdb` (created by Step 1).

**Output:** Printed summary — total row count and per-field null/blank count with percentage. Returns `null_counts` dict.

**Not a hard stop:** Step 4 does not halt or roll back; it reports problems for manual review. Any blanks flagged here should be QA'd before running the truncate-and-reload into `ASSET_ACCOUNTING.TRN_STREET_ASSETS`.

---

### Execution Order and Data Flow

```
scratch.gdb/TRN_STREET_RIVA  ←──── Step 1 (INSERT new HRM streets)
         │
         ├── Step 2 (UPDATE DATE_RET / DATE_REV / OLD_FDMID on retired rows)
         │
         ├── Step 3 (UPDATE SHAPE_LENGTH / SHORT_DESC / LONG_DESC / DATE_REV on changed rows)
         │
         └── Step 4 (QA: report null SHORT_DESC / LONG_DESC in TBL_new_streets_for_riva)
                    ↓
    Manual: TRUNCATE + INSERT into ASSET_ACCOUNTING.TRN_STREET_ASSETS
```

**Important:** Steps 2–4 all receive the same local RIVA copy path (`trn_street_riva_local`) returned by Step 1. The three ETL steps are commented out in `__main__` by default — uncomment selectively before running.

---

## Open Questions

| Question | Status |
|---|---|
| What is the reload frequency for `TRN_STREET_ASSETS`? Is this on a schedule or ad-hoc? | Unknown — appears to be manual / on-demand |
| What populates `SURF_MAT`, `SDI`, `PAVE_WIDTH`, `RATE_DATE`, `NUM_CURB`, `WIDTH2`, `BASEVAL`, `SURFVAL` in `TRN_STREET_ASSETS`? These fields exist in the schema but are not populated by the ETL. | Unknown — likely populated by the asset accounting system separately |
