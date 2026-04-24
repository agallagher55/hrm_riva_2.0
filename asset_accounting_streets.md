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

## Open Questions

| Question | Status |
|---|---|
| What is the reload frequency for `TRN_STREET_ASSETS`? Is this on a schedule or ad-hoc? | Unknown — appears to be manual / on-demand |
| What populates `SURF_MAT`, `SDI`, `PAVE_WIDTH`, `RATE_DATE`, `NUM_CURB`, `WIDTH2`, `BASEVAL`, `SURFVAL` in `TRN_STREET_ASSETS`? These fields exist in the schema but are not populated by the ETL. | Unknown — likely populated by the asset accounting system separately |
