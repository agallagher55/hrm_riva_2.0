-- use civics FDMID field

SELECT
    9999 AS STR_CODE, -- Placeholder for STR_CODE, can be updated as needed
    trn_streets.STR_NAME,
    trn_streets.STR_TYPE,
    trn_streets.FULL_NAME,
    trn_streets.STR_STATUS,
    trn_streets.ST_CLASS AS PST_CLASS,
    trn_streets.OWN,
    trn_streets.DATE_ACCEPT,
    trn_streets.SOURCE,
    trn_streets.FDMID,
    trn_streets.OLD_FDMID,
    trn_streets.GSA_LEFT AS GSA_NAME,
    trn_streets.DATE_ACT,
    trn_streets.SYS_DATE,
    trn_streets.SHAPE.STLength() AS SHAPE_LENGTH,
    hrm_parcels.PID, -- Assuming there's a unique parcel identifier
    STRING_AGG(acq_disp.ACQDISTYPE, ', ') AS ACQDISTYPE_AGG, -- Aggregates multiple values into one field
    STRING_AGG(acq_disp.TRANS_TYPE, ', ') AS TRANS_TYPE_AGG

FROM SDEADM.TRNLRS_TRN_STREET_VW trn_streets

-- Get intersecting parcels
LEFT JOIN SDEADM.LND_HRM_PARCEL hrm_parcels
ON hrm_parcels.SHAPE.STIntersects(trn_streets.SHAPE.STPointN(trn_streets.SHAPE.STNumPoints() / 2)) = 1

LEFT JOIN SDEADM.LND_HRM_PARCEL_HAS_ACQ_DISP AS has_acq_disp
ON has_acq_disp.ASSET_ID = hrm_parcels.ASSET_ID

-- Join to acq_disposal table
LEFT JOIN SDEADM.LND_ACQUISITION_DISPOSAL AS acq_disp
ON has_acq_disp.ACQDISP_ID = acq_disp.ACQDISP_ID

LEFT JOIN SDEADM.TRN_STREET_RIVA riva_streets
ON trn_streets.FDMID = riva_streets.FDMID

GROUP BY
    trn_streets.STR_NAME, trn_streets.STR_TYPE, trn_streets.FULL_NAME,
    trn_streets.STR_STATUS, trn_streets.ST_CLASS, trn_streets.OWN,
    trn_streets.DATE_ACCEPT, trn_streets.SOURCE, trn_streets.FDMID,
    trn_streets.OLD_FDMID, trn_streets.GSA_LEFT, trn_streets.DATE_ACT,
    trn_streets.SYS_DATE, trn_streets.SHAPE.STLength(), hrm_parcels.PID;
