--------------------------------------------------------------------------------------------------
-- Create new table --> TRN_STREET_RIVA_STAGE
--TRUNCATE TABLE SDEADM.TRN_STREET_RIVA_STAGE;
-- DROP TABLE SDEADM.TRN_STREET_RIVA_STAGE;
--
--SELECT * INTO SDEADM.TRN_STREET_RIVA_STAGE
--FROM SDEADM.TRN_STREET_RIVA;
--
--ALTER TABLE SDEADM.TRN_STREET_RIVA_STAGE
--DROP COLUMN OBJECTID;

--------------------------------------------------------------------------------------------------

TRUNCATE TABLE SDEADM.TRN_STREET_RIVA_STAGE;

-- Step 1 - Determine what new streets have been added to TRN_street that do not exist in TRN_STREET_RIVA
-- Get all HRM streets that are NOT already in current TRN_street_riva
-- Don't include retired streets

INSERT INTO SDEADM.TRN_STREET_RIVA_STAGE (
    STR_CODE, STR_NAME, STR_TYPE, FULL_NAME, STR_STATUS, PST_CLASS, OWN,
    DATE_ACCEPT, SOURCE, FDMID, OLD_FDMID, GSA_NAME, DATE_ACT, SYS_DATE, SHAPE_LENGTH
)
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
    trn_streets.SHAPE.STLength() AS SHAPE_LENGTH
FROM SDEADM.TRN_STREET trn_streets
LEFT JOIN SDEADM.TRN_STREET_RIVA riva_streets
    ON trn_streets.FDMID = riva_streets.FDMID
WHERE
    trn_streets.OWN LIKE 'HRM'  -- Select HRM-owned streets
    AND (
        riva_streets.FDMID IS NULL -- Ensures only new streets are inserted
        AND riva_streets.DATE_RET IS NULL
    );


-- Step 2 - Updating records in TRN_STREET_RIVA that have been retired and are now archived in TRN_street_retired:

-- retired streets - (NOT IN RO)

-- Update DATE_RET in riva_streets from trn_streets --> ADD TRN_STREET_RETIRED TO READ_ONLY AND REPLICA
UPDATE riva_streets

SET riva_streets.DATE_RET = retired_streets.DATE_RET,
    riva_streets.DATE_REV = GETDATE(),  -- Sets DATE_REV to today's date
	riva_streets.OLD_FDMID = retired_streets.OLD_FDMID,
	riva_streets.SHAPE_LENGTH = retired_streets.SHAPE.STLength()

FROM SDEADM.TRN_STREET_RIVA_STAGE AS riva_streets

JOIN SDEADM.TRN_STREET_RETIRED AS retired_streets
    ON riva_streets.FDMID = retired_streets.FDMID

WHERE retired_streets.DATE_RET IS NOT NULL;


-- Step 3 Updating existing segments to reflect any changes in length, to and from street.  
UPDATE riva_streets
SET riva_streets.SHAPE_LENGTH = trn_streets.SHAPE.STLength(),  -- Update SHAPE_LENGTH
    riva_streets.Short_Desc = trn_streets.FULL_NAME + ' (' + trn_streets.FROM_STR + ' TO ' + trn_streets.TO_STR + ')',  -- Update Short_Desc
    riva_streets.Long_Desc = trn_streets.FULL_NAME + ' (' + trn_streets.GSA_LEFT + ')',  -- Update Long_Desc
    riva_streets.OLD_FDMID = trn_streets.OLD_FDMID,  -- Update OLD_FDMID
    riva_streets.DATE_REV = GETDATE()  -- Update DATE_REV to today's date

FROM SDEADM.TRN_STREET_RIVA_STAGE riva_streets
JOIN SDEADM.TRN_STREET trn_streets
	ON riva_streets.FDMID = trn_streets.FDMID
WHERE riva_streets.DATE_RET IS NULL AND
	riva_streets.SHAPE_LENGTH <> trn_streets.SHAPE.STLength()


-----------------------------------------
