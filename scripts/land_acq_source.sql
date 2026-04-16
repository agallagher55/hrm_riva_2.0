-- land_asset_transfrom_vw
-- delete current view
-- create new view
-- update schema of [ASSET_ACCOUNTING].[lnd_land_assets] to include ACQDIPSCOST field 
	--> sylvie/rajni/andrea will have to update the sql update script
		--> Sylvie email subject: RE: current land and street asset exports
		--> Should this workflow be migrated to GIS

-- ASSET_ACCOUNTING.LAND_ASSETS_EXPORT_VW
-- delete current view
-- recreate view

SELECT *
FROM [ASSET_ACCOUNTING].[lnd_land_assets]

SELECT *
FROM [ASSET_ACCOUNTING].[LAND_ASSETS_EXPORT_VW]

CREATE VIEW [ASSET_ACCOUNTING].[LAND_ASSETS_EXPORT_VW] AS
WITH LAND_RECORD_COUNT AS (
    SELECT 
        asset_id, 
        COUNT(*) AS asset_record_count
    FROM [ASSET_ACCOUNTING].[LND_LAND_ASSETS]
    GROUP BY asset_id
)
SELECT 
    D.ASSET_ID,
    D.GROUP_ID,
    D.ACQ_TYPE,
    D.PID,
    D.ACQ_COST,
    D.ASSET_TYPE,
    D.OWNER,
    D.DISPOSAL,
    D.MAIN_CLASS,
    CONVERT(VARCHAR(20), D.ACQ_DATE) AS ACQ_DATE,
    CONVERT(VARCHAR(20), D.DISP_DATE) AS DISP_DATE,
    D.DISP_TYPE,
	D.ACQDISPSOURCE, -- NEW
    D.LAND_NAME,
    D.HECTARES,
    D.SERV_CAT,
    D.PARK_ID,
    D.PARK_NAME,
    D.REPL_COST,
    D.HRWC_FLAG,
    C.ASSET_RECORD_COUNT,
    CONVERT(VARCHAR(20), CAST(D.HRM_PARCEL_ADDDATE AS DATE)) AS HRM_PARCEL_ADDDATE,
    CONVERT(VARCHAR(20), CAST(D.HRM_PARCEL_MODDATE AS DATE)) AS HRM_PARCEL_MODDATE
FROM [ASSET_ACCOUNTING].[LND_LAND_ASSETS] D
JOIN LAND_RECORD_COUNT C 
    ON D.ASSET_ID = C.ASSET_ID
WHERE C.ASSET_RECORD_COUNT = 1;
