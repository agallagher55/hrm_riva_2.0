import sys
import os
import datetime
import time
import traceback
import logging
import arcpy

from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from configparser import ConfigParser
from HRMutils import (setupLog, send_mail, sql_script)

# Directories
WORKING_DIR = os.path.dirname(sys.path[0])

WD_FOLDER_NAME = os.path.basename(WORKING_DIR)
FILE_NAME = os.path.basename(__file__) 
FILE_NAME_BASE = os.path.splitext(FILE_NAME)[0]
SCRIPTS_DIR = os.path.join(WORKING_DIR, "Scripts")
SCRATCH_DIR = os.path.join(WORKING_DIR, "Scratch")
SQL_DIR = os.path.join(SCRIPTS_DIR, "SQL")

LRS_EXPORT_DIR = fr"\\msfs202.hrm.halifax.ca\common\hrmshare\ICT\ICT BIDS\ETL Data Exchange\LRS"

# Config Parser
config = ConfigParser()
config.read(r"E:\HRM\Scripts\Python\config.ini")

config_gdb = ConfigParser()
config_gdb.read(os.path.join(SCRIPTS_DIR, "gdb_config.ini"))

# Logging
log_file = os.path.join(config.get('LOGGING', 'logDir'), WD_FOLDER_NAME, f"{str(datetime.date.today())}_{FILE_NAME_BASE}.log")
logger = setupLog(log_file)
log_server = config.get('LOGGING', 'serverName')

console_handler = logging.StreamHandler()
log_formatter = logging.Formatter('%(asctime)s | %(levelname)s | FUNCTION: %(funcName)s | Msgs: %(message)s', datefmt='%d-%b-%y %H:%M:%S')
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)  # print logs to console

# Environment variables
arcpy.SetLogHistory(False)
arcpy.env.overwriteOutput = True

# Local variables
SDEADM_RW = config.get('SDEADM_RW', 'sdeFile')
SDEADM_RO = config.get('SDEADM_RO', 'sdeFile')
WEB_RO = config.get('GISAppFS', 'webRO')

SCRATCH_GDB = os.path.join(SCRATCH_DIR, "Scratch.gdb")
LRS_GDB = os.path.join(LRS_EXPORT_DIR, "lrs_view_data.gdb")

ASSET_ACCOUNTING_RO = config.get('AssetAccounting_RO', 'sdeFile')
PARCELS_SDE_RO = os.path.join(SDEADM_RO, "SDEADM.LND_parcels", "SDEADM.LND_parcel_polygon")
HRM_PARCELS_SDE_RO = os.path.join(SDEADM_RO, "SDEADM.LND_parcel_polygon_HRM")
MARITIMES_CLIP_SDE_RO = os.path.join(SDEADM_RO, "SDEADM.ADM_maritimes_clip")
STREET_ROUTES_SDE_RO = os.path.join(SDEADM_RO, "SDEADM.TRN_streets_routes")

HRM_Bndy_Export = os.path.join(SCRATCH_GDB, "HRM_Boundary")
Parcel_Export = os.path.join(SCRATCH_GDB, "LND_parcel_polygon")
Parcel_Clip_Export = os.path.join(SCRATCH_GDB, "LND_parcel_polygon_HRM")

portal_user = config.get("Portal_Admin", "username")
portal_pw = config.get("Portal_Admin", "password")
portal_url = config.get("Portal_Admin", "url")

logging_separator = "-" * 50


class LicenseError(Exception):
    pass


def run_error_processing(error_message):
    logger.info("Handling Error...")

    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]

    pymsg = "PYTHON ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n    " + \
            str(sys.exc_info()[0]) + ": " + str(sys.exc_info()[1]) + "\n"

    logger.error(pymsg)
    logger.info(error_message)

    arcpy_msgs = "GP ERRORS:\n" + arcpy.GetMessages(2) + "\n"

    if arcpy_msgs:
        logger.error(arcpy_msgs)

    # send_mail(
    #     to=str(config.get('EMAIL', 'recipients')).split(','),
    #     subject=f"{log_server}: GDB_Replication.py",
    #     text=f"\n\n{error_message}"
    #     f"\n\n{pymsg}"
    #     f"\n\n{arcpy_msgs}"
    # )


def execute_sqls(instance, sql_scripts):

    logger.info(f"Executing SQLs...")

    for count, sql_file in enumerate(sql_scripts, start=1):

        try:
            logger.info(f"{count}/{len(sql_scripts)}) Executing '{sql_file}'")

            conn = arcpy.ArcSDESQLExecute(SDEADM_RW)

            # Read sql file to get sql commands
            sql_file_path = os.path.join(SQL_DIR, sql_file)
            with open(sql_file_path, "r") as sfile:
                sql = sfile.read()

            if "RO" in instance:
                conn = arcpy.ArcSDESQLExecute(SDEADM_RO)
                logger.info("Instance: RO")

            else:
                logger.info("Instance: RW")

            query_result = conn.execute(sql)

            if query_result:
                logger.info(f"Query result successful: {query_result}")

            else:
                error_message = "Not sure if SQL ran successfully. Check results."

                if error_message:
                    logger.error(error_message)

            logger.info(f"\n{logging_separator}\n")

        except:
            run_error_processing(f"Error processing {sql_file}.")


def sync_replicas(instance):

    logger.info("Running Replication Process...")

    source_connection = SDEADM_RW
    dest_connection = SDEADM_RO

    replicas = str(config_gdb.get(instance, 'replicas')).split(',')
    logger.info(f"Replicas: {', '.join(replicas)}")

    for replica in replicas:
        logger.info(f"Syncing replica: {replica}...")

        try:
            logger.info(replica)
            arcpy.SynchronizeChanges_management(
                source_connection,
                replica,
                dest_connection,
                "FROM_GEODATABASE1_TO_2",
                "IN_FAVOR_OF_GDB1",
                "BY_OBJECT",
                "DO_NOT_RECONCILE"
            )

        except:
            run_error_processing(f"Error syncing replica {replica}.")


def populate_enc_coord_data(reference_data, web_ro_feature: str = f"LND_CIVIC_ADDRESS",
                            update_feature: str = "SDEADM.LND_CIVIC_ADDRESS_COORDS",
                            id_field: str = "CIV_ID"):
    logger.info(" ")
    logger.info(f"Updating coord data for {update_feature} feature in RO...")
    logger.info(logging_separator)

    with arcpy.EnvManager(workspace=SDEADM_RO):

        # Truncate COORDINATES_TABLE
        logger.info(f"Truncating {update_feature}...")
        arcpy.TruncateTable_management(update_feature)

        # Append data
        logger.info(f"Appending Data into '{update_feature}' from '{web_ro_feature}'...")
        arcpy.Append_management(
            inputs=os.path.join(WEB_RO, web_ro_feature),
            target=update_feature,
            schema_type="NO_TEST",
        )

        with arcpy.da.UpdateCursor(
                update_feature,
                [id_field, "LONGITUDE_WGS", "LATITUDE_WGS", "X_COORDINATE", "Y_COORDINATE"]
        ) as cursor:

            for row in cursor:

                id = row[0]

                # Get reference data for row
                update_row_info = reference_data[id]

                if update_row_info:
                    row[1] = round(reference_data[id]['LONGITUDE_WGS'], 8)
                    row[2] = round(reference_data[id]['LATITUDE_WGS'], 8)
                    row[3] = round(reference_data[id]['X_COORDINATE'], 8)
                    row[4] = round(reference_data[id]['Y_COORDINATE'], 8)

                    cursor.updateRow(row)


def get_enc_coord_data(web_feature: str = "LND_CIVIC_ADDRESS", id_field: str = "CIV_ID"):
    logger.info(" ")
    logger.info(f"Getting coord data for {web_feature} feature in RO...")
    logger.info(logging_separator)

    results = dict()

    with arcpy.EnvManager(workspace=WEB_RO):

        feature_fields = [
            x.name for x in arcpy.ListFields(web_feature)
            if x.type not in ('GlobalID', 'OID',) and 'SHAPE' not in x.name.upper()
        ]

        if id_field not in feature_fields:
            raise IndexError(f"ID field, {id_field} not found!")

        else:
            feature_fields.remove(id_field)

        cursor_fields = [id_field, "SHAPE@XY", "SHAPE@"] + feature_fields

        # Use a SearchCursor to iterate through each row in the feature class
        with arcpy.da.SearchCursor(web_feature, cursor_fields) as cursor:

            for row in cursor:
                id = row[0]
                x_original, y_original = row[1]  # Original X and Y
                shape = row[2]  # Geometry object

                # Transform the geometry to WGS 84 (SRID: 4326)
                shape_wgs84 = shape.projectAs(arcpy.SpatialReference(4326))

                # Extract the transformed X and Y (Longitude and Latitude)
                longitude = shape_wgs84.centroid.X
                latitude = shape_wgs84.centroid.Y

                # Build data dictionary
                results[id] = {
                    "LONGITUDE_WGS": longitude,
                    "LATITUDE_WGS": latitude,
                    "X_COORDINATE": x_original,
                    "Y_COORDINATE": y_original
                }

        return results


def update_pid_owner_ro(rw_sdeadm, ro_sdeadm):
    logger.info(f"Updating PID_OWNER in '{ro_sdeadm}' from '{rw_sdeadm}'...")

    ro_pidowner = os.path.join(ro_sdeadm, "SDEADM.PID_OWNER")
    rw_pidowner = os.path.join(rw_sdeadm, "SDEADM.PID_OWNER")

    ro_conn = arcpy.ArcSDESQLExecute(ro_sdeadm)

    # Truncate
    sql_truncate = """
    TRUNCATE TABLE SDEADM.PID_OWNER
    """

    ro_conn.execute(sql_truncate)

    # Load
    arcpy.Append_management(
        inputs=rw_pidowner,
        target=ro_pidowner,
        schema_type="TEST"
    )

    return ro_pidowner


def append_feature(input_feature, target_feature, sde_conn=SDEADM_RO):
    logger.info(f"Updating '{target_feature}' from '{input_feature}'...")

    ro_feature_name = arcpy.Describe(target_feature).name
    target_feature = os.path.join(sde_conn, ro_feature_name)

    with arcpy.EnvManager(preserveGlobalIds=True, workspace=sde_conn):
        # Truncate RO
        arcpy.TruncateTable_management(target_feature)

        # Load
        arcpy.Append_management(
            inputs=input_feature,
            target=target_feature,
            schema_type="NO_TEST"
        )

        return True


def get_coord_data(web_ro_db):

    logger.info(" ")
    logger.info("Getting coord data for SDEADM.LND_CIVIC_ADDRESS_COORDS feature in RO...")
    logger.info(logging_separator)

    civic_feature = "LND_CIVIC_ADDRESS"

    results = list()

    try:

        with arcpy.EnvManager(workspace=web_ro_db):

            # Use a SearchCursor to iterate through each row in the feature class
            with arcpy.da.SearchCursor(civic_feature, ["CIV_ID", "SHAPE@XY", "SHAPE@"]) as cursor:

                for row in cursor:

                    civ_id = row[0]
                    x_original, y_original = row[1]  # Original X and Y
                    shape = row[2]  # Geometry object

                    # Transform the geometry to WGS84 (SRID: 4326)
                    shape_wgs84 = shape.projectAs(arcpy.SpatialReference(4326))

                    # Extract the transformed X and Y (Longitude and Latitude)
                    longitude = shape_wgs84.centroid.X
                    latitude = shape_wgs84.centroid.Y

                    if all([longitude, latitude, x_original, y_original]):

                        # Store the results in the results list
                        results.append(
                            {
                                "CIV_ID": civ_id,
                                "LONGITUDE": longitude,
                                "LATITUDE": latitude,
                                "X_COORDINATE": x_original,
                                "Y_COORDINATE": y_original
                            }
                        )

        return results

    except:
        run_error_processing("Error getting LND_CIVIC_ADDRESS_COORDS feature in RO")


def populate_coord_data(sdeadm_ro, reference_data, civics_coords_feature="SDEADM.LND_CIVIC_ADDRESS_COORDS"):

    logger.info(" ")
    logger.info(f"Updating coord data for {civics_coords_feature} feature in RO...")
    logger.info(logging_separator)

    try:
        reference_data_count = len(reference_data)

        logger.info(f"reference_data_count: {reference_data_count}")

        if reference_data_count < 160000:
            logger.info(f"Doesn't seem to be enough civic records to update. Skipping load...")

        else:

            with arcpy.EnvManager(workspace=sdeadm_ro):

                arcpy.TruncateTable_management(civics_coords_feature)

                with arcpy.da.InsertCursor(
                        civics_coords_feature,
                        ["CIV_ID", "LONGITUDE", "LATITUDE", "X_COORDINATE", "Y_COORDINATE"]
                ) as cursor:

                    for row in reference_data:
                        cursor.insertRow(
                            [
                                row["CIV_ID"], row["LONGITUDE"], row["LATITUDE"], row["X_COORDINATE"],
                                row["Y_COORDINATE"],
                            ]
                        )

                # Record count
                logger.info(
                    f"{civics_coords_feature} record count: {arcpy.GetCount_management(civics_coords_feature)[0]}")
                return reference_data_count

    except:
        run_error_processing(f"Error updating {civics_coords_feature} feature in RO")


def update_annotations(rw_sdeadm, ro_sdeadm):
    """
    # Process - Update Street Annotation because we can't add Feature Annotation to Replicas  - PM 20231005

    :return:
    """

    logger.info(" ")
    logger.info("Street Annotation update in RO...")
    logger.info(logging_separator)

    street_routes = "SDEADM.TRN_streets_routes"

    annotations = [
        "TRN_street_anno_5k_on",
        "TRN_street_anno_10k_atlas",
        "TRN_street_anno_10k_on",
        "TRN_street_anno_20k_atlas",
        "TRN_street_anno_20k_on",
        "TRN_street_anno_40k_atlas"
    ]

    # Loop through each annotation dataset to perform the operations
    for anno in annotations:

        try:
            full_rw_path = os.path.join(rw_sdeadm, street_routes, anno)
            full_ro_path = os.path.join(ro_sdeadm, street_routes, anno)

            logger.info(f"Truncate and Append {anno} data......")
            arcpy.DeleteRows_management(full_ro_path)

            arcpy.Append_management(full_rw_path, full_ro_path, "NO_TEST")

        except Exception as e:
            run_error_processing(f"Error updating {anno}. Details: {str(e)}")

if __name__ == '__main__':

    start_time = time.asctime(time.localtime(time.time()))
    logger.info(f"Start: {start_time}")

    logger.info(logging_separator)
    logger.info("ROSDE Replication...")
    logger.info(logging_separator)

    try:

        sync_replicas("SDEADM_RO")

    except:
        run_error_processing(f"Error syncing replicas.")

    logger.info("Updating RO features (outside of replication)...")

    try:
        # Manually update RO features
        for feature in [

            "SDEADM.BLD_EMO_CONTACT",
            "SDEADM.BLD_EMO_POTENTIAL_SHELTER",
            "SDEADM.BLD_EMO_SPECIAL_POPULATION",
            "SDEADM.BLD_EMO_TEMPORARY_MORGUE",

            "SDEADM.LND_PARCEL_GOVOWN_LOOKUP",
            "SDEADM.LND_demographic_scenarios",

            "SDEADM.LND_ENCAMPMENT_LOG",
            "SDEADM.LND_res_rental_registry",

            "SDEADM.TRNLRS_TRN_ICE_ROUTE_VW"

        ]:
            rw_tbl = os.path.join(SDEADM_RW, feature)
            ro_tbl = os.path.join(SDEADM_RO, feature)

            append_feature(rw_tbl, ro_tbl)

    except:
        run_error_processing(f"Error updating {feature}")

    try:

        logger.info(logging_separator)
        logger.info("RWSDE SQL")
        logger.info(logging_separator)

        # Execute RWSDE SQL
        execute_sqls(
            "SDEADM_RW",
            str(config_gdb.get("SDEADM_RW", 'sqlScripts')).split(',')
        )

        logger.info("\n\n\n\n")

        logger.info(logging_separator)
        logger.info("ROSDE SQL")
        logger.info(logging_separator)

        # Execute HRM ROSDE SQL
        execute_sqls(
            "SDEADM_RO",
            str(config_gdb.get("SDEADM_RO", 'sqlScripts')).split(',')
        )

    except:
        run_error_processing(f"Error executing SQLs")

    # NEW (September 2023) - Update PID_OWNER in RO using truncate and load. Load from RW.
    try:
        update_pid_owner_ro(rw_sdeadm=SDEADM_RW, ro_sdeadm=SDEADM_RO)

    except:
        run_error_processing(f"Error updating PID_OWNER")

    logger.info("\n\n\n\n")

    logger.info("Updating coordinates...")

    try:

        civics_coord_table = "SDEADM.LND_CIVIC_ADDRESS_COORDS"

        civ_coord_data = get_coord_data(WEB_RO)
        appended_record_count = populate_coord_data(SDEADM_RO, civ_coord_data, civics_coord_table)

        if appended_record_count < 160000:
            run_error_processing(
                f"Only {appended_record_count} records were going available to load {civics_coord_table}. Load skipped.")

        # NEW - July 12, 2023
        encampment_features = {
            "LND_encampment_locations": "ENC_ID",
            "LND_encampment_sites": "ES_ID",
        }

        for feature in encampment_features:
            logger.info(feature)

            # Get (WGS) coordinates from RO feature using a search cursor
            # Use insert cursor to insert data into COORDINATES_TABLE

            feature_id = encampment_features[feature]

            coord_data = get_enc_coord_data(
                web_feature=feature,
                id_field=feature_id
            )

            # Update coordinate data
            populate_enc_coord_data(
                coord_data,
                web_ro_feature=feature,
                update_feature=f"SDEADM.{feature}_coords",
                id_field=feature_id
            )

    except:
        run_error_processing(f"Error calculating WGS coordinates.")

    # NEW - October 5, 2023
    try:
        update_annotations(SDEADM_RW, SDEADM_RO)

    except:
        run_error_processing(f"Error updating annotations.")

    # TODO: Each of the processes below could be turned into functions for isolated management

    # Process: Dissolve building polygon....
    logger.info(" ")
    logger.info("Dissolve building polygon...")
    logger.info(logging_separator)

    BuildingPolygon_Select_Scratch = os.path.join(SCRATCH_DIR, "Select_Building_Polygon.shp")
    BuildingPolygon_Dissolve_Scratch = os.path.join(SCRATCH_DIR, "Dissolve_Building_Polygon.shp")
    BuildingPolygon_RO = os.path.join(SDEADM_RO, "SDEADM.BLD_building_polygon")
    BuildingPolygon_Dissolve_RO = os.path.join(SDEADM_RO, "SDEADM.BLD_building_polygon_dissolve")

    try:
        if arcpy.Exists(BuildingPolygon_Select_Scratch):
            arcpy.Delete_management(BuildingPolygon_Select_Scratch)

        logger.info("Copy BLD_building_polygon to Scratch ...")
        arcpy.CopyFeatures_management(BuildingPolygon_RO, BuildingPolygon_Select_Scratch)

        if arcpy.Exists(BuildingPolygon_Dissolve_Scratch):
            arcpy.Delete_management(BuildingPolygon_Dissolve_Scratch)

        logger.info("Dissolve BLD_building_polygon ...")

        # Modified to create a Single Part Dissolved building polygon - PM Sept 06, 2018
        arcpy.Dissolve_management(
            BuildingPolygon_Select_Scratch,
            BuildingPolygon_Dissolve_Scratch,
            "BL_ID",
            "",
            "SINGLE_PART",
            "DISSOLVE_LINES"
        )

        logger.info("Delete rows from BLD_building_polygon_dissolve ...")
        arcpy.TruncateTable_management(BuildingPolygon_Dissolve_RO)

        logger.info("Append rows to BLD_building_polygon_dissolve ...")
        arcpy.Append_management(BuildingPolygon_Dissolve_Scratch, BuildingPolygon_Dissolve_RO, "NO_TEST", "", "")

    except:
        run_error_processing("Error updating BLD_building_polygon_dissolve")

    logger.info(" ")
    logger.info("Projplan update...")
    logger.info(logging_separator)

    try:

        if arcpy.Exists(SCRATCH_GDB):
            arcpy.Delete_management(SCRATCH_GDB)
            logger.info(f"Deleted workspace: {SCRATCH_GDB}")

        arcpy.CreateFileGDB_management(SCRATCH_DIR, os.path.basename(SCRATCH_GDB))
        logger.info(f"Created workspace: {SCRATCH_GDB}")

        ProjPlan_Stage = os.path.join(SDEADM_RW, "SDEADM.LND_PARCEL_PROJPLAN_STAGE")
        ProjPlan_RWSDE = os.path.join(SDEADM_RW, "SDEADM.LND_parcel_projplan_fc")
        ProjPlan_ROSDE = os.path.join(SDEADM_RO, "SDEADM.LND_parcel_projplan_fc")
        ProjPlan_Export = os.path.join(SCRATCH_GDB, "ProjPlan_Export")

        logger.info("Copy data to Scratch...")

        if arcpy.Exists(ProjPlan_Export):
            arcpy.Delete_management(ProjPlan_Export)

        arcpy.FeatureClassToFeatureClass_conversion(ProjPlan_Stage, SCRATCH_GDB, "ProjPlan_Export")

        logger.info("Truncate RWSDE...")
        arcpy.TruncateTable_management(ProjPlan_RWSDE)

        logger.info("Append to RWSDE...")
        arcpy.Append_management(ProjPlan_Export, ProjPlan_RWSDE, "NO_TEST")

        logger.info("Truncate ROSDE...")
        arcpy.TruncateTable_management(ProjPlan_ROSDE)

        logger.info("Append to ROSDE...")
        arcpy.Append_management(ProjPlan_Export, ProjPlan_ROSDE, "NO_TEST")

    except:
        run_error_processing(f"Error updating {ProjPlan_ROSDE}.")

    logger.info(" ")
    logger.info("Parcel Owner update...")
    logger.info(logging_separator)

    try:
        parcel_owner_stage = os.path.join(SDEADM_RO, "SDEADM.LND_PARCEL_OWNER_STAGE")
        parcel_owner = os.path.join(SDEADM_RO, "SDEADM.LND_parcel_owner")

        parcel_owner_export = os.path.join(SCRATCH_GDB, "Owner_Export")

        logger.info("Copy parcel owner data to Scratch...")

        if arcpy.Exists(parcel_owner_export):
            arcpy.Delete_management(parcel_owner_export)

        arcpy.FeatureClassToFeatureClass_conversion(parcel_owner_stage, SCRATCH_GDB, "Owner_Export")

        # Parcel Owner
        logger.info(f"Truncate ROSDE table {parcel_owner}...")
        arcpy.TruncateTable_management(parcel_owner)

        logger.info(f"Append {parcel_owner_export} to ROSDE table {parcel_owner}...")
        arcpy.Append_management(parcel_owner_export, parcel_owner, "NO_TEST")

    except:
        run_error_processing(f"Error updating {parcel_owner}.")

    logger.info(" ")
    logger.info("EMO views...")
    logger.info(logging_separator)

    try:
        emo_datasets = {
            'BLD_EMO_MORGUE_STAGE': 'BLD_emo_morgue_fc',
            'BLD_EMO_SPEC_POP_STAGE': 'BLD_emo_spec_pop_fc',
            'BLD_EMO_SHELTER_STAGE': 'BLD_emo_shelter_fc',
            'BLD_DOCUMENT_STAGE': 'BLD_document_fc',
        }

        for staging_feature, final_feature in emo_datasets.items():

            STAGING_FEATURE_PATH_RO = os.path.join(SDEADM_RO, "SDEADM." + staging_feature)
            FINAL_FEATURE_PATH_RO = os.path.join(SDEADM_RO, "SDEADM." + final_feature)
            FINAL_FEATURE_PATH_RW = os.path.join(SDEADM_RW, "SDEADM." + final_feature)

            logger.info(f"Truncate {final_feature}...")
            arcpy.TruncateTable_management(FINAL_FEATURE_PATH_RO)

            logger.info(f"ROSDE: Append {staging_feature} to {final_feature}...")
            arcpy.Append_management(
                STAGING_FEATURE_PATH_RO,
                FINAL_FEATURE_PATH_RO,
                "NO_TEST"
            )

            logger.info(f"Truncate {final_feature}...")
            arcpy.TruncateTable_management(FINAL_FEATURE_PATH_RW)

            logger.info(f"RWSDE: Append {staging_feature} to {final_feature}...")
            arcpy.Append_management(
                STAGING_FEATURE_PATH_RO,
                FINAL_FEATURE_PATH_RW,
                "NO_TEST"
            )

    except:
        run_error_processing(f"Error updating {final_feature}.")

    # Process - Parking lot points FK 20151130 Modified by PM 20170726
    logger.info(" ")
    logger.info("Parking lot point update...")
    logger.info(logging_separator)

    PARKING_LOT_POINT_SCRATCH = os.path.join(SCRATCH_GDB, "TRN_parking_lot_point")
    PARKING_LOT_RO = os.path.join(SDEADM_RO, "SDEADM.TRN_parking_lot")
    PARKING_LOT_POINT_RO = os.path.join(SDEADM_RO, "SDEADM.TRN_parking_lot_point")
    PARKING_LOT_POINT_RW = os.path.join(SDEADM_RW, "SDEADM.TRN_parking_lot_point")

    try:
        logger.info("Copy parking lots to scratch...")
        if arcpy.Exists(PARKING_LOT_POINT_SCRATCH):
            arcpy.Delete_management(PARKING_LOT_POINT_SCRATCH)

        arcpy.FeatureToPoint_management(
            PARKING_LOT_RO,
            PARKING_LOT_POINT_SCRATCH,
            "INSIDE"
        )

        logger.info("Truncate TRN_parking_lot_point in RO...")
        arcpy.TruncateTable_management(PARKING_LOT_POINT_RO)

        logger.info("Append to TRN_parking_lot_point in RO...")
        arcpy.Append_management(
            PARKING_LOT_POINT_SCRATCH,
            PARKING_LOT_POINT_RO,
            "NO_TEST"
        )

        logger.info("Delete rows in TRN_parking_lot_point in RW...")
        arcpy.DeleteRows_management(in_rows=PARKING_LOT_POINT_RW)

        logger.info("Append to TRN_parking_lot_point in RW...")
        arcpy.Append_management(
            PARKING_LOT_POINT_SCRATCH,
            PARKING_LOT_POINT_RW,
            "NO_TEST"
        )

    except:
        run_error_processing(f"Error updating {PARKING_LOT_POINT_RW}")

    # Process - Update BLD_Function_LUT table for FDM ODS - PM	20171018
    logger.info(" ")
    logger.info("BLD_Function_LUT update...")
    logger.info(logging_separator)

    try:
        LUT_Table = os.path.join(SDEADM_RO, "BLD_FUNCTION_LUT")
        logger.info("Truncate BLD_FUNCTION_LUT in ROSDE...")

        if arcpy.Exists(LUT_Table):
            arcpy.TruncateTable_management(LUT_Table)

        logger.info("Copy Building Use domain data to Scratch...")
        domList = ["Bldg_BLAF_uses", "Bldg_BLCM_uses", "Bldg_BLID_uses", "Bldg_BLIS_uses", "Bldg_BLIT_uses",
                   "Bldg_BLRC_uses", "Bldg_BLRS_uses", "Bldg_BLTR_uses"]

        for dom in domList:
            DOM_PATH_SCRATCH = os.path.join(SCRATCH_GDB, dom)

            if arcpy.Exists(DOM_PATH_SCRATCH):
                arcpy.Delete_management(DOM_PATH_SCRATCH)
            arcpy.DomainToTable_management(SDEADM_RO, dom, DOM_PATH_SCRATCH, "CODE", "DESCRIP")

        logger.info("Append domain values to BLD_FUNCTION_LUT in ROSDE...")

        lutList = [
            "Bldg_BLAF_uses", "Bldg_BLCM_uses", "Bldg_BLID_uses", "Bldg_BLIS_uses",
            "Bldg_BLIT_uses", "Bldg_BLRC_uses", "Bldg_BLRS_uses", "Bldg_BLTR_uses"
        ]

        for lut in lutList:
            arcpy.arcpy.Append_management(
                os.path.join(SCRATCH_GDB, lut), LUT_Table, "NO_TEST", "", ""
            )

    except:
        run_error_processing(f"Error updating {LUT_Table}.")

    # Process - Convert Views to FC PM 20170727
    logger.info(" ")
    logger.info("Views into FC...")
    logger.info(logging_separator)

    wgs84Datasets = {
        'LND_PARCEL_GOVOWN_VW': 'LND_PARCEL_GOVOWN_fc',
        'LND_PARCEL_LRIS_VW': 'LND_PARCEL_LRIS_fc',
    }

    for staging_fc, fc in wgs84Datasets.items():

        FC_PATH_RO = os.path.join(SDEADM_RO, "SDEADM." + fc)
        STAGING_FC_PATH_RO = os.path.join(SDEADM_RO, "SDEADM." + staging_fc)

        try:

            logger.info(f"Truncate {fc}...")
            arcpy.TruncateTable_management(FC_PATH_RO)

            logger.info(f"ROSDE: Append {staging_fc} to {fc}...")
            arcpy.Append_management(
                STAGING_FC_PATH_RO, FC_PATH_RO, "NO_TEST"
            )

        except:
            run_error_processing(f"Error updating {staging_fc, FC_PATH_RO}")

    # Process - Update TRN_Traffic_Collision_OD table for Open Data. Dec 16, 2020 - MP
    # This particualr query is not supported in file geodatabases so we need to use the feature class from SDE to perform the query on then project the data before updating web_RO.
    logger.info(" ")
    logger.info("TRN_Traffic_Collision_OD update...")
    logger.info(logging_separator)

    try:
        Traffic_Collision_SDE = os.path.join(SDEADM_RO, "TRN_Traffic_Collision")
        Traffic_Collision_OD = os.path.join(WEB_RO, "TRN_Traffic_Collision_OD")
        Traffic_Collision_temp = os.path.join(SCRATCH_GDB, "TRN_Traffic_Collision")

        logger.info("Delete Staging Traffic Collision data...")
        if arcpy.Exists(Traffic_Collision_temp):
            arcpy.Delete_management(Traffic_Collision_temp)

        logger.info("Query and copy Traffic Collision data to Scratch...")
        arcpy.MakeFeatureLayer_management(Traffic_Collision_SDE, "TRN_traffic_collision_Layer",
                                          "ACCIDENT_DATE >= '2018-01-01 00:00:00' AND ACCIDENT_DATE <= EOMONTH(DATEADD(DAY, 1, GETDATE()), -2) AND ROW_FLAG = 1")

        # Sort based on Accident Date. Sept 29, 2021 - MP
        arcpy.Sort_management("TRN_traffic_collision_Layer", "in_memory/TRN_traffic_collision_Layer_Sorted",
                              [["ACCIDENT_DATE", "DESCENDING"]])

        # Project to WGS84 since we are no longer using WEBGIS. Nov 1, 2024 - MP
        logger.info("Project and create FC from selection......")
        arcpy.management.Project("in_memory/TRN_traffic_collision_Layer_Sorted", Traffic_Collision_temp, 3857,
                                 'NAD83_CSRS_1997_to_NAD83_CSRS_2010 + NAD_1983_CSRS_To_WGS_1984_2')

        if arcpy.Exists(Traffic_Collision_OD):
            logger.info("Truncate and Append Traffic Collision data...")
            arcpy.TruncateTable_management(Traffic_Collision_OD)

            arcpy.arcpy.Append_management(Traffic_Collision_temp, Traffic_Collision_OD, "NO_TEST", "", "")

        else:
            logger.info("Traffic Collision data does not exist in web_RO...creating new feature class...")
            arcpy.conversion.ExportFeatures(Traffic_Collision_temp, Traffic_Collision_OD)

    except:
        run_error_processing(f"Error updating {Traffic_Collision_OD}.")

    # Process - Update LND_Park_Recreation_Feature_OD table for Open Data. May 6, 2024 - MP
    # The BU does not want the ASSETSTAT field in Open Data so we cannot use this definition query in the Pro document and is why we need to create this feature class.
    logger.info(" ")
    logger.info("LND_Park_Recreation_Feature_OD update...")
    logger.info(logging_separator)

    try:
        ParkRecreationFeature_WGS84 = os.path.join(WEB_RO, "LND_park_recreation_feature")
        ParkRecreationFeature_OD = os.path.join(WEB_RO, "LND_park_recreation_feature_OD")
        ParkRecreationFeature_temp = os.path.join(SCRATCH_GDB, "LND_park_recreation_feature")

        logger.info("Delete Staging Park Recreation Feature data...")
        if arcpy.Exists(ParkRecreationFeature_temp):
            arcpy.Delete_management(ParkRecreationFeature_temp)

        logger.info("Query and copy Park Recreation Feature data to Scratch...")
        arcpy.conversion.ExportFeatures(ParkRecreationFeature_WGS84, ParkRecreationFeature_temp, "ASSETSTAT = 'INS'")

        if arcpy.Exists(ParkRecreationFeature_OD):
            logger.info("Truncate and Append Park Recreation Feature data......")
            arcpy.TruncateTable_management(ParkRecreationFeature_OD)

            arcpy.arcpy.Append_management(ParkRecreationFeature_temp, ParkRecreationFeature_OD, "NO_TEST", "", "")

        else:
            logger.info("Park Recreation Feature data does not exist in web_RO...creating new feature class...")
            arcpy.conversion.ExportFeatures(ParkRecreationFeature_temp, ParkRecreationFeature_OD)

    except:
        run_error_processing(f"Error updating {ParkRecreationFeature_OD}.")

    # Process - Clip parcel data to HRM Boundary for Posse - PM	20220215
    logger.info(" ")
    logger.info("Parcels in HRM...")
    logger.info(logging_separator)

    try:

        logger.info("Copy data to Scratch...")

        if arcpy.Exists(Parcel_Export):
            arcpy.Delete_management(Parcel_Export)

        if arcpy.Exists(HRM_Bndy_Export):
            arcpy.Delete_management(HRM_Bndy_Export)

        if arcpy.Exists(Parcel_Clip_Export):
            arcpy.Delete_management(Parcel_Clip_Export)

        arcpy.CopyFeatures_management(PARCELS_SDE_RO, Parcel_Export)

        logger.info("Query and copy HRM Boundary to Scratch...")
        arcpy.MakeFeatureLayer_management(MARITIMES_CLIP_SDE_RO, "HRM_Boundary_Layer", "COUNTY = 'HALIFAX'")
        arcpy.CopyFeatures_management("HRM_Boundary_Layer", HRM_Bndy_Export)
        logger.info("Clip Parcels by HRM Boundary...")

        arcpy.Clip_analysis(Parcel_Export, HRM_Bndy_Export, Parcel_Clip_Export)

        logger.info("Truncate ROSDE...")
        arcpy.TruncateTable_management(HRM_PARCELS_SDE_RO)

        logger.info("Append to ROSDE...")
        arcpy.Append_management(Parcel_Clip_Export, HRM_PARCELS_SDE_RO, "NO_TEST")

    except:
        run_error_processing(f"Error updating {HRM_PARCELS_SDE_RO}.")

    # Close the Log File:
    end_time = time.asctime(time.localtime(time.time()))
    logger.info(logging_separator)
    logger.info(f"End: {end_time}")
