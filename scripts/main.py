# ETL Process

import arcpy
import logging
import os

from datetime import datetime

from gispy import utils

# Settings
arcpy.SetLogHistory(False)
arcpy.env.overwriteOutput = True

SDE = r"E:\HRM\Scripts\SDE\SQL\Dev\dev_RW_sdeadm.sde"

# TRN_STREET = os.path.join(SDE, "SDEADM.TRN_streets_routes", "SDEADM.TRN_street")
TRN_STREET = os.path.join(SDE, "SDEADM.TRNLRS_TRN_STREET_VW")

TRN_STREET_RIVA = os.path.join(SDE, "SDEADM.TRN_STREET_RIVA")
TRNLRS_SEGMENTED = os.path.join(SDE, "SDEADM.TRNLRS_segmented_street_events")
E_STREET_STATUS = os.path.join(SDE, "SDEADM.TRNLRS", "SDEADM.E_StreetStatus")

PROJECT_DIR = os.getcwd()
SCRIPTS_DIR = os.path.join(PROJECT_DIR, "scripts")

# SDE = os.path.join(SCRIPTS_DIR, "prod_copy.gdb")

# Logger
logger = logging.getLogger("riva_etl")
logger.setLevel(logging.INFO)

_formatter = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_formatter)

_file_handler = logging.FileHandler(os.path.join(SCRIPTS_DIR, "riva_etl.log"), encoding="utf-8")
_file_handler.setFormatter(_formatter)

logger.addHandler(_console_handler)
logger.addHandler(_file_handler)


def step_one_new_hrm_streets():
    """
    - Determine what new streets have been added to TRN_street that do not exist in TRN_STREET_RIVA
    - TRN_STREET_RIVA
    - TRN_STREET

    :return:
    """

    hrm_streets_filter = "OWN LIKE 'HRM'"
    not_retired_filter = "DATE_RET IS NULL"

    try:

        # Create local workspace
        local_gdb = os.path.join(SCRIPTS_DIR, "scratch.gdb")

        if not arcpy.Exists(local_gdb):
            logger.info("Creating local geodatabase...")

            utils.create_fgdb(
                out_folder_path=SCRIPTS_DIR,
                out_name="scratch.gdb"
            )

        # Export TRN_STREET_RIVA to local workspace for backup purposes
        logger.info("Exporting TRN_STREET_RIVA to local workspace for backup purposes...")
        arcpy.TableToGeodatabase_conversion(
            Input_Table=TRN_STREET_RIVA,
            Output_Geodatabase=local_gdb
        )
        trn_street_riva_copy = os.path.join(local_gdb, 'TRN_STREET_RIVA')

        # Step 1 - Determine what new streets have been added to TRN_street that do not exist in TRN_STREET_RIVA
        logger.info("Step 1: Determining what new streets have been added to TRN_street that do not exist in TRN_STREET_RIVA...")

        # Select from TRN_street all records where OWN = HRM.
        logger.info("Filtering TRN_street for HRM streets...")
        trn_streets_hrm = arcpy.Select_analysis(
            in_features=TRN_STREET,
            out_feature_class=os.path.join(local_gdb, "TRN_street_HRMowned"),
            where_clause=hrm_streets_filter
        )[0]
        logger.info(arcpy.GetMessages())

        logger.info("Making copy of HRM streets to remove current RIVA streets from...")
        trn_street_new_streets_riva = arcpy.Select_analysis(
            in_features=trn_streets_hrm,
            out_feature_class=os.path.join(local_gdb, "trn_street_new_streets_riva")
        )[0]
        logger.info(arcpy.GetMessages())

        # Select in TRN_STREET_RIVA all records AND
        # Remove from current selection all records where DATE_RET IS NOT NULL
        logger.info("Filtering TRN_STREET_RIVA for non-retired street FDMIDs...")
        current_trn_street_riva_fdmids = [
            row[0] for row in arcpy.da.SearchCursor(TRN_STREET_RIVA, ["FDMID"], not_retired_filter)
        ]

        # Get all TRN_streets_hrm that are NOT already in current trn_street_riva
        # Remove any records in trn_street_new_streets_riva (copy of HRM streets) if FDMID is in TRN_STREET_RIVA
        logger.info("Finding rows in hrm streets currently already in RIVA and deleting...")
        with arcpy.da.UpdateCursor(trn_street_new_streets_riva, ["FDMID", ]) as cursor:

            for row in cursor:

                if row[0] in current_trn_street_riva_fdmids:
                    cursor.deleteRow()
                    logger.info(f"Deleted FDMID: {row[0]}")

        # Open the attribute table for the trn_street_new_streets_riva
        # Export records to a table in the file geodatabase called TBL_new_streets_for_riva
        logger.info("Exporting records to a table in the file geodatabase called TBL_new_streets_for_riva...")
        tbl_new_streets_for_riva = arcpy.ExportTable_conversion(
            trn_street_new_streets_riva,
            os.path.join(local_gdb, "TBL_new_streets_for_riva")
        )[0]

        # APPEND
        logger.info("Appending new streets into RIVA table...")
        arcpy.Append_management(
            inputs=tbl_new_streets_for_riva,
            target=trn_street_riva_copy,
            schema_type="NO_TEST"
        )

        return trn_street_riva_copy, local_gdb

    except Exception as e:
        logger.exception(e)


def step_two_update_retired_streets(trn_street_riva, local_gdb):

    """
    - TRN_STREET_RIVA
    - TRN_street

    :return:
    """

    logger.info("Starting Step 2: Updating Retired Streets...")

    # Build DATE_ACCEPT lookup from E_StreetStatus keyed by ROUTE_ID (used as DATE_ACT)
    logger.info("Building DATE_ACCEPT lookup from E_StreetStatus...")
    street_status_date_accept = {}

    for row in arcpy.da.SearchCursor(E_STREET_STATUS, ["ROUTEID", "DATE_ACCEPT"]):
        routeid, date_accept = row

        if routeid not in street_status_date_accept:
            street_status_date_accept[routeid] = date_accept

    logger.info("Getting records in TRN_STREET_RIVA that are no longer in TRN_STREET...")
    trn_street_fdmids = set(x[0] for x in arcpy.da.SearchCursor(TRN_STREET, ['FDMID']))

    # FDMIDs in RIVA not yet retired that are absent from TRN_STREET
    riva_retired_fdmids = set()
    for row in arcpy.da.SearchCursor(trn_street_riva, ["FDMID"], "DATE_RET IS NULL"):

        fdmid = row[0]
        if fdmid not in trn_street_fdmids:
            riva_retired_fdmids.add(fdmid)

    if not riva_retired_fdmids:
        logger.info("No new retired streets to update.")
        return

    # Pull retirement data from TRNLRS_segmented_street_events for matching FDMIDs.
    # TO_DATE IS NOT NULL = retired in LRS; ROUTE_ID links to E_StreetStatus.ROUTEID.
    logger.info("Querying LRS for retirement data...")
    retired_data = {}

    for row in arcpy.da.SearchCursor(
        TRNLRS_SEGMENTED,
        ["FDMID", "TO_DATE", "OLD_FDMID", "SHAPE@LENGTH", "ROUTE_ID"],
        "TO_DATE IS NOT NULL AND FDMID IS NOT NULL"
    ):
        fdmid, to_date, old_fdmid, shape_length, route_id = row
        if fdmid in riva_retired_fdmids and fdmid not in retired_data:
            retired_data[fdmid] = {
                'date_ret': to_date,
                'old_fdmid': old_fdmid,
                'shape_length': shape_length,
                'date_act': street_status_date_accept.get(route_id),
            }

    with arcpy.da.UpdateCursor(
        trn_street_riva,
        ["FDMID", "DATE_RET", "DATE_REV", "OLD_FDMID", "SHAPE_LENGTH", "DATE_ACT"]
    ) as cursor:

        for row in cursor:
            fdmid = row[0]

            if fdmid in retired_data:

                data = retired_data[fdmid]
                row[1] = data['date_ret']
                row[2] = datetime.today()
                row[3] = data['old_fdmid']
                row[4] = data['shape_length']
                row[5] = data['date_act']
                cursor.updateRow(row)
                logger.info(f"Updated FDMID: {fdmid}")


def step_three_updating_existing(trn_street_riva):
    """

    :param trn_street_riva:
    :return:
    """

    logger.info("Step 3: Updating Existing Streets...")

    # •	Create a join between TRN_STREET_RIVA and TRN_street using FDMID as common attribute, and only keep matching records
    # •	Select all records in TRN_street_riva
    # •	Remove from Selection set records where TRN_street_riva.ret_date IS NOT NULL.
    # •	Remove from selection records where:
    #       TRN_street_riva.shape.length = TRN_street.shape.length
    #       (Seems that this can only be done when using a copy of TRN_street that has been saved to a FGDB)
    # •	Note - It is rare to have segments length match, it is normal for all segments to have the following calcs done..

    trn_street_fdmids = {
        x[0]: {
            'shape_length': x[1], 'full_name': x[2], 'from_str': x[3], 'to_str': x[4], 'gsa_left': x[5],
            'old_fdmid': x[6], 'date_act': x[7], 'sys_date': x[8]
        } for x in arcpy.da.SearchCursor(
            TRN_STREET,
            ["FDMID", "SHAPE@LENGTH", "FULL_NAME", "FROM_STR", "TO_STR", "GSA_LEFT", "OLD_FDMID", "DATE_ACT", "SYS_DATE"],
        )
    }

    with arcpy.da.UpdateCursor(
            trn_street_riva,
            ["FDMID", "SHAPE_LENGTH", "SHORT_DESC", "LONG_DESC", "OLD_FDMID", "DATE_REV", "DATE_ACT", "SYS_DATE"],
            "DATE_RET IS NULL"
    ) as cursor:

        for row in cursor:

            fdmid = row[0]
            shape_length = row[1]

            if fdmid in trn_street_fdmids:
                trn_street_row_info = trn_street_fdmids.get(fdmid)
                trn_street_len = trn_street_row_info['shape_length']

                if trn_street_len == shape_length:
                    continue  # Note - It is rare to have segments length match.

                full_name = trn_street_row_info['full_name']
                from_str = trn_street_row_info['from_str']
                to_str = trn_street_row_info['to_str']
                gsa_left = trn_street_row_info['gsa_left']
                old_fdmid = trn_street_row_info['old_fdmid']
                date_act = trn_street_row_info['date_act']
                sys_date = trn_street_row_info['sys_date']

                row[1] = trn_street_len
                row[2] = f'{full_name} ({from_str} TO {to_str})'  # SHORT_DESC
                row[3] = f'{full_name} {gsa_left}'  # LONG_DESC
                row[4] = old_fdmid  # OLD_FDMID
                row[5] = datetime.today()  # DATE_REV
                row[6] = date_act  # DATE_ACT
                row[7] = sys_date  # SYS_DATE

                cursor.updateRow(row)
                logger.info(f"Row FDMID {fdmid} updated.")

                # •	For remaining records, update based on the following calculations:
                # •	Update TRN_street_riva to equal the shape.length in TRN_street
                # •	** create a table of TRN_street to calc lengths
                # •	Update the Short_Desc and Long_Desc in TRN_street_riva - Use the following logic statement:
                # •	Short_Desc:
                # [SDEADM.TRN_street.FULL_NAME] + " (" + [SDEADM.TRN_street.FROM_STR] + " TO " + [SDEADM.TRN_street.TO_STR] + ")"
                # •	Long Desc:
                # [SDEADM.TRN_street.FULL_NAME] + " (" + [SDEADM.TRN_street.GSA_LEFT] + ")"
                # •	Update the OLD_FDMID field to equal OLD_FDMID in TRN_street
                # •	Update the DATE_REV in TRN_street_riva for records updated


def step_four_validation_review(local_gdb):
    """
    QA review of net new streets inserted in step 1.
    Reads TBL_new_streets_for_riva and reports null/blank counts for
    SHORT_DESC, LONG_DESC, and DATE_REV — fields that must not be empty.
    """
    logger.info("Step 4: Validation Review of Net New Streets...")

    tbl = os.path.join(local_gdb, "TBL_new_streets_for_riva")

    if not arcpy.Exists(tbl):
        logger.info("TBL_new_streets_for_riva not found — run step_one_new_hrm_streets() first.")
        return

    fields = ["SHORT_DESC", "LONG_DESC", "DATE_REV"]
    null_counts = {f: 0 for f in fields}
    total = 0

    for row in arcpy.da.SearchCursor(tbl, fields):
        total += 1
        for i, field in enumerate(fields):
            val = row[i]
            if val is None or (isinstance(val, str) and val.strip() == ""):
                null_counts[field] += 1

    logger.info(f"Total net new records in TBL_new_streets_for_riva: {total}")
    logger.info("Null/blank counts:")
    for field, count in null_counts.items():
        pct = f"{count / total:.0%}" if total else "N/A"
        logger.info(f"  {field}: {count} null/blank ({pct})")

    return null_counts


if __name__ == "__main__":

    # STEP 1
    trn_street_riva_local, local_workspace = step_one_new_hrm_streets()

    # STEP 2
    step_two_update_retired_streets(trn_street_riva_local, local_workspace)

    # STEP 3
    step_three_updating_existing(trn_street_riva_local)

    # STEP 4
    step_four_validation_review(local_workspace)

    # input("Truncate and load RW")
    # input("Truncate and load ASSET_ACCOUNTING.TRN_STREET_RIVA")
