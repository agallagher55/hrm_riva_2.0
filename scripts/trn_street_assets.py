import arcpy
import os

from datetime import datetime

from gispy import utils

# Settings
arcpy.SetLogHistory(False)
arcpy.env.overwriteOutput = True

SDE = r"E:\HRM\Scripts\SDE\SQL\qa_RW_sdeadm.sde"

TRNLRS_TRN_STREET_VW = os.path.join(SDE, "SDEADM.TRNLRS_TRN_STREET_VW")
TRNLRS_SEGMENTED = os.path.join(SDE, "SDEADM.TRNLRS_segmented_street_events")
E_STREET_STATUS = os.path.join(SDE, "SDEADM.TRNLRS", "SDEADM.E_StreetStatus")

TRN_STREET_RIVA = os.path.join(SDE, "SDEADM.TRN_STREET_RIVA")
AA_TRN_STREET_RIVA = os.path.join(SDE, "ASSET_ACCOUNTING.TRN_STREET_RIVA")

PROJECT_DIR = os.path.dirname(os.getcwd())
SCRIPTS_DIR = os.path.join(PROJECT_DIR, "scripts")


def step_one_new_hrm_streets(local_gdb: str):
    """
    - Determine what new streets have been added to TRNLRS_TRN_STREET_VW that do not exist in TRN_STREET_RIVA
    - TRN_STREET_RIVA
    - TRNLRS_TRN_STREET_VW

    :return:
    """

    hrm_streets_filter = "OWN LIKE 'HRM'"

    try:

        # Step 1 - Determine what new streets have been added to TRN_street that do not exist in TRN_STREET_RIVA
        print(
            "\nStep 1: Determining what new streets have been added to TRN_street that do not exist in TRN_STREET_RIVA...")

        # Select from TRN_street all records where OWN = HRM.
        print("\nFiltering TRN_street for HRM streets...")
        hrm_owned_streets = arcpy.Select_analysis(
            in_features=TRNLRS_TRN_STREET_VW,
            out_feature_class=os.path.join(local_gdb, "TRN_street_HRMowned"),
            where_clause=hrm_streets_filter
        )[0]

        # Select in TRN_STREET_RIVA all records AND
        # Remove from current selection all records where DATE_RET IS NOT NULL
        print("\nFiltering TRN_STREET_RIVA for non-retired street FDMIDs...")

        not_retired_filter = "DATE_RET IS NULL"

        non_retired_riva_fdmids = [
            row[0] for row in arcpy.da.SearchCursor(TRN_STREET_RIVA, ["FDMID"], not_retired_filter)
        ]

        print("\nMaking copy of HRM streets to remove current RIVA streets from...")
        hrm_owned_new_riva_streets = arcpy.Select_analysis(
            in_features=hrm_owned_streets,
            out_feature_class=os.path.join(local_gdb, "hrm_owned_new_riva_streets")
        )[0]

        # Get all TRN_streets_hrm that are NOT already in current trn_street_riva
        # Remove any records in hrm_owned_new_riva_streets (copy of HRM streets) if FDMID is in TRN_STREET_RIVA
        print("\nFinding rows in hrm streets currently already in RIVA and deleting...")
        with arcpy.da.UpdateCursor(hrm_owned_new_riva_streets, ["FDMID", ]) as cursor:

            for row in cursor:

                if row[0] in non_retired_riva_fdmids:
                    cursor.deleteRow()
                    print(f"\tDeleted FDMID: {row[0]}")

        # Open the attribute table for the hrm_owned_new_riva_streets
        # Export records to a table in the file geodatabase called TBL_new_streets_for_riva
        print("Exporting records to a table in the file geodatabase called new_streets_for_riva...")
        tbl_new_streets_for_riva = arcpy.ExportTable_conversion(
            hrm_owned_new_riva_streets,
            os.path.join(local_gdb, "new_streets_for_riva")
        )[0]

        # SHORT_DESC and LONG_DESC are absent from the source — add and compute them
        # before building the field mapping so addTable picks them up.
        print("\nAdding and computing SHORT_DESC and LONG_DESC...")
        arcpy.AddField_management(tbl_new_streets_for_riva, "SHORT_DESC", "TEXT", field_length=255)
        arcpy.AddField_management(tbl_new_streets_for_riva, "LONG_DESC", "TEXT", field_length=255)

        with arcpy.da.UpdateCursor(
                tbl_new_streets_for_riva,
                ["FULL_NAME", "FROM_STR", "TO_STR", "GSA_LEFT", "SHORT_DESC", "LONG_DESC"]
        ) as cursor:
            
            for row in cursor:
                
                full_name = row[0] or ""
                from_str = row[1] or ""
                to_str = row[2] or ""
                gsa_left = row[3] or ""
                row[4] = f"{full_name} ({from_str} TO {to_str})"
                row[5] = f"{full_name} ({gsa_left})"
                
                cursor.updateRow(row)

        # Build FieldMappings: load all source fields as pass-throughs, then override
        # the output name for fields whose source name differs from the target name.
        print("\nBuilding field mappings for append...")
        field_mappings = arcpy.FieldMappings()
        field_mappings.addTable(tbl_new_streets_for_riva)

        field_renames = {
            "FROM_STR": "FROM_STREET",
            "TO_STR": "TO_STREET",
            "GSA_LEFT": "GSA_NAME",
            "ADDDATE": "DATE_ACT",
            "MODDATE": "SYS_DATE",
            "ST_CLASS": "PST_CLASS",
            "STR_CODE_L": "STR_CODE",
        }
        
        for source_name, target_name in field_renames.items():
            
            idx = field_mappings.findFieldMapIndex(source_name)
            
            if idx == -1:
                continue
                
            fm = field_mappings.getFieldMap(idx)
            out_field = fm.outputField
            out_field.name = target_name
            fm.outputField = out_field
            field_mappings.replaceFieldMap(idx, fm)

        # APPEND
        trn_street_riva_copy = os.path.join(local_gdb, 'TRN_STREET_RIVA')

        # Export TRN_STREET_RIVA to local workspace for backup purposes
        print("\nExporting TRN_STREET_RIVA to local workspace...")
        arcpy.TableToGeodatabase_conversion(
            Input_Table=TRN_STREET_RIVA,
            Output_Geodatabase=local_gdb
        )

        print(f"\nAppending new streets into RIVA table...")
        arcpy.Append_management(
            inputs=tbl_new_streets_for_riva,
            target=trn_street_riva_copy,
            schema_type="NO_TEST",
            field_mapping=field_mappings
        )

        return trn_street_riva_copy, local_gdb

    except Exception as e:
        print(e)
        raise RuntimeError(f"step_one_new_hrm_streets failed: {e}") from e


def step_two_update_retired_streets(new_riva_streets):
    """
    Update retired streets in set of RIVA streets to be added to RIVA streets table
    - TRN_STREET_RIVA
    - TRNLRS_TRN_STREET_VW

    :return:
    """

    print("\nStarting Step 2: Updating Retired Streets...")

    print("Getting records in TRN_STREET_RIVA that are no longer in TRN_STREET...")
    trn_street_fdmids = set(x[0] for x in arcpy.da.SearchCursor(TRNLRS_TRN_STREET_VW, ['FDMID']))

    # FDMIDs in RIVA not yet retired that are absent from TRN_STREET
    not_retired_filter = "DATE_RET IS NULL"

    riva_retired_fdmids = {
        row[0]
        for row in arcpy.da.SearchCursor(new_riva_streets, ["FDMID"], not_retired_filter)
        if row[0] not in trn_street_fdmids
    }

    if not riva_retired_fdmids:
        print("No new retired streets to update.")
        return

    # Build DATE_ACCEPT lookup from E_StreetStatus keyed by ROUTE_ID (used as DATE_ACT)
    print("Building DATE_ACCEPT lookup from E_StreetStatus...")

    street_status_date_accept = {
        route_id: date_accept
        for route_id, date_accept in arcpy.da.SearchCursor(E_STREET_STATUS, ["ROUTEID", "DATE_ACCEPT"])
    }

    # Pull retirement data from TRNLRS_segmented_street_events for matching FDMIDs.
    # TO_DATE IS NOT NULL = retired in LRS; ROUTE_ID links to E_StreetStatus.ROUTEID.
    print("Querying LRS for retirement data...")
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
            new_riva_streets,
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
                print(f"\tUpdated FDMID: {fdmid}")

    # TODO: Create feature of retired streets


def step_three_updating_existing_riva_streets(trn_street_riva):
    """
    From original documentation:
    # •	Create a join between TRN_STREET_RIVA and TRN_street using FDMID as common attribute, and only keep matching records
    # •	Select all records in TRN_street_riva
    # •	Remove from Selection set records where TRN_street_riva.ret_date IS NOT NULL.
    # •	Remove from selection records where:
    #       TRN_street_riva.shape.length = TRN_street.shape.length
    #       (Seems that this can only be done when using a copy of TRN_street that has been saved to a FGDB)
    # •	Note - It is rare to have segments length match, it is normal for all segments to have the following calcs done..
    :param trn_street_riva:
    :return:
    """

    print("\nStep 3: Updating Existing Streets...")

    trn_street_fdmids = {
        x[0]: {
            'shape_length': x[1],
            'full_name': x[2],
            'from_str': x[3],
            'to_str': x[4],
            'gsa_left': x[5],
            'old_fdmid': x[6],
            'date_act': x[7],
            'sys_date': x[8]
        } for x in arcpy.da.SearchCursor(
            TRNLRS_TRN_STREET_VW,
            ["FDMID", "SHAPE@LENGTH", "FULL_NAME", "FROM_STR", "TO_STR", "GSA_LEFT", "OLD_FDMID", "ADDDATE", "MODDATE"],
        )
    }

    with arcpy.da.UpdateCursor(
            trn_street_riva,
            ["FDMID", "SHAPE_LENGTH", "SHORT_DESC", "LONG_DESC", "OLD_FDMID", "DATE_REV", "DATE_ACT", "SYS_DATE"],
            "DATE_RET IS NULL"
    ) as cursor:

        for row in cursor:

            fdmid = row[0]
            local_riva_shape_length = row[1]

            if fdmid in trn_street_fdmids:

                trn_street_row_info = trn_street_fdmids.get(fdmid)
                trn_street_len = trn_street_row_info['shape_length']

                if trn_street_len == local_riva_shape_length:
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
                row[3] = f'{full_name} ({gsa_left})'  # LONG_DESC
                row[4] = old_fdmid  # OLD_FDMID
                row[5] = datetime.today()  # DATE_REV
                row[6] = date_act  # DATE_ACT
                row[7] = sys_date  # SYS_DATE

                cursor.updateRow(row)
                print(f"\tRow FDMID {fdmid} updated.")

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


def step_four_validation_review(local_gdb: str, riva_feature: str):
    """
    QA review of net new streets inserted in step 1.
    Reads TBL_new_streets_for_riva and reports null/blank counts for
    SHORT_DESC, LONG_DESC, and DATE_REV — fields that must not be empty.
    """

    print("\nStep 4: Validation Review of Net New Streets...")

    tbl = os.path.join(local_gdb, "TBL_new_streets_for_riva")

    if not arcpy.Exists(tbl):
        print("  TBL_new_streets_for_riva not found — run step_one_new_hrm_streets() first.")
        return

    fields = ["SHORT_DESC", "LONG_DESC", ]
    null_counts = {f: 0 for f in fields}
    total = 0

    for row in arcpy.da.SearchCursor(riva_feature, fields):

        total += 1

        for i, field in enumerate(fields):
            val = row[i]

            if val is None or (isinstance(val, str) and val.strip() == ""):
                null_counts[field] += 1

    print(f"\n  Total net new records in TBL_new_streets_for_riva: {total}")
    print("  Null/blank counts:")
    for field, count in null_counts.items():
        pct = f"{count / total:.0%}" if total else "N/A"
        print(f"    {field}: {count} null/blank ({pct})")

    return null_counts


def step_five_truncate_load_asset_accounting(source_riva: str = None):
    """
    Truncate ASSET_ACCOUNTING.TRN_STREET_RIVA and reload from SDEADM.TRN_STREET_RIVA
    (or a supplied local copy produced by steps 1–3).
    """

    target = AA_TRN_STREET_RIVA

    try:
        print("\nStep 5: Truncate and Load ASSET_ACCOUNTING.TRN_STREET_RIVA...")

        print(f"  Truncating {target}...")
        arcpy.TruncateTable_management(target)
        print("  Table truncated.")

        print(f"  Appending records from {source_riva}...")
        arcpy.Append_management(
            inputs=source_riva,
            target=target,
            schema_type="NO_TEST"
        )

        count = int(arcpy.GetCount_management(target)[0])
        print(f"  Load complete — {count} records in ASSET_ACCOUNTING.TRN_STREET_RIVA.")

        return target

    except Exception as e:
        print(e)
        raise RuntimeError(f"step_five_truncate_load_asset_accounting failed: {e}") from e


if __name__ == "__main__":

    # Create local workspace
    local_gdb = os.path.join(SCRIPTS_DIR, "scratch.gdb")

    if not arcpy.Exists(local_gdb):
        print("Creating local geodatabase...")

        utils.create_fgdb(
            out_folder_path=SCRIPTS_DIR,
            out_name="scratch.gdb"
        )

    # STEP 1: Get new streets to be added to RIVA
    new_riva_streets, local_workspace = step_one_new_hrm_streets(local_gdb=local_gdb)

    # STEP 2
    step_two_update_retired_streets(new_riva_streets)

    # STEP 3
    step_three_updating_existing_riva_streets(new_riva_streets)

    # STEP 4
    step_four_validation_review(local_workspace, new_riva_streets)

    # input("Truncate and load RW")
    step_five_truncate_load_asset_accounting(source_riva=new_riva_streets)

    # Truncate and load RO
