# ETL Process

import arcpy
import utils
import os

from datetime import datetime

# Settings
arcpy.SetLogHistory(False)
arcpy.env.overwriteOutput = True

SDE = r"E:\HRM\Scripts\SDE\SQL\dev_RW_sdeadm.sde"

TRN_STREET = os.path.join(SDE, "SDEADM.TRN_streets_routes", "SDEADM.TRN_street")
TRN_STREET_RIVA = os.path.join(SDE, "SDEADM.TRN_STREET_RIVA")
TRN_STREET_RETIRED = os.path.join(SDE, "SDEADM.TRN_street_retired")

PROJECT_DIR = os.path.dirname(os.getcwd())
SCRIPTS_DIR = os.path.join(PROJECT_DIR, "scripts")

# SDE = os.path.join(SCRIPTS_DIR, "prod_copy.gdb")


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
            print("Creating local geodatabase...")

            utils.create_fgdb(
                out_folder_path=SCRIPTS_DIR,
                out_name="scratch.gdb"
            )

        # Export TRN_STREET_RIVA to local workspace for backup purposes
        print("\nExporting TRN_STREET_RIVA to local workspace for backup purposes...")
        arcpy.TableToGeodatabase_conversion(
            Input_Table=TRN_STREET_RIVA,
            Output_Geodatabase=local_gdb
        )
        trn_street_riva_copy = os.path.join(local_gdb, 'TRN_STREET_RIVA')

        # Step 1 - Determine what new streets have been added to TRN_street that do not exist in TRN_STREET_RIVA
        print("\nStep 1: Determining what new streets have been added to TRN_street that do not exist in TRN_STREET_RIVA...")

        # Select from TRN_street all records where OWN = HRM.
        print("\nFiltering TRN_street for HRM streets...")
        trn_streets_hrm = arcpy.Select_analysis(
            in_features=TRN_STREET,
            out_feature_class=os.path.join(local_gdb, "TRN_street_HRMowned"),
            where_clause=hrm_streets_filter
        )[0]
        print(arcpy.GetMessages())

        print("\nMaking copy of hrm streets to remove current RIVA streets from...")
        trn_street_new_streets_riva = arcpy.Select_analysis(
            in_features=trn_streets_hrm,
            out_feature_class=os.path.join(local_gdb, "trn_street_new_streets_riva")
        )[0]
        print(arcpy.GetMessages())

        # Select in TRN_STREET_RIVA all records AND
        # Remove from current selection all records where DATE_RET IS NOT NULL
        print("\nFiltering TRN_STREET_RIVA for non-retired street FDMIDs...")
        current_trn_street_riva_fdmids = [
            row[0] for row in arcpy.da.SearchCursor(TRN_STREET_RIVA, ["FDMID"], not_retired_filter)
        ]

        # Get all TRN_streets_hrm that are NOT already in current trn_street_riva
        # Remove any records in trn_street_new_streets_riva (copy of HRM streets) if FDMID is in TRN_STREET_RIVA
        print("\nFinding rows in hrm streets currently already in RIVA and deleting...")
        with arcpy.da.UpdateCursor(trn_street_new_streets_riva, ["FDMID", ]) as cursor:

            for row in cursor:

                if row[0] in current_trn_street_riva_fdmids:
                    cursor.deleteRow()
                    print(f"\tDeleted FDMID: {row[0]}")

        # Open the attribute table for the trn_street_new_streets_riva
        # Export records to a table in the file geodatabase called TBL_new_streets_for_riva
        print("Exporting records to a table in the file geodatabase called TBL_new_streets_for_riva...")
        tbl_new_streets_for_riva = arcpy.ExportTable_conversion(
            trn_street_new_streets_riva,
            os.path.join(local_gdb, "TBL_new_streets_for_riva")
        )[0]

        # APPEND
        print(f"\nAppending new streets into RIVA table...")
        arcpy.Append_management(
            inputs=tbl_new_streets_for_riva,
            target=trn_street_riva_copy,
            schema_type="NO_TEST"
        )

        return trn_street_riva_copy, local_gdb

    except Exception as e:
        print(e)


def step_two_update_retired_streets(trn_street_riva, local_gdb):

    """
    - TRN_STREET_RIVA
    - TRN_street

    :return:
    """

    print("\nStarting Step 2: Updating Retired Streets...")

    # •	Create a relate between TRN_STREET_RIVA and TRN_street using FDMID as common attribute
    # •	Create a relate between TRN_street_riva and TRN_street_retired using FDMID as common attribute

    # •	Select all records in TRN_street and relate to TRN_street_riva
    # •	Reverse selection set in the TRN_street_riva table
    # •	In the TRN_street_riva table, remove from selection set records that meet the following condition: DATE_RET IS NOT NULL
    # TODO: Get all records in TRN_STREET_RIVA that are NOT in TRN_street and where DATE_RET IS NOT NULL

    print("Getting records in TRN_STREET_RIVA that are no longer in TRN_STREET...")

    trn_street_fdmids = list(set([x[0] for x in arcpy.da.SearchCursor(TRN_STREET, ['FDMID', ])]))  # 1pk records

    riva_retired_street_fdmids = list()  # 260 - 700004812, 700008166 --- 700013207

    for row in arcpy.da.SearchCursor(trn_street_riva,["FDMID", ], "DATE_RET IS NOT NULL"):
        fdmid = row[0]

        if fdmid not in trn_street_fdmids:
            riva_retired_street_fdmids.append(fdmid)

    # •	If no records remain, this means there are no new retired streets that need to be updated in the TRN_STREET_RIVA table.
    # If there are records meet selection query, continue with next step
    if riva_retired_street_fdmids:
        # •	Relate the selection set in TRN_street_riva to TRN_street_retired
        # •	Save selection set to a new line feature class called TRN_street_new_retired_streets_riva

        # Get all retired streets from TRN_street_retired
        trn_street_new_retired_streets_riva = arcpy.Select_analysis(
            in_features=TRN_STREET_RETIRED,
            out_feature_class=os.path.join(local_gdb, "TRN_street_new_retired_streets_riva")
        )
        with arcpy.da.UpdateCursor(trn_street_new_retired_streets_riva, ["FDMID"]) as cursor:

            for row in cursor:
                fdmid = row[0]

                if fdmid not in riva_retired_street_fdmids:
                    cursor.deleteRow()

        del cursor

        # •	This output will be joined to the TRN_street_riva table, to update the DATE_RET field, shape.length, and anything else.
        # TODO: Update TRN_STREET_RIVA with retired street data

        # TRN_STREET_RIVA.DATE_RET = TRN_street_new_retired_streets_riva.DATE_RET
        # TRN_STREET_RIVA.DATE_REV = Today's Date
        # TRN_STREET_RIVA.OLD_FDMID = TRN_street_new_retired_streets_riva.OLD_FDMID
        # TRN_STREET_RIVA.shape_length = TRN_street_new_retired_streets_riva.shape_length

        new_retired_streets_data = {
            row[0]: row for row in arcpy.da.SearchCursor(
                trn_street_new_retired_streets_riva,
                ["FDMID", "DATE_RET", "OLD_FDMID", "SHAPE@LENGTH", "DATE_ACT"]
            )
        }

        # •	Create a join between the TRN_STREET_RIVA and TRN_street_new_retired_streets_riva using FDMID as common attribute,
        #   and only keep the matching records.
        # •	Start an edit session
        # •	Update the following attributes using the Field Calculator tool (right-clicking on attribute field column heading)

        with arcpy.da.UpdateCursor(trn_street_riva, ["FDMID", "DATE_RET", "DATE_REV", "OLD_FDMID", "SHAPE_LENGTH", "DATE_ACT"]) as cursor:

            for row in cursor:
                fdmid = row[0]

                if fdmid in new_retired_streets_data:
                    # Update row
                    update_info = new_retired_streets_data.get(fdmid)

                    row[1] = update_info[1]  # DATE_RET
                    row[2] = datetime.today()  # DATE_REV
                    row[3] = update_info[2]  # OLD_FDMID
                    row[4] = update_info[3]  # SHAPE.LENGTH
                    row[5] = update_info[4]  # DATE_ACT

                    cursor.updateRow(row)
                    print(f"\tUpdated {fdmid}")


def step_three_updating_existing(trn_street_riva):
    """

    :param trn_street_riva:
    :return:
    """

    print("\nStep 3: Updating Existing Streets...")

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


if __name__ == "__main__":

    # STEP 1
    trn_street_riva_local, local_workspace = step_one_new_hrm_streets()

    # STEP 2
    # step_two_update_retired_streets(trn_street_riva_local, local_workspace)

    # STEP 3
    # step_three_updating_existing(trn_street_riva_local)

    # input("Truncate and load RW")
    # input("Truncate and load ASSET_ACCOUNTING.TRN_STREET_RIVA")

    # TODO: Needs some review. The following shouldn't be blank (QA):
    #  SHORT_DESC
    #  LONG_DESC
    #  DATE_REV
    #  FDMID: 700013207
