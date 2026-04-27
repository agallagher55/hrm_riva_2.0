import arcpy


def update_riva_from_event_table(
    riva_fc: str,
    event_table: str,
    event_field: str,
    target_field: str,
    segmented_table: str,
    active_only: bool = True,
    riva_filter: str = "DATE_RET IS NULL",
) -> int:
    """
    Update a single field in TRN_STREET_RIVA from a matching LRS event table.

    Join path:
        TRN_STREET_RIVA.FDMID
        → segmented_table (FDMID, ROUTE_ID, FROMMEASURE, TOMEASURE)
        → event_table     (ROUTEID, FROMMEASURE, TOMEASURE, <event_field>)

    When multiple events overlap a RIVA segment the event with the greatest
    measure overlap wins.  Assumes segmented_table uses ROUTE_ID (not ROUTEID)
    and that event tables use the standard LRS ROUTEID field name.

    Args:
        riva_fc:          Path to TRN_STREET_RIVA (SDE or local copy).
        event_table:      Full path to the LRS event table
                          e.g. os.path.join(SDE, "SDEADM.TRNLRS", "SDEADM.E_Width")
        event_field:      Attribute field to read from the event table (e.g. "Width").
        target_field:     Field in TRN_STREET_RIVA to write (e.g. "PAVE_WIDTH").
        segmented_table:  Path to TRNLRS_segmented_street_events.
        active_only:      When True (default) reads only events where TODATE IS NULL.
        riva_filter:      Where clause applied to the TRN_STREET_RIVA update cursor.

    Returns:
        Number of records updated.
    """

    # --- 1. FDMID → (route_id, from_measure, to_measure) -------------------------
    # Filter to active segmentation rows only (TO_DATE IS NULL).
    print(f"  Building FDMID route/measure index from segmented streets...")
    fdmid_lookup: dict = {}

    for row in arcpy.da.SearchCursor(
        segmented_table,
        ["FDMID", "ROUTE_ID", "FROMMEASURE", "TOMEASURE"],
        "FDMID IS NOT NULL AND TO_DATE IS NULL",
    ):
        
        fdmid, route_id, from_m, to_m = row
        
        if fdmid not in fdmid_lookup:
            fdmid_lookup[fdmid] = (route_id, from_m or 0.0, to_m or 0.0)

    print(f"  {len(fdmid_lookup)} FDMID entries indexed.")

    # --- 2. ROUTEID → [(from_m, to_m, value), ...] from event table ---------------
    where = "TODATE IS NULL" if active_only else None
    route_events: dict = {}

    print(f"  Reading '{event_field}' from {event_table}...")
    for row in arcpy.da.SearchCursor(
        event_table,
        ["ROUTEID", "FROMMEASURE", "TOMEASURE", event_field],
        where,
    ):
        route_id, from_m, to_m, value = row
        if value is None:
            continue
            
        route_events.setdefault(route_id, []).append((from_m or 0.0, to_m or 0.0, value))

    print(f"  {sum(len(v) for v in route_events.values())} active event records read.")

    # --- 3. Update RIVA via best-overlap matching ----------------------------------
    print(f"  Updating {target_field} in {riva_fc}...")
    updated = 0

    with arcpy.da.UpdateCursor(riva_fc, ["FDMID", target_field], riva_filter) as cursor:
        
        for row in cursor:
            fdmid = row[0]

            seg = fdmid_lookup.get(fdmid)
            
            if not seg:
                continue

            route_id, seg_from, seg_to = seg
            events = route_events.get(route_id, [])
            
            if not events:
                continue

            # Pick the event with the greatest overlap with this RIVA segment.
            best_value = None
            best_overlap = 0.0

            for ev_from, ev_to, value in events:
                overlap = min(seg_to, ev_to) - max(seg_from, ev_from)
                
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_value = value

            if best_value is not None and best_overlap > 0:
                
                row[1] = best_value
                cursor.updateRow(row)
                updated += 1

    print(f"  {target_field}: {updated} records updated.")
    return updated
