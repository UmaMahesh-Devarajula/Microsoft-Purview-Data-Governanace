import os
import csv
import datetime
import json
from typing import Dict, List
from data-sources import SOURCE_TYPES
from data-sources import PARSED_FIELDS
from data-sources import COMMON_PROPERTIES

def get_superset_fields() -> List[str]:
    """
    Build a superset header that includes:
    - source_type, COMMON_PROPERTIES
    - all user-entered properties from SOURCE_TYPES
    - parsed fields (subscription_id, resource_group, resource_name)
    - timestamp
    """
    fields = ["source_type"] + COMMON_PROPERTIES[:]
    seen = set(fields)
    for st in SOURCE_TYPES.values():
        for p in st["properties"]:
            if p not in seen:
                fields.append(p)
                seen.add(p)
    # add parsed fields explicitly (they are not in SOURCE_TYPES properties)
    for pf in PARSED_FIELDS:
        if pf not in seen:
            fields.append(pf)
            seen.add(pf)
    fields.append("timestamp")
    return fields

def reconcile_and_ensure_csv(csv_path: str, superset_fields: List[str]) -> List[str]:
    """
    Ensure CSV exists and header contains superset_fields.
    If file exists and header differs, rewrite file with merged header preserving rows.
    Returns the merged header used for subsequent writes.
    """
    dirpath = os.path.dirname(csv_path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)

    if not os.path.exists(csv_path):
        # create new file with superset header
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=superset_fields)
            writer.writeheader()
        return superset_fields

    # File exists: read existing header and rows
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        existing_fields = reader.fieldnames or []
        rows = list(reader)

    # If headers already match exactly, nothing to do
    if existing_fields == superset_fields:
        return superset_fields

    # Compute merged header (preserve existing order, then add missing fields)
    merged = existing_fields[:]  # keep existing order
    for fld in superset_fields:
        if fld not in merged:
            merged.append(fld)

    # Rewrite file with merged header and existing rows (map missing fields to "")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=merged)
        writer.writeheader()
        for r in rows:
            out = {k: r.get(k, "") for k in merged}
            writer.writerow(out)

    return merged

def write_record_with_reconcile(csv_path: str, record: Dict[str, str], superset_fields: List[str]):
    try:
        merged_fields = reconcile_and_ensure_csv(csv_path, superset_fields)
        row = {k: record.get(k, "") for k in merged_fields}
        # Debug prints to help trace issues
        print("CSV path:", csv_path)
        print("Merged header:", merged_fields)
        print("Row to write:", row)
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=merged_fields)
            writer.writerow(row)
        print("CSV write succeeded")
    except Exception as e:
        print("CSV write failed:", repr(e))
        raise
