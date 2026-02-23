#!/usr/bin/env python3
import os
import csv
import datetime
import json
from typing import Dict, List
from PurviewScanClient.purviewscanclient import get_purview_scan_client
from Authenticate.authenticate import authenticate
from datasources.datasources import SOURCE_TYPES
from datasources.datasources import COMMON_PROPERTIES
from datasources.datasources import PARSED_FIELDS
from datasources.buildpayload import build_payload
from datasources.parseresourceId import parse_resource_id
from Collections.resolvecollection import resolve_collection_name
from datasourcesgenerateregistereddatasourcescsv import get_superset_fields
from datasourcesgenerateregistereddatasourcescsv import reconcile_and_ensure_csv
from datasourcesgenerateregistereddatasourcescsv import write_record_with_reconcile

# Use repo working directory to avoid confusion about different user homes
CSV_FILE = os.path.join(os.getcwd(), "registered-datasources.csv")

def register_datasource():
    print("Supported source types:", ", ".join(SOURCE_TYPES.keys()))
    source_type = input("Enter data source type: ").strip()
    if source_type not in SOURCE_TYPES:
        print("Unsupported source type.")
        return

    props: Dict[str, str] = {}
    # Collect common properties
    for prop in COMMON_PROPERTIES:
        props[prop] = input(f"Enter {prop}: ").strip()

    # Collect only user-entered source-specific properties (do NOT prompt for parsed fields)
    for prop in SOURCE_TYPES[source_type]["properties"]:
        props[prop] = input(f"Enter {prop}: ").strip()

    # If Azure source and resource_id provided, parse and populate parsed fields
    if source_type in ["AdlsGen2", "AzureStorage", "AzureSqlDatabase", "AzureCosmosDb", "AzureSynapseAnalytics"]:
        resource_id = props.get("resource_id", "").strip()
        if resource_id:
            try:
                parsed = parse_resource_id(resource_id)
                props["subscription_id"] = parsed["subscriptionId"]
                props["resource_group"] = parsed["resourceGroup"]
                props["resource_name"] = parsed["resourceName"]
            except ValueError as ve:
                print(f"Warning: Resource ID parse error: {ve}")
                props.setdefault("subscription_id", "")
                props.setdefault("resource_group", "")
                props.setdefault("resource_name", "")
        else:
            props.setdefault("subscription_id", "")
            props.setdefault("resource_group", "")
            props.setdefault("resource_name", "")

    # Resolve collection name to internal Purview name (best-effort)
    props["collection_name"] = resolve_collection_name(props.get("collection_name", ""))

    # Build payload and register
    payload = build_payload(source_type, props)
    print("Payload being sent:")
    print(json.dumps(payload, indent=2))

    client = get_purview_scan_client()
    try:
        response = client.data_sources.create_or_update(props.get("ds_name", ""), body=payload)
        print("Data source registered:", response)
    except Exception as e:
        print("Error registering data source:", e)
        return

    # Prepare record for CSV using superset header (includes parsed fields)
    superset_fields = get_superset_fields()
    record: Dict[str, str] = {"source_type": source_type}
    for p in COMMON_PROPERTIES:
        record[p] = props.get(p, "")
    # include all user-entered properties
    for st in SOURCE_TYPES.values():
        for p in st["properties"]:
            if p not in record:
                record[p] = props.get(p, "")
    # add parsed fields explicitly
    for pf in PARSED_FIELDS:
        record[pf] = props.get(pf, "")
    # Use timezone-aware timestamp
    record["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Write record with reconciliation (preserve existing rows and merge headers)
    write_record_with_reconcile(CSV_FILE, record, superset_fields)
    generate_backup_script(source_type, props, payload)

register_datasource()

