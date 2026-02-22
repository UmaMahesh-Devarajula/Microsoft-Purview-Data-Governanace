#!/usr/bin/env python3
import os
import csv
import datetime
import json
from typing import Dict, List
from azure.identity import ClientSecretCredential
from azure.purview.scanning import PurviewScanningClient
from azure.purview.administration.account import PurviewAccountClient
from authenticate import authenticate

# Configuration
BACKUP_DIR = "backup-datasources"
# Use repo working directory to avoid confusion about different user homes
CSV_FILE = os.path.join(os.getcwd(), "datasources.csv")
creds = authenticate()

# Source definitions (only user-entered properties here)
SOURCE_TYPES = {
    "AzureSubscription": {
        "kind": "AzureSubscription",
        "properties": ["subscription_id", "resource_id"]
    },
    "AzureResourceGroup": {
        "kind": "AzureResourceGroup",
        "properties": ["resource_group", "subscription_id", "resource_id"]
    },
    "AWS account": {
        "kind": "AmazonAccount",
        "properties": ["awsAccount_id"]
    },
    "AzureSynapseAnalytics": {
        "kind": "AzureSynapseWorkspace",
        "properties": ["dedicated_SqlEndpoint", "serverless_SqlEndpoint", "resource_id", "location"]
    },
    "AdlsGen2": {
        "kind": "AdlsGen2",
        "properties": ["endpoint", "resource_id", "location"]
    },
    "AzureStorage": {
        "kind": "AzureStorage",
        "properties": ["endpoint", "resource_id", "location"]
    },
    "AzureSqlDatabase": {
        "kind": "AzureSqlDatabase",
        "properties": ["server_endpoint", "resource_id", "location"]
    },
    "AzureCosmosDb": {
        "kind": "AzureCosmosDb",
        "properties": ["account_uri", "resource_id", "location"]
    },
    "Fabric": {
        "kind": "Fabric",
        "properties": ["tenant"]
    },
    "SqlServer": {
        "kind": "SqlServer",
        "properties": ["server_endpoint"]
    },
    "Oracle": {
        "kind": "Oracle",
        "properties": ["host", "port", "service_name"]
    },
    "Teradata": {
        "kind": "Teradata",
        "properties": ["host"]
    },
    "SapS4Hana": {
        "kind": "SapS4Hana",
        "properties": ["application_server", "system_number"]
    }
}

# Common properties users always enter
COMMON_PROPERTIES = ["ds_name", "collection_name"]

# Parsed fields (derived from resource_id) that we want in CSV/payload but not prompted
PARSED_FIELDS = ["subscription_id", "resource_group", "resource_name"]

def get_credentials():
    return ClientSecretCredential(
        client_id=creds["client_id"],
        client_secret=creds["client_secret"],
        tenant_id=creds["tenant_id"]
    )

def get_purview_client():
    credentials = get_credentials()
    return PurviewScanningClient(
        endpoint=f"https://{creds['purview_account_name']}.scan.purview.azure.com",
        credential=credentials,
        logging_enable=True
    )

def get_admin_client():
    credentials = get_credentials()
    return PurviewAccountClient(
        endpoint=f"https://{creds['purview_account_name']}.purview.azure.com",
        credential=credentials,
        logging_enable=True
    )

def resolve_collection_name(user_collection_name: str) -> str:
    admin_client = get_admin_client()
    try:
        collection_list = admin_client.collections.list_collections()
        for collection in collection_list:
            if collection.get("friendlyName", "").lower() == user_collection_name.lower():
                return collection.get("name", user_collection_name)
    except Exception:
        # If admin call fails, fall back to user-provided name
        pass
    return user_collection_name

def parse_resource_id(resource_id: str) -> Dict[str, str]:
    """
    Parse an Azure resourceId string into subscriptionId, resourceGroup, and resourceName.
    Expected format:
    /subscriptions/<subId>/resourceGroups/<rg>/providers/<provider>/<type>/<resourceName>
    """
    parts = resource_id.strip("/").split("/")
    if len(parts) < 6:
        raise ValueError(f"Invalid resourceId format: {resource_id}")
    try:
        subscription_id = parts[1]
        resource_group = parts[3]
        resource_name = parts[-1]
    except IndexError:
        raise ValueError(f"Invalid resourceId format: {resource_id}")
    return {"subscriptionId": subscription_id, "resourceGroup": resource_group, "resourceName": resource_name}

def build_payload(source_type: str, props: Dict[str, str]) -> Dict:
    kind = SOURCE_TYPES[source_type]["kind"]
    properties = {}

    if source_type == "AdlsGen2":
        properties.update({
            "endpoint": props.get("endpoint", ""),
            "location": props.get("location", ""),
            "resourceGroup": props.get("resource_group", ""),
            "resourceId": props.get("resource_id", ""),
            "resourceName": props.get("resource_name", ""),
            "subscriptionId": props.get("subscription_id", "")
        })
    elif source_type == "AzureSubscription":
        properties.update({
            "subscriptionId": props.get("subscription_id", ""),
            "resourceId": props.get("resource_id", "")
        })
    elif source_type == "AzureResourceGroup":
        properties.update({
            "resourceGroup": props.get("resource_group"),
            "subscriptionId": props.get("subscription_id", ""),
            "resourceId": props.get("resource_id", "")
        })
    elif source_type == "AWS account":
        properties.update({
            "awsAccountId": props.get("awsAccount_id"),
        })
    elif source_type == "AzureSynapseAnalytics":
        properties.update({
            "dedicatedSqlEndpoint": props.get("dedicated_SqlEndpoint", ""),
            "serverlessSqlEndpoint": props.get("serverless_SqlEndpoint", ""),
            "location": props.get("location", ""),
            "resourceGroup": props.get("resource_group", ""),
            "resourceId": props.get("resource_id", ""),
            "resourceName": props.get("resource_name", ""),
            "subscriptionId": props.get("subscription_id", "")
        })
    elif source_type == "AzureStorage":
        properties.update({
            "endpoint": props.get("endpoint", ""),
            "location": props.get("location", ""),
            "resourceGroup": props.get("resource_group", ""),
            "resourceId": props.get("resource_id", ""),
            "resourceName": props.get("resource_name", ""),
            "subscriptionId": props.get("subscription_id", "")
        })

    elif source_type == "AzureSqlDatabase":
        properties.update({
            "serverEndpoint": props.get("server_endpoint", ""),
            "resourceId": props.get("resource_id", ""),
            "subscriptionId": props.get("subscription_id", ""),
            "resourceGroup": props.get("resource_group", ""),
            "resourceName": props.get("resource_name", ""),
            "location": props.get("location", "")
        })

    elif source_type == "AzureCosmosDb":
        properties.update({
            "accountUri": props.get("account_uri", ""),
            "location": props.get("location", ""),
            "resourceGroup": props.get("resource_group", ""),
            "resourceId": props.get("resource_id", ""),
            "resourceName": props.get("resource_name", ""),
            "subscriptionId": props.get("subscription_id", "")
        })
    elif source_type == "Fabric":
        properties.update({
            "tenant": props.get("tenant", ""),
        })

    elif source_type == "SqlServer":
        properties.update({"serverEndpoint": props.get("server_endpoint", "")})

    elif source_type == "Oracle":
        properties.update({
            "host": props.get("host", ""),
            "port": props.get("port", ""),
            "serviceName": props.get("service_name", "")
        })

    elif source_type == "Teradata":
        properties.update({"host": props.get("host", "")})

    elif source_type == "SapS4Hana":
        properties.update({
            "applicationServer": props.get("application_server", ""),
            "systemNumber": props.get("system_number", "")
        })

    properties["collection"] = {
        "type": "CollectionReference",
        "referenceName": props.get("collection_name", "")
    }

    return {"name": props.get("ds_name", ""), "kind": kind, "properties": properties}

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

def generate_backup_script(source_type: str, props: Dict[str, str], payload: Dict):
    os.makedirs(BACKUP_DIR, exist_ok=True)
    filename = os.path.join(BACKUP_DIR, f"backup-{props.get('ds_name','datasource')}-registration.py")
    script = f'''import json
from azure.identity import ClientSecretCredential
from azure.purview.scanning import PurviewScanningClient
from authenticate import authenticate

creds = authenticate()

def get_credentials():
    return ClientSecretCredential(
        client_id=creds["client_id"],
        client_secret=creds["client_secret"],
        tenant_id=creds["tenant_id"]
    )

def get_purview_client():
    credentials = get_credentials()
    return PurviewScanningClient(
        endpoint=f"https://{{creds['purview_account_name']}}.scan.purview.azure.com",
        credential=credentials,
        logging_enable=True
    )

def recreate_datasource():
    client = get_purview_client()
    data_source = {json.dumps(payload, indent=2)}
    response = client.data_sources.create_or_update(data_source_name="{props.get('ds_name','')}", body=data_source)
    print("Data source recreated:", response)

if __name__ == "__main__":
    recreate_datasource()
'''
    with open(filename, "w", encoding="utf-8") as f:
        f.write(script)
    print(f"Backup script created: {filename}")

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

    client = get_purview_client()
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

if __name__ == "__main__":
    register_datasource()
