#!/usr/bin/env python3
import os
import csv
import datetime
import json
from typing import Dict
from azure.identity import ClientSecretCredential
from azure.purview.scanning import PurviewScanningClient
from azure.purview.administration.account import PurviewAccountClient
from authenticate import authenticate

# Configuration
BACKUP_DIR = "backup-datasources"
CSV_FILE = os.path.expanduser("~/datasources.csv")  # safe writable path
creds = authenticate()

# Source definitions
SOURCE_TYPES = {
    "AdlsGen2": {
        "kind": "AdlsGen2",
        "properties": ["endpoint", "resource_id", "subscription_id", "resource_group", "resource_name", "location"]
    },
    "AzureStorage": {
        "kind": "AzureStorage",
        "properties": ["endpoint", "resource_id", "subscription_id", "resource_group", "resource_name", "location"]
    },
    "AzureSqlDatabase": {
        "kind": "AzureSqlDatabase",
        "properties": ["server_endpoint", "resource_id", "subscription_id", "resource_group", "resource_name", "location"]
    },
    "AzureCosmosDb": {
        "kind": "AzureCosmosDb",
        "properties": ["account_uri", "resource_id", "subscription_id", "resource_group", "resource_name", "location"]
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

COMMON_PROPERTIES = ["ds_name", "collection_name"]

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
    collection_list = admin_client.collections.list_collections()
    for collection in collection_list:
        if collection.get("friendlyName", "").lower() == user_collection_name.lower():
            return collection.get("name", user_collection_name)
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
    # parts example: ['subscriptions','<subId>','resourceGroups','<rg>','providers',...,'<resourceType>','<resourceName>']
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

def ensure_csv_header(superset_fields):
    """
    Ensure CSV exists and has the superset header. If file exists but header differs, do not overwrite;
    append rows using DictWriter with fieldnames superset_fields.
    """
    file_exists = os.path.exists(CSV_FILE)
    if not file_exists:
        os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True) if os.path.dirname(CSV_FILE) else None
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=superset_fields)
            writer.writeheader()

def get_superset_fields():
    """
    Build a superset header that includes:
    - source_type, COMMON_PROPERTIES
    - all properties from SOURCE_TYPES (unique)
    - timestamp
    """
    fields = ["source_type"] + COMMON_PROPERTIES[:]
    seen = set(fields)
    for st in SOURCE_TYPES.values():
        for p in st["properties"]:
            if p not in seen:
                fields.append(p)
                seen.add(p)
    fields.append("timestamp")
    return fields

def write_to_csv_record(record: Dict[str, str], superset_fields):
    ensure_csv_header(superset_fields)
    # Ensure all keys exist
    row = {k: record.get(k, "") for k in superset_fields}
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=superset_fields)
        writer.writerow(row)

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

    props = {}
    # Collect common properties
    for prop in COMMON_PROPERTIES:
        props[prop] = input(f"Enter {prop}: ").strip()

    # Collect source-specific properties
    for prop in SOURCE_TYPES[source_type]["properties"]:
        # For Azure sources, resource_id is required; prompt for it if present
        props[prop] = input(f"Enter {prop}: ").strip()

    # If Azure source and resource_id provided, parse and populate parsed fields
    if source_type in ["AdlsGen2", "AzureStorage", "AzureSqlDatabase", "AzureCosmosDb"]:
        resource_id = props.get("resource_id", "").strip()
        if resource_id:
            try:
                parsed = parse_resource_id(resource_id)
                props["subscription_id"] = parsed["subscriptionId"]
                props["resource_group"] = parsed["resourceGroup"]
                props["resource_name"] = parsed["resourceName"]
            except ValueError as ve:
                print(f"Resource ID parse error: {ve}")
                # Continue but leave parsed fields empty
                props.setdefault("subscription_id", "")
                props.setdefault("resource_group", "")
                props.setdefault("resource_name", "")
        else:
            props.setdefault("subscription_id", "")
            props.setdefault("resource_group", "")
            props.setdefault("resource_name", "")

    # Resolve collection name to internal Purview name
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

    # Prepare record for CSV using superset header
    superset_fields = get_superset_fields()
    record = {"source_type": source_type}
    # add common props
    for p in COMMON_PROPERTIES:
        record[p] = props.get(p, "")
    # add all possible properties (fill missing with empty string)
    for field in superset_fields:
        if field in ["source_type"] + COMMON_PROPERTIES + ["timestamp"]:
            continue
        record[field] = props.get(field, "")
    record["timestamp"] = datetime.datetime.utcnow().isoformat() + "Z"

    write_to_csv_record(record, superset_fields)
    generate_backup_script(source_type, props, payload)

if __name__ == "__main__":
    register_datasource()
