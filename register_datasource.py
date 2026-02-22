import os
import csv
import datetime
import json
from azure.identity import ClientSecretCredential
from azure.purview.scanning import PurviewScanningClient
from azure.purview.administration.account import PurviewAccountClient
from authenticate import authenticate

BACKUP_DIR = "backup-datasources"
CSV_FILE = os.path.expanduser("~/datasources.csv")  # safer path

creds = authenticate()

# Superset of all possible fields across Azure + non-Azure sources
CSV_HEADER = [
    "source_type", "ds_name", "collection_name",
    "server_endpoint", "account_uri", "endpoint",
    "resource_id", "subscription_id", "resource_group", "resource_name", "location",
    "host", "port", "service_name",
    "application_server", "system_number",
    "timestamp"
]

SOURCE_TYPES = {
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

def resolve_collection_name(user_collection_name):
    admin_client = get_admin_client()
    collection_list = admin_client.collections.list_collections()
    for collection in collection_list:
        if collection["friendlyName"].lower() == user_collection_name.lower():
            return collection["name"]
    return user_collection_name

def parse_resource_id(resource_id: str):
    parts = resource_id.strip("/").split("/")
    result = {}
    try:
        result["subscriptionId"] = parts[1]
        result["resourceGroup"] = parts[3]
        result["resourceName"] = parts[-1]
    except IndexError:
        raise ValueError(f"Invalid resourceId format: {resource_id}")
    return result

def build_payload(source_type, props):
    kind = SOURCE_TYPES[source_type]["kind"]
    properties = {}

    if source_type in ["AdlsGen2", "AzureStorage", "AzureSqlDatabase", "AzureCosmosDb"]:
        parsed = parse_resource_id(props["resource_id"])
        props["subscription_id"] = parsed["subscriptionId"]
        props["resource_group"] = parsed["resourceGroup"]
        props["resource_name"] = parsed["resourceName"]

    if source_type == "AdlsGen2":
        properties.update({
            "endpoint": props["endpoint"],
            "location": props["location"],
            "resourceGroup": props["resource_group"],
            "resourceId": props["resource_id"],
            "resourceName": props["resource_name"],
            "subscriptionId": props["subscription_id"]
        })

    elif source_type == "AzureStorage":
        properties.update({
            "endpoint": props["endpoint"],
            "location": props["location"],
            "resourceGroup": props["resource_group"],
            "resourceId": props["resource_id"],
            "resourceName": props["resource_name"],
            "subscriptionId": props["subscription_id"]
        })

    elif source_type == "AzureSqlDatabase":
        properties.update({
            "serverEndpoint": props["server_endpoint"],
            "resourceId": props["resource_id"],
            "subscriptionId": props["subscription_id"],
            "resourceGroup": props["resource_group"],
            "resourceName": props["resource_name"],
            "location": props["location"]
        })

    elif source_type == "AzureCosmosDb":
        properties.update({
            "accountUri": props["account_uri"],
            "location": props["location"],
            "resourceGroup": props["resource_group"],
            "resourceId": props["resource_id"],
            "resourceName": props["resource_name"],
            "subscriptionId": props["subscription_id"]
        })

    elif source_type == "SqlServer":
        properties.update({"serverEndpoint": props["server_endpoint"]})

    elif source_type == "Oracle":
        properties.update({
            "host": props["host"],
            "port": props["port"],
            "serviceName": props["service_name"]
        })

    elif source_type == "Teradata":
        properties.update({"host": props["host"]})

    elif source_type == "SapS4Hana":
        properties.update({
            "applicationServer": props["application_server"],
            "systemNumber": props["system_number"]
        })

    properties["collection"] = {
        "type": "CollectionReference",
        "referenceName": props["collection_name"]
    }

    return {
        "name": props["ds_name"],
        "kind": kind,
        "properties": properties
    }

def register_datasource():
    print("Supported source types:", ", ".join(SOURCE_TYPES.keys()))
    source_type = input("Enter data source type: ")
    if source_type not in SOURCE_TYPES:
        print("❌ Unsupported source type.")
        return

    props = {}
    for prop in COMMON_PROPERTIES:
        props[prop] = input(f"Enter {prop}: ")
    for prop in SOURCE_TYPES[source_type]["properties"]:
        props[prop] = input(f"Enter {prop}: ")

    if source_type in ["AdlsGen2", "AzureStorage", "AzureSqlDatabase", "AzureCosmosDb"]:
        parsed = parse_resource_id(props["resource_id"])
        props["subscription_id"] = parsed["subscriptionId"]
        props["resource_group"] = parsed["resourceGroup"]
        props["resource_name"] = parsed["resourceName"]

    props["collection_name"] = resolve_collection_name(props["collection_name"])

    payload = build_payload(source_type, props)

    print("Payload being sent:")
    print(json.dumps(payload, indent=2))

    client = get_purview_client()

    try:
        response = client.data_sources.create_or_update(props["ds_name"], body=payload)
        print("✅ Data source registered:", response)
    except Exception as e:
        print("❌ Error registering data source:", e)
        return

    write_to_csv(source_type, props)
    generate_backup_script(source_type, props)

def write_to_csv(source_type, props):
    file_exists = os.path.exists(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        if not file_exists:
            writer.writeheader()

        row = {key: props.get(key, "") for key in CSV_HEADER}
        row["source_type"] = source_type
        row["timestamp"] = datetime.datetime.now()
        writer.writerow(row)

def generate_backup_script(source_type, props):
    os.makedirs(BACKUP_DIR, exist_ok=True)
    filename = os.path.join(BACKUP_DIR, f"backup-{props['ds_name']}-registration.py")
    payload = build_payload(source_type, props)

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
    response = client.data_sources.create_or_update(data_source_name="{props['ds_name']}", body=data_source)
    print("Data source recreated:", response)

if __name__ == "__main__":
    recreate_datasource()
'''
    with open(filename, "w", encoding="utf-8") as f:
        f.write(script)
