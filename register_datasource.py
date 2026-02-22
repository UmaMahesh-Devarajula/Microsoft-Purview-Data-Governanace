import os
import csv
import datetime
from azure.identity import ClientSecretCredential
from azure.purview.scanning import PurviewScanningClient
from authenticate import authenticate

BACKUP_DIR = "backup-datasources"
CSV_FILE = "datasources.csv"

# Supported source types and required properties (per Microsoft docs)
SOURCE_TYPES = {
    "AdlsGen2": {
        "kind": "AdlsGen2",
        "properties": ["resource_id", "rg_location"]
    },
    "AzureStorage": {
        "kind": "AzureStorage",
        "properties": ["resource_id", "rg_location"]
    },
    "AzureSqlDatabase": {
        "kind": "AzureSqlDatabase",
        "properties": ["server_endpoint", "resource_id"]
    },
    "AzureCosmosDb": {
        "kind": "AzureCosmosDb",
        "properties": ["account_name", "resource_id"]
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
    # Extend with Snowflake, AmazonS3, PostgreSQL, MySQL, etc.
}

COMMON_PROPERTIES = ["ds_name", "collection_name"]

def get_credentials(config):
    return ClientSecretCredential(
        tenant_id=config["tenant_id"],
        client_id=config["client_id"],
        client_secret=config["client_secret"]
    )

def build_payload(source_type, props):
    kind = SOURCE_TYPES[source_type]["kind"]
    properties = {}

    if source_type in ["AdlsGen2", "AzureStorage"]:
        properties = {
            "resourceId": props["resource_id"],
            "location": props["rg_location"]
        }

    elif source_type == "AzureSqlDatabase":
        properties = {
            "serverEndpoint": props["server_endpoint"],
            "resourceId": props["resource_id"]
        }

    elif source_type == "AzureCosmosDb":
        properties = {
            "accountName": props["account_name"],
            "resourceId": props["resource_id"]
        }

    elif source_type == "SqlServer":
        properties = {
            "serverEndpoint": props["server_endpoint"]
        }

    elif source_type == "Oracle":
        properties = {
            "host": props["host"],
            "port": props["port"],
            "serviceName": props["service_name"]
        }

    elif source_type == "Teradata":
        properties = {
            "host": props["host"]
        }

    elif source_type == "SapS4Hana":
        properties = {
            "applicationServer": props["application_server"],
            "systemNumber": props["system_number"]
        }

    # Add common collection reference
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
    config = authenticate()
    purview_account = config["purview_account_name"]
    endpoint = f"https://{purview_account}.purview.azure.com"

    print("Supported source types:", ", ".join(SOURCE_TYPES.keys()))
    source_type = input("Enter data source type: ")
    if source_type not in SOURCE_TYPES:
        print("❌ Unsupported source type.")
        return

    props = {}
    # Collect common properties
    for prop in COMMON_PROPERTIES:
        props[prop] = input(f"Enter {prop}: ")
    # Collect type-specific properties
    for prop in SOURCE_TYPES[source_type]["properties"]:
        props[prop] = input(f"Enter {prop}: ")

    payload = build_payload(source_type, props)

    credential = get_credentials(config)
    client = PurviewScanningClient(endpoint=endpoint, credential=credential)

    try:
        response = client.data_sources.create_or_update(data_source_name=props["ds_name"], body=payload)
        print("✅ Data source registered:", response)
    except Exception as e:
        print("❌ Error registering data source:", e)
        return

    row = [source_type] + [props[p] for p in COMMON_PROPERTIES + SOURCE_TYPES[source_type]["properties"]] + [datetime.datetime.now()]
    write_to_csv(row, source_type)
    generate_backup_script(source_type, props, config)

def write_to_csv(row, source_type):
    header = ["source_type"] + COMMON_PROPERTIES + SOURCE_TYPES[source_type]["properties"] + ["timestamp"]
    file_exists = os.path.exists(CSV_FILE)
    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)

def generate_backup_script(source_type, props, config):
    os.makedirs(BACKUP_DIR, exist_ok=True)
    filename = os.path.join(BACKUP_DIR, f"backup-{props['ds_name']}-registration.py")
    payload = build_payload(source_type, props)

    script = f'''from azure.identity import ClientSecretCredential
from azure.purview.scanning import PurviewScanningClient
from authenticate import authenticate

def recreate_datasource():
    creds = authenticate()
    credential = ClientSecretCredential(
        tenant_id=creds["tenant_id"],
        client_id=creds["client_id"],
        client_secret=creds["client_secret"]
    )
    endpoint = f"https://{{creds['purview_account_name']}}.purview.azure.com"
    client = PurviewScanningClient(endpoint=endpoint, credential=credential)

    data_source = {payload}

    response = client.data_sources.create_or_update(data_source_name="{props['ds_name']}", body=data_source)
    print("✅ Data source recreated:", response)

if __name__ == "__main__":
    recreate_datasource()
'''
    with open(filename, "w") as f:
        f.write(script)
    print(f"📂 Backup script created: {filename}")

if __name__ == "__main__":
    register_datasource()
