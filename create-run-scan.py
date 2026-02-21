import uuid
from azure.purview.scanning import PurviewScanningClient
from azure.purview.administration.account import PurviewAccountClient
from azure.identity import ClientSecretCredential
from authenticate import authenticate

def get_credentials(config):
    return ClientSecretCredential(
        client_id=config["client_id"],
        client_secret=config["client_secret"],
        tenant_id=config["tenant_id"]
    )

def get_collection_name(admin_client, friendly_name):
    collections = admin_client.collections.list_collections()
    for collection in collections:
        if collection["friendlyName"].lower() == friendly_name.lower():
            return collection["name"]
    return None

def trigger_adls_scan():
    config = authenticate()
    purview_account = config["purview_account_name"]
    purview_endpoint = f"https://{purview_account}.purview.azure.com"

    ds_name = input("Enter registered data source name: ")
    scan_name = input("Enter scan name: ")
    collection_friendly_name = input("Enter friendly collection name: ")

    credentials = get_credentials(config)
    admin_client = PurviewAccountClient(endpoint=purview_endpoint, credential=credentials)
    collection_name = get_collection_name(admin_client, collection_friendly_name)

    if not collection_name:
        print("❌ Collection not found.")
        return

    scanning_client = PurviewScanningClient(endpoint=purview_endpoint, credential=credentials)

    scan_payload = {
        "kind": "AdlsGen2Msi",
        "properties": {
            "scanRulesetName": "AdlsGen",
            "scanRulesetType": "System",
            "collection": {
                "type": "CollectionReference",
                "referenceName": collection_name
            }
        }
    }

    try:
        response = scanning_client.scans.create_or_update(
            data_source_name=ds_name,
            scan_name=scan_name,
            body=scan_payload
        )
        print("✅ Scan created or updated:", response)
    except Exception as e:
        print("❌ Error creating scan:", e)
        return

    run_id = str(uuid.uuid4())
    try:
        response = scanning_client.scan_result.run_scan(
            data_source_name=ds_name,
            scan_name=scan_name,
            run_id=run_id
        )
        print("✅ Scan started:", response)
    except Exception as e:
        print("❌ Error starting scan:", e)

if __name__ == "__main__":
    trigger_adls_scan()
