from azure.purview.scanning import PurviewScanningClient
from azure.identity import ClientSecretCredential 
from azure.core.exceptions import HttpResponseError
from azure.purview.administration.account import PurviewAccountClient
from authenticate import authenticate

creds = authenticate() 
purview_endpoint = f"https://{creds['purview_account_name']}.purview.azure.com"
purview_scan_endpoint = f"https://{creds['purview_account_name']}.scan.purview.azure.com"
storage_name = input("name of your Storage Account: ")
storage_id = input("resource id of your Storage Account: ")
rg_name = input("name of storage accounts resource group: ")
rg_location = input("location of data source resource group: ")
collection_name = input("name of the collection where you want to register the data source: ")
ds_name = input("a friendly data source name: ")


catalog_client = PurviewCatalogClient( endpoint=f"https://{creds['purview_account_name']}.purview.azure.com", credential=credential )

def get_credentials():
	credentials = ClientSecretCredential(client_id=creds["client_id"], client_secret=creds["client_secret"], tenant_id=creds["tenant_id"])
	return credentials

def get_purview_client():
	credentials = get_credentials()
	client = PurviewScanningClient(endpoint=purview_scan_endpoint, credential=credentials, logging_enable=True)  
	return client

def get_admin_client():
	credentials = get_credentials()
	client = PurviewAccountClient(endpoint=purview_endpoint, credential=credentials, logging_enable=True)
	return client

try:
	admin_client = get_admin_client()
except ValueError as e:
        print(e)

collection_list = admin_client.collections.list_collections()
for collection in collection_list:
	if collection["friendlyName"].lower() == collection_name.lower():
		collection_name = collection["name"]


body_input = {
	"kind": "AzureStorage",
	"properties": {
		"endpoint": f"https://{storage_name}.blob.core.windows.net/",
		"resourceGroup": rg_name,
		"location": rg_location,
		"resourceName": storage_name,
 		"resourceId": storage_id,
		"collection": {
			"type": "CollectionReference",
			"referenceName": collection_name
		},
		"dataUseGovernance": "Disabled"
	}
}

try:
	client = get_purview_client()
except ValueError as e:
        print(e)

try:
	response = client.data_sources.create_or_update(ds_name, body=body_input)
	print(response)
	print(f"Data source {ds_name} successfully created or updated")
except HttpResponseError as e:
    print(e)