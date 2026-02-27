from PurviewCredentials.credential import get_credentials
from Authenticate.authenticate import authenticate
from azure.purview.catalog import PurviewCatalogClient

def get_purview_catalog_client():
    credentials = get_credentials()
    creds = authenticate()
    return PurviewCatalogClient(
        endpoint=f"https://{creds['purview_account_name']}.purview.azure.com",
        credential=credentials,
        logging_enable=True
    )