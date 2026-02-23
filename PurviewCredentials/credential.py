from azure.identity import ClientSecretCredential
from Authenticate.authenticate import authenticate

def get_credentials():
    creds = authenticate()
    return ClientSecretCredential(
        client_id=creds["client_id"],
        client_secret=creds["client_secret"],
        tenant_id=creds["tenant_id"]
    )