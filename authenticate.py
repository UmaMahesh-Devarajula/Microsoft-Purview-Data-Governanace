import json
import os

CONFIG_FILE = "purview_config.json"

def authenticate():
    # If config file exists, load credentials
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            config = json.load(f)
        return config

    # First-time setup: prompt user for credentials
    print("🔐 First-time authentication setup:")
    tenant_id = input("Enter Tenant ID: ")
    client_id = input("Enter Service Principal Client ID: ")
    client_secret = input("Enter Service Principal Secret Value: ")
    purview_account_name = input("Enter Purview Account Name: ")

    # Save credentials to config file
    config = {
        "tenant_id": tenant_id,
        "client_id": client_id,
        "client_secret": client_secret,
        "purview_account_name": purview_account_name
    }

    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=4)

    print("✅ Credentials saved to purview_config.json")
    return config

# Example usage
if __name__ == "__main__":
    creds = authenticate()
    print("Purview Account:", creds["purview_account_name"])
