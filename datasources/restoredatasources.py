import os
import json
from typing import Dict, List
from PurviewScanClient.purviewscanclient import get_purview_scan_client

def restore_data_sources():
    client = get_purview_scan_client()
    filepath = input("Enter file path")

    try:
        with open(filepath, 'r') as f:
            data_sources = json.load(f)
            
        for ds in data_sources:
            ds_name = ds['name']
            # Re-register data source
            # Note: Sensitive properties like passwords or secrets are NOT returned 
            # by the API and must be managed via Azure Key Vault.
            client.data_sources.create_or_update(
                data_source_name=ds_name, 
                body=ds
            )
            print(f"Restored data source: {ds_name}")
            
    except FileNotFoundError:
        print("Backup file not found.")
    except HttpResponseError as e:
        print(f"Error restoring data sources: {e}")

if "__name__" == "__main__":
    restore_data_sources()
