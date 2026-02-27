import json
import os
import time
from PurviewCatalogClient.purviewcatalogclient import get_purview_catalog_client


def restoreMetadata():
    
    client = get_purview_catalog_client

    BACKUP_FILE = input("enter filepath")

    with open(BACKUP_FILE, "r") as f:
        assets = json.load(f)

    cleaned_assets = []
    for asset in assets:
        # Create a copy to avoid mutating original data
        new_asset = {
            "typeName": asset["typeName"],
            "attributes": asset["attributes"]
        }
        
        # Look for parent attributes (like 'table' for a column or 'db' for a table)
        # We replace the GUID reference with a QualifiedName reference
        if "relationshipAttributes" in asset:
            new_rel_attrs = {}
            for rel_name, rel_val in asset["relationshipAttributes"].items():
                if isinstance(rel_val, dict) and "qualifiedName" in rel_val.get("attributes", {}):
                    # Link by QualifiedName instead of the old, broken GUID
                    new_rel_attrs[rel_name] = {
                        "typeName": rel_val["typeName"],
                        "uniqueAttributes": {
                            "qualifiedName": rel_val["attributes"]["qualifiedName"]
                        }
                    }
            if new_rel_attrs:
                new_asset["relationshipAttributes"] = new_rel_attrs

        cleaned_assets.append(new_asset)

    # Upload in batches
    batch_size = 50
    for i in range(0, len(cleaned_assets), batch_size):
        batch = cleaned_assets[i : i + batch_size]
        try:
            client.entity.create_or_update_entities(entities={"entities": batch})
            print(f"Restored batch {i//batch_size + 1} with hierarchy.")
        except Exception as e:
            print(f"Error in batch {i}: {e}")

if __name__ == "__main__":
    restoreMetadata()
