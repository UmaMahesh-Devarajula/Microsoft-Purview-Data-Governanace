import json
import os
import time
from PurviewCatalogClient.purviewcatalogclient import get_purview_catalog_client



def restoreMetadata():
    # 1. Initialize Client
    client = get_purview_catalog_client()

    BACKUP_FILE = input("enter file path")

    # 2. Load backup data
    print(f"Loading backup from {BACKUP_FILE}...")
    try:
        with open(BACKUP_FILE, "r", encoding="utf-8") as f:
            assets_to_restore = json.load(f)
    except FileNotFoundError:
        print("Backup file not found. Please run the export script first.")
        return

    # 3. Restore in batches
    batch_size = 100
    print(f"Starting restoration of {len(assets_to_restore)} assets...")

    for i in range(0, len(assets_to_restore), batch_size):
        batch = assets_to_restore[i : i + batch_size]
        
        # Prepare the Atlas bulk request payload
        payload = {"entities": batch}

        try:
            # Use the bulk create or update method
            # This will create new assets or update existing ones based on qualifiedName
            response = client.entity.create_or_update_entities(entities=payload)
            
            # Check for assigned GUIDs in the response
            guid_assignments = response.get("guidAssignments", {})
            print(f"Restored batch {i//batch_size + 1}: {len(guid_assignments)} entities processed.")
            
            time.sleep(1)  # Optional: slight delay to manage API load
        except Exception as e:
            print(f"Failed to restore batch starting at index {i}: {e}")

    print("\nRestoration process complete!")

if __name__ == "__main__":
    restoreMetadata()