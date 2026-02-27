import json
from datetime import datetime
import time
from PurviewCatalogClient.purviewcatalogclient import get_purview_catalog_client

date=(datetime.now().strftime('%Y-%m-%%d'))
OUTPUT_FILE = f"Metadata\purview_full_backup{date}.json"

def export_full_metadata():
    # 1. Initialize catalog client
    
    client = get_purview_catalog_client()

    # 2. Step 1: Collect all GUIDs (Discovery)
    all_guids = []
    limit = 1000
    offset = 0
    print("Step 1: Finding all asset GUIDs using pagination")

    while True:
        search_request = {"keywords": "*", "limit": limit, "offset": offset}
        response = client.discovery.query(search_request=search_request)
        batch = response.get("value", [])
        
        if not batch:
            break
            
        # Collect IDs for the next stage
        all_guids.extend([asset.get("id") for asset in batch if asset.get("id")])
        print(f"Discovered {len(all_guids)} assets...")
        offset += limit
        time.sleep(0.5)  # Avoid rate limiting

    # 3. Step 2: Fetch full details for each GUID in batches
    # Purview batch API usually supports up to 100 GUIDs per call
    full_backup_data = []
    batch_size = 100
    print(f"\nStep 2: Fetching full metadata for {len(all_guids)} assets...")

    for i in range(0, len(all_guids), batch_size):
        guid_batch = all_guids[i : i + batch_size]
        try:
            # list_by_guids returns the complete Atlas entity format
            details = client.entity.list_by_guids(guid=guid_batch)
            
            # Extract the actual entity objects (stored in 'entities')
            entities = details.get("entities", [])
            full_backup_data.extend(entities)
            
            print(f"Fetched {len(full_backup_data)} / {len(all_guids)} details...")
        except Exception as e:
            print(f"Failed to fetch batch starting at index {i}: {e}")

    # 4. Save to JSON
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(full_backup_data, f, indent=4)

    print(f"\nSuccess! Full metadata for {len(full_backup_data)} assets saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    export_full_metadata()