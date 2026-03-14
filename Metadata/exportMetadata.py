import json
import time
from datetime import datetime
from PurviewCatalogClient.purviewcatalogclient import get_purview_catalog_client

date=(datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
OUTPUT_FILE = fr"Metadata\purview_full_backup_{date}.json"

def exportmetadata():
    client = get_purview_catalog_client()

    all_guids = []
    limit = 1000
    offset = 0

    print("Step 1: Collecting GUIDs...")
    while True:
        search_request = {"keywords": "*", "limit": limit, "offset": offset}
        response = client.discovery.query(search_request=search_request)
        batch = response.get("value", [])
        if not batch:
            break
        all_guids.extend([asset.get("id") for asset in batch if asset.get("id")])
        print(f"Discovered {len(all_guids)} assets...")
        offset += limit
        time.sleep(0.2)

    print(f"\nStep 2: Fetching full metadata for {len(all_guids)} assets...")
    full_backup_data = []
    batch_size = 100
    for i in range(0, len(all_guids), batch_size):
        guid_batch = all_guids[i : i + batch_size]
        try:
            details = client.entity.list_by_guids(guids=guid_batch)
            entities = details.get("entities", [])
            full_backup_data.extend(entities)
            print(f"Fetched {len(full_backup_data)} / {len(all_guids)} entities...")
        except Exception as e:
            print(f"Failed batch {i}: {e}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(full_backup_data, f, indent=4)

    print(f"\n✅ Success! Metadata saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    exportmetadata()
