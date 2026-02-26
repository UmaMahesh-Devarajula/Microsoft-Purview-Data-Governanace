from PurviewClient.purviewclient import get_purview_admin_client
from datetime import datetime
import pandas as pd
import os

def list_collections():
    client = get_purview_admin_client()
# Fetch collections
    collections = client.collections.list_collections()
    print(collections)
    # Extract hierarchy info
    collection_data = []
    for coll in collections:
        collection_data.append({
            "collection_name": coll.name,
            "friendly_name": coll.friendly_name,
            # Parent collection is usually a dict containing 'referenceName'
            "parent_collection": coll.parent_collection.reference_name if coll.parent_collection else None
        })

    # Write to CSV
    df = pd.DataFrame(collection_data)
    df.to_csv(f"purview_hierarchy{datetime.now()}.csv", index=False)
    print("Collection structure exported to purview_hierarchy.csv")

if __name__ == "__main__":
    list_collections()