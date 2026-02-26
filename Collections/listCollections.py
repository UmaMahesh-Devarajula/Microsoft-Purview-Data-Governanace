from PurviewClient.purviewclient import get_purview_admin_client
from datetime import datetime
import pandas as pd
import os

def listCollections():
    client = get_purview_admin_client()
# Fetch collections
    collections = client.collections.list_collections()
    print(collections)
    # Extract hierarchy info
    collection_data = []
    for coll in collections:
        collectionx_data.append({
        "name": coll.get("name"),
        "friendlyName": coll.get("friendlyName"),
        "parentName": coll.get("parentCollection", {}).get("referenceName")
    })

    # Write to CSV
    df = pd.DataFrame(collection_data)
    df.to_csv(f"purview_hierarchy{datetime.now()}.csv", index=False)
    print("Collection structure exported to purview_hierarchy.csv")

if __name__ == "__main__":
    listCollections()