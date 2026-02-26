from PurviewClient.purviewclient import get_purview_admin_client
from datetime import datetime
import pandas as pd
import os

def recreate_from_csv():
    client = get_purview_admin_client()
    file_path=input("Enter collection hierarchy file path")

    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return

    df = pd.read_csv(file_path)
    
    # Recreate collections in order
    for _, row in df.iterrows():
        # Skip root (usually has no parentName in the export)
        if pd.isna(row['parentName']):
            continue
            
        collection_body = {
            "friendlyName": row['friendlyName'],
            "parentCollection": {
                "referenceName": row['parentName'],
                "type": "CollectionReference"
            },
            "description": row['description'] if not pd.isna(row['description']) else ""
        }
        
        try:
            client.collections.create_or_update(
                collection_name=row['name'], 
                collection=collection_body
            )
            print(f"Success: {row['friendlyName']}")
        except Exception as e:
            print(f"Failed {row['friendlyName']}: {e}")

if "__name__" == "__main__":
    recreate_from_csv()