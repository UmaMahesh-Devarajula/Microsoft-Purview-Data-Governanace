import os
import csv
from datetime import datetime
import json
from typing import Dict, List
from PurviewScanClient.purviewscanclient import get_purview_scan_client

date=(datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
filepath = fr"datasources\datasources{date}.json"

def exportdatasources():
    client = get_purview_scan_client()
    try:
        # Fetch all data sources
        response = client.data_sources.list_all()
        data_sources = [item for item in response]
        
        # Save to JSON file
        with open(filepath, 'w') as f:
            json.dump(data_sources, f, indent=4)
        print(f"Successfully exported {len(data_sources)} data sources to {filepath}")
        
    except HttpResponseError as e:
        print(f"Error exporting data sources: {e}")

if "__name__" == "__main__":
    exportdatasources()    