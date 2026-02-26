from PurviewClient.purviewclient import get_purview_admin_client
from datetime import datetime
import pandas as pd
import os
import json
import requests
from Authenticate.authenticate import authenticate
from azure.identity import ClientSecretCredential

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
            if row['name'] != row['friendlyName']:
                domain_name= row['name']
                print(domain_name)
                domain_friendly_name = row['friendlyName']  
                print(domain_friendly_name)
                def create_domain(domain_name, domain_friendly_name):
                    # 2. Get Auth Token using Service Principal
                    r=authenticate()
                    cred = ClientSecretCredential(r["tenant_id"], r["client_id"], r["client_secret"])
                    token = cred.get_token(f"https://{r["tenant_id"]}-api.purview-service.microsoft.com")
                    print(token.token)

                    # 3. Prepare REST Request
                    url = f"{r["tenant_id"]}-api.purview-service.microsoft.com/account/domains/{domain_name}?api-version=2023-12-01-preview"
                    headers = {
                        "Authorization": f"Bearer {token.token}",
                        "Content-Type": "application/json"
                    }
                    body = {
                        "properties": {
                            "containerType":"Domain",
                            "friendlyName": row['friendlyName'],
                            "description": row['description']
                        }
                    }

                    response = requests.put(url, headers=headers, json=body)

                    if response.status_code in [200, 201]:
                        print(f"SUCCESS: Domain '{row['friendlyName']}' created.")
                    else:
                        print(f"[{get_now()}] ERROR {response.status_code}: {response.text}")

                create_domain(domain_name, domain_friendly_name)
        
        continue
            
        collection_body = {
            "friendlyName": row['friendlyName'],
            "parentCollection": {
                "referenceName": row['parentName'],
                "type": "CollectionReference"
            }
        }
        
        try:
            client.collections.create_or_update_collection(
                collection_name=row['name'], 
                collection=collection_body
            )
            print(f"Success: {row['friendlyName']}")
        except Exception as e:
            print(f"Failed {row['friendlyName']}: {e}")

if "__name__" == "__main__":
    recreate_from_csv()