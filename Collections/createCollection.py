from PurviewClient.purviewclient import get_purview_admin_client
from datetime import datetime

def createCollection():
    client = get_purview_admin_client()
    c_name = input("Enter Collection Name: ")
    c_description = input("Enter collection description: ")
    c_parent = input("Enter parent collection name: ")

    c_body = {
       "description": f"{c_description}",  # Optional. Gets or sets the description.
       "friendlyName": f"{c_name}",  # Optional. Gets or sets the friendly name of the collection.
       "name": f"{c_name}",  # Optional. Gets the name.
       "parentCollection": {
           "referenceName": f"{c_parent}",  # Optional. Gets or sets the reference name.
           "type": "CollectionReference" 
           }
           
    }
    response= client.create_or_update_collection(collection_name= c_name, collection= Any, body= c_body)
    print(response)

if "__name__" == "__main__":
    createCollection()