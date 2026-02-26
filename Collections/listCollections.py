from PurviewClient.purviewclient import get_purview_admin_client

def listCollections:
    admin_client = get_purview_admin_client()
    try:
        collection_list = admin_client.collections.list_collections()
        for collection in collection_list:
            print(collection)
    except Exception as e:
        # If admin call fails, fall back to user-provided name
        print(e)
        pass