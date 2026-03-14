from PurviewScanClient.purviewscanclient import get_purview_scan_client

def deletedatasource():
    ds_name = input("Enter Data source you want to delete: ")
    client = get_purview_scan_client()
    try:
        response = client.datadata_sources.delete(data_source_name= ds_name)
        print(f"Data Source {ds_name} is deleted sucessfully")
    except Exception as e:
        print(f"Error in deleting Data Source {ds_name}:", e)
        return

if "__name__" == "__main__":
    deletedatasource()