from PurviewSacnClient.purviewscanclient import get_purview_scan_client

def listdatasources():
    client = get_purview_scan_client()
    ds = list(client.data_sources.list_all())

    for s in ds:
        print(s)
    # rows = []

    #for c in collections:
        # rows.append([
        #     c.get("name"),
        #     c.get("friendlyName"),
        #     c.get("description"),
        #     c.get("parentCollection", {}).get("referenceName", "-"),
        #     c["systemData"].get("createdAt"),
        #     c.get("collectionProvisioningState")
        # ])

    # Define headers
    #headers = ["Name", "Friendly Name", "Description", "Parent", "Created At", "State"]

    # Print table
    #print(tabulate(rows, headers=headers, tablefmt="grid"))

if "__name__" == "__main__":
    listdatasources()