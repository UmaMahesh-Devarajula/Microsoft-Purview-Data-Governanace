from PurviewScanClient.purviewscanclient import get_purview_scan_client
from tabulate import tabulate

def listdatasources():
    client = get_purview_scan_client()
    ds = list(client.data_sources.list_all())

    rows = []
    for d in ds:
        rows.append([
            d.get("kind"),
            d.get("name"),
            d.get("properties").get("createdAt")
            d.get("properties").get("parentCollection")
            d.get("properties").get("collection").get("referenceName")
            d.get("properties")
        ])

    # Define headers
    headers = ["Kind", "Data Source Name", "Created At", "Parent Collection Name", "Collection Name", "Properties"]

    # Print table
    print(tabulate(rows, headers=headers, tablefmt="grid"))

if "__name__" == "__main__":
    listdatasources()