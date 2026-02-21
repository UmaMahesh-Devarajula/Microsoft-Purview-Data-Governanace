from traceback import print_tb
from azure.identity import ClientSecretCredential 
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.purview import PurviewManagementClient
from azure.mgmt.purview.models import *
from datetime import datetime, timedelta
import time

def createpurview():

    Prerequisites = """
    To create purview account please ensure you meet the following Prerequisites:
    Azure subscription.
    Service Principal
    The user account(Service Principal) that you use to sign in to Azure must be a member of the contributor or owner role, or an administrator of the Azure subscription.
    You must register the 'Microsoft.Purview' resource provider within your Azure subscription.

    If you've met the prerequisites, please ensure you have the following details to procced further
    Tenant ID
    Subscription ID
    Service principal client ID
    Service principal Secret Value
    Resource Group Name (if it's not an existing resource group, this program will create this resource group)
    Region (for your resource group and your Microsoft Purview account.)
    """

    print(Prerequisites)
    Tid= input("Enter Tenant ID:")
    subscription_id = input("Enter Azure Subscription ID:")
    SPid = input("Enter Service Principals Application (client) ID:")
    SPsv = input("Enter Service Principals Secret value:")
    rg_name = input("Enter resource group name:")
    purview_name = input("Enter purview account name (It must be globally unique):")
    location = input("enter region") 

    # Specify your Active Directory client ID, client secret, and tenant ID
    credentials = ClientSecretCredential(client_id=SPid, client_secret=SPsv, tenant_id=Tid) 
    resource_client = ResourceManagementClient(credentials, subscription_id)
    purview_client = PurviewManagementClient(credentials, subscription_id)

    # create the resource group if the resource group does not exits
    rg_list = resource_client.resource_groups.list()
    if rg_name not in rg_list:
        resource_client.resource_groups.create_or_update(rg_name, {"location": location})

    #Create a purview
    identity = Identity(type= "SystemAssigned")
    sku = AccountSku(name= 'Standard', capacity= 4)
    purview_resource = Account(identity=identity,sku=sku,location =location)
       
    try:
        pa = purview_client.accounts.begin_create_or_update(rg_name, purview_name, purview_resource).result()
        print("✅ Purview account created successfully!")
        print("Location:", pa.location, "Name:", purview_name, "ID:", pa.id, "Tags:", pa.tags) 
    except Exception as e:
        print("❌ Error creating account:", e)
        print_tb(e)
 
    # Monitor provisioning state
    while getattr(pa, 'provisioning_state') != "Succeeded":
        pa = purview_client.accounts.get(rg_name, purview_name)  
        print("Provisioning state:", getattr(pa, 'provisioning_state'))
        if getattr(pa, 'provisioning_state') == "Failed":
            print("❌ Account creation failed")
            break
        time.sleep(30)    

# Run script
createpurview()

