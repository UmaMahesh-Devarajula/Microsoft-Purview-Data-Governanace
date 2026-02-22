# Purview Data Source Registeration and backup/recovery Automation

This repository provides a **toolkit** for registering few data sources listed below in **Microsoft Purview** using the REST API. It automates registration, backup script generation, and logging for multiple source types (Azure and non‑Azure).

---

## 🚀 Features
- Register data sources via **Purview Scanning API** (`.scan.purview.azure.com`).
- Resolve **collection names** automatically from friendly names.
- Generate **backup scripts** for disaster recovery.
- Log registrations to CSV for audit and tracking.
- Support for multiple source types:
  - Azure SQL Database
  - Azure Storage
  - ADLS Gen2
  - Azure Cosmos DB
  - SQL Server
  - Oracle
  - Teradata
  - SAP S/4HANA

---

## 📦 Prerequisites
- Python 3.9+  
- Azure SDK packages:
  ```bash
  pip install azure-identity azure-purview-scanning azure-purview-administration
  ```
- A valid **Purview account** with:
  - Service principal (client ID, secret, tenant ID).
  - Permissions to register data sources.

---

## ⚙️ Configuration
Create an `authenticate.py` file that returns your credentials:

```python
def authenticate():
    return {
        "tenant_id": "<tenant-guid>",
        "client_id": "<app-client-id>",
        "client_secret": "<app-client-secret>",
        "purview_account_name": "<purview-account-name>"
    }
```

---

## 🖥️ Usage
Run the script:

```bash
python register_datasource.py
```

You’ll be prompted for:
- Data source type (e.g., `AzureSqlDatabase`)
- Common properties (`ds_name`, `collection_name`)
- Source‑specific properties (e.g., `server_endpoint`, `resource_id`, etc.)

The script will:
1. Build the payload.
2. Print the JSON payload for verification.
3. Register the data source via the **scan endpoint**.
4. Log details to `datasources.csv`.
5. Generate a backup script under `backup-datasources/`.

---

## 📘 Example: Register Azure SQL Database

```text
Enter data source type: AzureSqlDatabase
Enter ds_name: AzSqlDb
Enter collection_name: Silver
Enter server_endpoint: xxxx.database.windows.net
Enter resource_id: /subscriptions/<subId>/resourceGroups/RG/providers/Microsoft.Sql/servers/xxxx
Enter location: westus2
```

Payload sent:

```json
{
  "name": "AzSqlDb",
  "kind": "AzureSqlDatabase",
  "properties": {
    "serverEndpoint": "xxxx.database.windows.net",
    "resourceId": "...",
    "subscriptionId": "...",
    "resourceGroup": "RG",
    "resourceName": "xxxx",
    "location": "westus2",
    "collection": {
      "type": "CollectionReference",
      "referenceName": "Silver"
    }
  }
}
```

---

## 📂 Outputs
- **CSV log** → `datasources.csv`  
- **Backup scripts** → `backup-datasources/backup-<ds_name>-registration.py`

---

## 🛡️ Notes
- Always use the **scan endpoint** for data source registration.  
- Administration tasks (collections, accounts) use the **main Purview endpoint**.  
- Ensure property names are **camelCase** to avoid `BadRequest`.  
- Backup scripts are written in **UTF‑8** to avoid encoding errors.

---

