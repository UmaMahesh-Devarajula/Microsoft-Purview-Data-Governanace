#!/usr/bin/env python3
"""
create-scan.py

Rebuilt Purview scan automation with robust scanAuthorization handling.

- Endpoint: https://{account}.purview.azure.com/scan
- Creates Key Vault connections and SqlAuth credentials (KeyVaultSecret)
- Creates scans and retries with alternative scanAuthorization shapes if needed
- Starts scan runs, polls status, logs CSV/JSON, generates backup scripts
- Verbose debug output for HTTP requests/responses

Requirements:
- pip install azure-identity requests
- authenticate() in authenticate.py returning dict:
  { "client_id", "client_secret", "tenant_id", "purview_account_name" }
"""
import os
import json
import csv
import time
import datetime
from typing import Dict, Any, List, Optional, Tuple
import requests
from azure.identity import ClientSecretCredential
from authenticate import authenticate

API_VERSION = "2023-09-01"
BACKUP_DIR = "backup-scans"
CRED_BACKUP_DIR = "backup-credentials"
CSV_FILE = os.path.join(os.getcwd(), "scans_log.csv")
JSON_LOG = os.path.join(os.getcwd(), "scans_log.json")
CRED_CSV = os.path.join(os.getcwd(), "credentials_log.csv")
CRED_JSON = os.path.join(os.getcwd(), "credentials_log.json")

creds = authenticate()

# ---------------------------
# Endpoint helpers
# ---------------------------
def normalize_account_name(raw: str) -> str:
    s = raw.strip()
    if s.startswith("http://") or s.startswith("https://"):
        s = s.split("://", 1)[1]
    s = s.split("/", 1)[0]
    if s.endswith(".purview.azure.com"):
        s = s[: -len(".purview.azure.com")]
    if s.endswith(".scan.purview.azure.com"):
        s = s[: -len(".scan.purview.azure.com")]
    return s

def scanning_endpoint_for(account_name: str) -> str:
    return f"https://{account_name}.purview.azure.com/scan"

def get_access_token_for(creds_local: Dict[str, str]) -> str:
    credential = ClientSecretCredential(
        client_id=creds_local["client_id"],
        client_secret=creds_local["client_secret"],
        tenant_id=creds_local["tenant_id"]
    )
    token = credential.get_token("https://purview.azure.net/.default")
    return token.token

def resolve_endpoint(creds_local: Dict[str, str]) -> str:
    candidate = normalize_account_name(creds_local.get("purview_account_name", ""))
    endpoint = scanning_endpoint_for(candidate)
    token = get_access_token_for(creds_local)
    url = endpoint.rstrip("/") + "/azureKeyVaults"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"api-version": API_VERSION}, timeout=10)
    if resp.status_code in (200, 204):
        return endpoint
    print("Auto-resolve of scanning endpoint failed. Please enter Purview account name or full endpoint.")
    while True:
        override = input("Purview account name or full endpoint: ").strip()
        if not override:
            continue
        if override.startswith("http://") or override.startswith("https://") or ".purview.azure.com" in override:
            host = override.split("://", 1)[-1].split("/", 1)[0]
            if host.endswith(".purview.azure.com"):
                prefix = host.split(".purview.azure.com", 1)[0]
                endpoint = scanning_endpoint_for(prefix)
            else:
                endpoint = override.rstrip("/")
        else:
            endpoint = scanning_endpoint_for(normalize_account_name(override))
        token = get_access_token_for(creds_local)
        url = endpoint.rstrip("/") + "/azureKeyVaults"
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"api-version": API_VERSION}, timeout=10)
        if resp.status_code in (200, 204):
            return endpoint
        print("Validation failed for provided endpoint. Try again.")

# ---------------------------
# HTTP call with debug
# ---------------------------
def call_purview(method: str, endpoint: str, path: str, token: str, params: Dict[str, str] = None, body: Any = None, timeout: int = 120) -> Any:
    url = endpoint.rstrip("/") + path
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    print("DEBUG: Purview request ->", method, url)
    if params:
        print("DEBUG: params ->", params)
    if body is not None:
        try:
            print("DEBUG: body ->", json.dumps(body, indent=2))
        except Exception:
            print("DEBUG: body ->", str(body))
    resp = requests.request(method, url, headers=headers, params=params, json=body, timeout=timeout)
    print("DEBUG: HTTP status:", resp.status_code)
    print("DEBUG: response text:", resp.text)
    try:
        req = resp.request
        print("DEBUG: request url:", req.url)
        print("DEBUG: request headers:", dict(req.headers))
        if req.body:
            try:
                print("DEBUG: request body (raw):", req.body.decode() if isinstance(req.body, bytes) else req.body)
            except Exception:
                print("DEBUG: request body (raw):", str(req.body))
    except Exception:
        pass
    try:
        data = resp.json()
    except ValueError:
        data = {"raw_text": resp.text}
    if not resp.ok:
        raise RuntimeError(f"Purview API error {resp.status_code}: {resp.text}")
    return data

# ---------------------------
# CSV/JSON logging helpers
# ---------------------------
def reconcile_and_ensure_csv(csv_path: str, superset_fields: List[str]) -> List[str]:
    dirpath = os.path.dirname(csv_path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    if not os.path.exists(csv_path):
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=superset_fields)
            writer.writeheader()
        return superset_fields
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        existing_fields = reader.fieldnames or []
        rows = list(reader)
    if existing_fields == superset_fields:
        return superset_fields
    merged = existing_fields[:]
    for fld in superset_fields:
        if fld not in merged:
            merged.append(fld)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=merged)
        writer.writeheader()
        for r in rows:
            out = {k: r.get(k, "") for k in merged}
            writer.writerow(out)
    return merged

def append_csv_record(csv_path: str, record: Dict[str, Any], superset_fields: List[str]):
    merged_fields = reconcile_and_ensure_csv(csv_path, superset_fields)
    row = {k: record.get(k, "") for k in merged_fields}
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=merged_fields)
        writer.writerow(row)

def append_json_log(json_path: str, entry: Dict[str, Any]):
    logs = []
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                logs = json.load(f)
        except Exception:
            logs = []
    logs.append(entry)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(logs, f, indent=2, default=str)

# ---------------------------
# Key Vault connection helpers
# ---------------------------
def key_vault_connection_exists(endpoint: str, token: str, kv_name: str) -> bool:
    try:
        call_purview("GET", endpoint, f"/azureKeyVaults/{kv_name}", token, params={"api-version": API_VERSION})
        return True
    except Exception:
        return False

def create_or_replace_key_vault_connection(endpoint: str, token: str, kv_name: str, base_url: str, description: Optional[str] = None) -> Dict[str, Any]:
    body = {"properties": {"baseUrl": base_url}}
    if description:
        body["properties"]["description"] = description
    return call_purview("PUT", endpoint, f"/azureKeyVaults/{kv_name}", token, params={"api-version": API_VERSION}, body=body)

# ---------------------------
# Credential helpers (SqlAuth uses KeyVaultSecret)
# ---------------------------
def credential_exists(endpoint: str, token: str, credential_name: str) -> bool:
    try:
        call_purview("GET", endpoint, f"/credentials/{credential_name}", token, params={"api-version": API_VERSION})
        return True
    except Exception:
        return False

def create_sqlauth_credential(endpoint: str, token: str) -> Optional[Dict[str, Any]]:
    name = input("Credential name: ").strip()
    username = input("SQL username: ").strip()
    print("SqlAuth requires the password to be stored in Key Vault.")
    secret_name = input("Key Vault secret name (e.g., sql-password-secret): ").strip()
    vault_name = input("Key Vault name (e.g., kvx09): ").strip()
    vault_resource_id = input("Key Vault resourceId (full ARM id): ").strip()
    if not key_vault_connection_exists(endpoint, token, vault_name):
        print(f"Key Vault connection '{vault_name}' not found in Purview.")
        create_kv = input("Create Key Vault connection now? (y/n): ").strip().lower()
        if create_kv == "y":
            base_url = input("Key Vault baseUrl (https://<name>.vault.azure.net/): ").strip()
            desc = input("Description (optional): ").strip()
            try:
                create_or_replace_key_vault_connection(endpoint, token, vault_name, base_url, desc)
                print("Key Vault connection created.")
            except Exception as e:
                print("Failed to create Key Vault connection:", e)
                return None
        else:
            print("Please create Key Vault connection in Purview first and retry.")
            return None
    body = {
        "kind": "SqlAuth",
        "properties": {
            "credential": {
                "type": "KeyVaultSecret",
                "secretName": secret_name,
                "vaultName": vault_name,
                "vaultResourceId": vault_resource_id
            },
            "typeProperties": {
                "user": username,
                "password": {
                    "secretName": secret_name,
                    "vaultName": vault_name,
                    "vaultResourceId": vault_resource_id
                }
            }
        }
    }
    try:
        resp = create_or_replace_credential(endpoint, token, name, body)
    except Exception as e:
        print("Failed to create SqlAuth credential:", e)
        return None
    generate_credential_backup_script(name, body)
    record = {"credential_name": name, "kind": "SqlAuth", "properties": json.dumps(body["properties"]), "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    append_csv_record(CRED_CSV, record, ["credential_name", "kind", "properties", "timestamp"])
    append_json_log(CRED_JSON, record)
    print("SqlAuth credential created:", resp.get("name", name))
    return resp

def create_or_replace_credential(endpoint: str, token: str, credential_name: str, credential_body: Dict[str, Any]) -> Dict[str, Any]:
    return call_purview("PUT", endpoint, f"/credentials/{credential_name}", token, params={"api-version": API_VERSION}, body=credential_body)

def generate_credential_backup_script(credential_name: str, credential_body: Dict[str, Any]):
    os.makedirs(CRED_BACKUP_DIR, exist_ok=True)
    filename = os.path.join(CRED_BACKUP_DIR, f"{credential_name}-credential.py")
    script = f'''#!/usr/bin/env python3
import json, requests
from azure.identity import ClientSecretCredential
from authenticate import authenticate
creds = authenticate()
def get_token():
    cred = ClientSecretCredential(client_id=creds["client_id"], client_secret=creds["client_secret"], tenant_id=creds["tenant_id"])
    return cred.get_token("https://purview.azure.net/.default").token
def main():
    token = get_token()
    endpoint = f"https://{{creds['purview_account_name']}}.purview.azure.com/scan"
    path = "/credentials/{credential_name}"
    url = endpoint.rstrip("/") + path + "?api-version={API_VERSION}"
    headers = {{"Authorization": f"Bearer {{token}}", "Content-Type": "application/json"}}
    body = {json.dumps(credential_body, indent=2)}
    resp = requests.put(url, headers=headers, json=body)
    print("Status:", resp.status_code); print(resp.text)
if __name__ == "__main__": main()
'''
    with open(filename, "w", encoding="utf-8") as f:
        f.write(script)
    print("Credential backup script created:", filename)

# ---------------------------
# Scan helpers with retry for scanAuthorizationType
# ---------------------------
def ensure_scan_exists_with_retries(endpoint: str, token: str, datasource_name: str, scan_name: str, kind: str, properties: Dict[str, Any]) -> Dict[str, Any]:
    """
    Try to create the scan. If the server returns Scan_InvalidType for the chosen
    scanAuthorizationType, retry with alternative authorization shapes.
    """
    get_path = f"/datasources/{datasource_name}/scans/{scan_name}"
    params = {"api-version": API_VERSION}
    try:
        scan = call_purview("GET", endpoint, get_path, token, params=params)
        print("Scan exists:", scan_name)
        return scan
    except Exception as e_get:
        print("Scan GET failed or not found, creating. Reason:", repr(e_get))

    # Attempt list of candidate authorization types/shapes.
    # Order: Credential (generic), SqlAuthentication (explicit), ManagedIdentity
    # For Credential we expect properties already contains scanAuthorizationType/scanAuthorization
    candidates = []

    # If user already provided scanAuthorizationType in properties, try it first
    if "scanAuthorizationType" in properties and "scanAuthorization" in properties:
        candidates.append(properties)
    # Candidate 1: Credential (generic)
    candidates.append({**properties, **{
        "scanAuthorizationType": "Credential",
        "scanAuthorization": {"credential": properties.get("scanAuthorization", {}).get("credential", {"referenceName": properties.get("credentials", {}).get("referenceName")})}
    }})
    # Candidate 2: SqlAuthentication (explicit)
    candidates.append({**properties, **{
        "scanAuthorizationType": "SqlAuthentication",
        "scanAuthorization": {"sqlAuthentication": {"credential": {"referenceName": properties.get("credentials", {}).get("referenceName")}}}
    }})
    # Candidate 3: ManagedIdentity (if user provided managed identity info earlier)
    if properties.get("scanAuthorization", {}).get("managedIdentity"):
        candidates.append({**properties, **{
            "scanAuthorizationType": "ManagedIdentity",
            "scanAuthorization": {"managedIdentity": properties["scanAuthorization"]["managedIdentity"]}
        }})

    last_err = None
    for idx, props_try in enumerate(candidates):
        body = {"kind": kind, "properties": props_try}
        try:
            print(f"Attempting to create scan (attempt {idx+1}) with scanAuthorizationType={props_try.get('scanAuthorizationType')}")
            created = call_purview("PUT", endpoint, f"/datasources/{datasource_name}/scans/{scan_name}", token, params=params, body=body)
            print("Scan created/updated:", created.get("name", scan_name))
            return created
        except Exception as e:
            last_err = e
            # If server returned Scan_InvalidType, continue to next candidate
            err_text = str(e)
            if "Scan_InvalidType" in err_text or "Invalid scanAuthorizationType" in err_text:
                print("Server rejected scanAuthorizationType, trying next candidate...")
                continue
            else:
                # For other errors, stop and raise
                raise
    # If we exhausted candidates, raise last error
    raise last_err

def run_scan(endpoint: str, token: str, datasource_name: str, scan_name: str, scan_level: Optional[str] = None) -> Dict[str, Any]:
    path = f"/datasources/{datasource_name}/scans/{scan_name}:run"
    params = {"api-version": API_VERSION}
    if scan_level:
        params["scanLevel"] = scan_level
    return call_purview("POST", endpoint, path, token, params=params, body={})

def get_scan_run_status(endpoint: str, token: str, datasource_name: str, scan_name: str, run_id: str) -> Dict[str, Any]:
    path = f"/datasources/{datasource_name}/scans/{scan_name}/runs/{run_id}"
    params = {"api-version": API_VERSION}
    return call_purview("GET", endpoint, path, token, params=params)

def generate_scan_backup_script(scan_name: str, datasource_name: str, kind: str, properties: Dict[str, Any]):
    os.makedirs(BACKUP_DIR, exist_ok=True)
    filename = os.path.join(BACKUP_DIR, f"{scan_name}-scan.py")
    script = f'''#!/usr/bin/env python3
import json, requests
from azure.identity import ClientSecretCredential
from authenticate import authenticate
creds = authenticate()
def get_token():
    cred = ClientSecretCredential(client_id=creds["client_id"], client_secret=creds["client_secret"], tenant_id=creds["tenant_id"])
    return cred.get_token("https://purview.azure.net/.default").token
def main():
    token = get_token()
    endpoint = f"https://{{creds['purview_account_name']}}.purview.azure.com/scan"
    path = f"/datasources/{datasource_name}/scans/{scan_name}"
    url = endpoint.rstrip("/") + path + "?api-version={API_VERSION}"
    headers = {{"Authorization": f"Bearer {{token}}", "Content-Type": "application/json"}}
    body = {json.dumps({'kind': kind, 'properties': properties}, indent=2)}
    resp = requests.put(url, headers=headers, json=body)
    print("Status:", resp.status_code); print(resp.text)
if __name__ == "__main__": main()
'''
    with open(filename, "w", encoding="utf-8") as f:
        f.write(script)
    print("Backup scan script created:", filename)

# ---------------------------
# Interactive builders
# ---------------------------
def interactive_build_scan_body(endpoint: str, token: str, datasource_type: str) -> Tuple[str, Dict[str, Any]]:
    print(f"Building scan body for datasource type: {datasource_type}")
    kind = input(f"Enter scan kind (e.g., AzureSqlDatabase, AdlsGen2) [default: {datasource_type}]: ").strip() or datasource_type
    auth_choice = input("Authorization method: 1) Credential  2) ManagedIdentity  (enter 1 or 2) [default 1]: ").strip() or "1"
    if auth_choice == "1":
        credential_ref = input("Enter existing Purview credential name (must exist): ").strip()
        if not credential_exists(endpoint, token, credential_ref):
            print("Credential not found in Purview. Create it first.")
            raise RuntimeError("Credential not found.")
        scan_auth_type = "Credential"
        scan_auth = {"credential": {"referenceName": credential_ref}}
    else:
        mi_resource_id = input("Enter user-assigned managed identity resourceId (full ARM id): ").strip()
        if not mi_resource_id:
            raise RuntimeError("Managed identity resourceId required.")
        scan_auth_type = "ManagedIdentity"
        scan_auth = {"managedIdentity": {"resourceId": mi_resource_id}}
    props: Dict[str, Any] = {
        "scanRulesetName": None,
        "scanType": "Full",
        "scanRuleset": {},
        "scanTrigger": {},
        "scanLevel": "Full",
        "scanAuthorizationType": scan_auth_type,
        "scanAuthorization": scan_auth,
        # keep credentials reference too (some shapes expect it)
        "credentials": {"referenceName": scan_auth.get("credential", {}).get("referenceName")} if scan_auth_type == "Credential" else {}
    }
    if "adls" in datasource_type.lower() or "storage" in datasource_type.lower():
        root_path = input("Enter root path to scan (container/folder) or leave blank: ").strip()
        if root_path:
            props["scanRuleset"]["scanPaths"] = [root_path]
    if "sql" in datasource_type.lower():
        db = input("Enter database name or leave blank: ").strip()
        if db:
            props["scanRuleset"]["database"] = db
    return kind, props

# ---------------------------
# Run scans from CSV log
# ---------------------------
def fetch_scan_entries_from_csv(csv_path: str) -> List[Dict[str, str]]:
    if not os.path.exists(csv_path):
        return []
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)

def run_scans_from_log(endpoint: str, token: str):
    entries = fetch_scan_entries_from_csv(CSV_FILE)
    if not entries:
        print("No scan entries found in CSV.")
        return
    superset_fields = ["scan_name", "datasource_name", "scan_type", "scan_schedule", "scan_config", "scan_id", "scan_run_id", "scan_status", "timestamp"]
    for e in entries:
        ds = e.get("datasource_name")
        scan = e.get("scan_name")
        if not ds or not scan:
            continue
        try:
            print(f"Starting run for {ds}/{scan}...")
            resp = run_scan(endpoint, token, ds, scan, e.get("scan_type"))
            run_id = resp.get("runId") if isinstance(resp, dict) else None
            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
            record = {
                "scan_name": scan,
                "datasource_name": ds,
                "scan_type": e.get("scan_type", ""),
                "scan_schedule": e.get("scan_schedule", ""),
                "scan_config": e.get("scan_config", ""),
                "scan_id": e.get("scan_id", ""),
                "scan_run_id": run_id or "",
                "scan_status": "Started",
                "timestamp": timestamp
            }
            append_csv_record(CSV_FILE, record, superset_fields)
            append_json_log(JSON_LOG, record)
            print("Run started, runId:", run_id)
        except Exception as ex:
            print(f"Failed to start run for {ds}/{scan}: {ex}")

# ---------------------------
# Main interactive flow
# ---------------------------
def main():
    endpoint = resolve_endpoint(creds)
    token = get_access_token_for(creds)
    print("Purview Scan Automation (endpoint):", endpoint)
    print("1) Create SqlAuth credential (Key Vault secret)  2) Create Key Vault connection  3) Create/Run scan  4) Run scans from log  5) Exit")
    while True:
        choice = input("Choose action (1/2/3/4/5): ").strip()
        if choice == "1":
            create_sqlauth_credential(endpoint, token)
        elif choice == "2":
            kv_name = input("Key Vault connection name: ").strip()
            base_url = input("Key Vault baseUrl (https://<name>.vault.azure.net/): ").strip()
            desc = input("Description (optional): ").strip()
            try:
                create_or_replace_key_vault_connection(endpoint, token, kv_name, base_url, desc)
                print("Key Vault connection created.")
            except Exception as e:
                print("Failed to create Key Vault connection:", e)
        elif choice == "3":
            datasource_name = input("Enter datasource referenceName (as registered in Purview): ").strip()
            scan_name = input("Enter scan name: ").strip()
            datasource_type = input("Enter scan datasource type (e.g., AzureSqlDatabase, AdlsGen2, AzureStorage): ").strip()
            scan_level = input("Enter scan level (Full/Incremental) or leave blank: ").strip() or None
            schedule = input("Enter schedule (cron or description) or leave blank: ").strip()
            try:
                kind, properties = interactive_build_scan_body(endpoint, token, datasource_type)
            except Exception as e:
                print("Scan body build failed:", e)
                continue
            if schedule:
                properties.setdefault("scanTrigger", {})["schedule"] = schedule
            try:
                scan_resource = ensure_scan_exists_with_retries(endpoint, token, datasource_name, scan_name, kind, properties)
            except Exception as e:
                print("Failed to create/ensure scan:", e)
                continue
            generate_scan_backup_script(scan_name, datasource_name, kind, properties)
            try:
                run_resp = run_scan(endpoint, token, datasource_name, scan_name, scan_level)
                run_id = run_resp.get("runId") if isinstance(run_resp, dict) else None
                timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
                record = {
                    "scan_name": scan_name,
                    "datasource_name": datasource_name,
                    "scan_type": properties.get("scanType", ""),
                    "scan_schedule": schedule,
                    "scan_config": json.dumps({"kind": kind, "properties": properties}),
                    "scan_id": scan_resource.get("name") if isinstance(scan_resource, dict) else getattr(scan_resource, "name", ""),
                    "scan_run_id": run_id or "",
                    "scan_status": "Started",
                    "timestamp": timestamp
                }
                superset_fields = ["scan_name", "datasource_name", "scan_type", "scan_schedule", "scan_config", "scan_id", "scan_run_id", "scan_status", "timestamp"]
                append_csv_record(CSV_FILE, record, superset_fields)
                append_json_log(JSON_LOG, record)
                print("Scan run started. RunId:", run_id)
            except Exception as e:
                print("Failed to start scan run:", e)
                continue
            if run_id:
                print("Polling run status (10 attempts, 15s interval)...")
                for i in range(10):
                    try:
                        status_resp = get_scan_run_status(endpoint, token, datasource_name, scan_name, run_id)
                        final_status = status_resp.get("status") if isinstance(status_resp, dict) else None
                        print(f"Poll {i+1}: status={final_status}")
                        record_update = dict(record)
                        record_update["scan_status"] = final_status or record_update["scan_status"]
                        record_update["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                        append_csv_record(CSV_FILE, record_update, superset_fields)
                        append_json_log(JSON_LOG, {"update": record_update})
                        if final_status and str(final_status).lower() in ("completed", "succeeded", "failed", "cancelled"):
                            print("Scan run finished with status:", final_status)
                            break
                    except Exception as e:
                        print("Polling error:", e)
                    time.sleep(15)
        elif choice == "4":
            run_scans_from_log(endpoint, token)
        elif choice == "5":
            print("Exiting.")
            break
        else:
            print("Invalid choice. Try again.")

if __name__ == "__main__":
    main()
