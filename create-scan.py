#!/usr/bin/env python3
"""
create-scan.py (normalized endpoint + validation)

- Normalizes purview_account_name from authenticate()
- Validates scanning endpoint before dataplane calls
- Prompts to override if normalization fails
- Implements create/replace credentials, key vault connections, scans, run, logging, backups
"""
import os
import json
import csv
import time
import datetime
from typing import Dict, Any, List, Optional
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
# Endpoint normalization & validation
# ---------------------------
def normalize_account_name(raw: str) -> str:
    """
    Normalize various inputs into the Purview account name (host prefix).
    Accepts:
      - 'purview09api'
      - 'https://purview09api.purview.azure.com'
      - 'purview09.scan.purview.azure.com'
      - 'https://purview09api.scan.purview.azure.com'
    Returns the host prefix like 'purview09' or 'purview09api' depending on input.
    Heuristics:
      - If input contains '.scan.purview.azure.com', strip that and any scheme.
      - If input contains '/', strip scheme and path.
      - If input ends with '-api' or 'api' and that is not desired, user will be prompted to confirm.
    """
    s = raw.strip()
    # remove scheme
    if s.startswith("http://") or s.startswith("https://"):
        s = s.split("://", 1)[1]
    # remove path
    s = s.split("/", 1)[0]
    # if contains .scan.purview.azure.com, strip that suffix
    suffix = ".scan.purview.azure.com"
    if s.endswith(suffix):
        s = s[: -len(suffix)]
    # if user accidentally provided full domain like purview09api.scan..., s becomes 'purview09api'
    # return the remaining host prefix
    return s

def scanning_endpoint_for(account_name: str) -> str:
    return f"https://{account_name}.purview.azure.com"

def validate_scanning_endpoint(endpoint: str, token: str) -> bool:
    """
    Try a lightweight GET to /scan/azureKeyVaults to confirm the dataplane endpoint is reachable.
    Returns True if we get a 200 or 204 or a structured response; False otherwise.
    """
    url = endpoint.rstrip("/") + "/scan/azureKeyVaults"
    params = {"api-version": API_VERSION}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        print("DEBUG: validation GET status:", resp.status_code)
        # treat 200/204 as success; some accounts may return 200 with empty list
        return resp.status_code in (200, 204)
    except Exception as e:
        print("DEBUG: validation GET exception:", repr(e))
        return False

def resolve_and_confirm_endpoint(creds: Dict[str, str]) -> str:
    """
    Normalize account name from creds and validate endpoint.
    If validation fails, prompt user to enter correct account name or full endpoint.
    """
    raw = creds.get("purview_account_name", "")
    candidate = normalize_account_name(raw)
    endpoint = scanning_endpoint_for(candidate)
    token = get_access_token(creds)
    print("Resolved scanning endpoint:", endpoint)
    ok = validate_scanning_endpoint(endpoint, token)
    if ok:
        print("Endpoint validation succeeded.")
        return endpoint
    # prompt user to override
    print("Endpoint validation failed for:", endpoint)
    while True:
        override = input("Enter Purview account name or full scanning endpoint (or press Enter to retry normalization): ").strip()
        if not override:
            # retry normalization with original raw but allow user to edit creds in authenticate if needed
            print("Retrying validation with normalized account name again...")
            ok = validate_scanning_endpoint(endpoint, token)
            if ok:
                return endpoint
            else:
                print("Still failing. Please provide correct account name or full endpoint.")
                continue
        # if user provided full endpoint
        if override.startswith("http://") or override.startswith("https://") or ".scan.purview.azure.com" in override:
            # extract host prefix if possible
            if override.startswith("http://") or override.startswith("https://"):
                host = override.split("://", 1)[1].split("/", 1)[0]
            else:
                host = override.split("/", 1)[0]
            if host.endswith(".scan.purview.azure.com"):
                prefix = host.split(".scan.purview.azure.com", 1)[0]
                endpoint = scanning_endpoint_for(prefix)
            else:
                # user gave some other endpoint; use as-is
                endpoint = override.rstrip("/")
        else:
            # user gave account name prefix
            endpoint = scanning_endpoint_for(normalize_account_name(override))
        print("Trying endpoint:", endpoint)
        token = get_access_token(creds)
        if validate_scanning_endpoint(endpoint, token):
            print("Endpoint validation succeeded.")
            return endpoint
        else:
            print("Validation failed for provided value. Try again or check Purview account name in Azure Portal.")

# ---------------------------
# Auth helper (uses authenticate())
# ---------------------------
def get_access_token(creds_local: Dict[str, str]) -> str:
    credential = ClientSecretCredential(
        client_id=creds_local["client_id"],
        client_secret=creds_local["client_secret"],
        tenant_id=creds_local["tenant_id"]
    )
    token = credential.get_token("https://purview.azure.net/.default")
    return token.token

# ---------------------------
# Centralized HTTP call with debug
# ---------------------------
def call_purview(method: str, endpoint: str, path: str, token: str, params: Dict[str, str] = None, body: Any = None, timeout: int = 120) -> Any:
    url = endpoint.rstrip("/") + path
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
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
# CSV / JSON reconciliation & logging
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
# Credential & Key Vault operations (use validated endpoint)
# ---------------------------
def create_or_replace_credential(endpoint: str, token: str, credential_name: str, credential_body: Dict[str, Any]) -> Dict[str, Any]:
    path = f"/scan/credentials/{credential_name}"
    params = {"api-version": API_VERSION}
    return call_purview("PUT", endpoint, path, token, params=params, body=credential_body)

def create_or_replace_key_vault_connection(endpoint: str, token: str, kv_name: str, kv_body: Dict[str, Any]) -> Dict[str, Any]:
    path = f"/scan/azureKeyVaults/{kv_name}"
    params = {"api-version": API_VERSION}
    return call_purview("PUT", endpoint, path, token, params=params, body=kv_body)

def generate_credential_backup_script(credential_name: str, credential_body: Dict[str, Any]):
    os.makedirs(CRED_BACKUP_DIR, exist_ok=True)
    filename = os.path.join(CRED_BACKUP_DIR, f"{credential_name}-credential.py")
    script = f'''#!/usr/bin/env python3
import json
import requests
from azure.identity import ClientSecretCredential
from authenticate import authenticate

creds = authenticate()
def get_token():
    cred = ClientSecretCredential(client_id=creds["client_id"], client_secret=creds["client_secret"], tenant_id=creds["tenant_id"])
    return cred.get_token("https://purview.azure.net/.default").token

def main():
    token = get_token()
    endpoint = f"https://{{creds['purview_account_name']}}.scan.purview.azure.com"
    path = "/scan/credentials/{credential_name}"
    url = endpoint.rstrip("/") + path + "?api-version={API_VERSION}"
    headers = {{"Authorization": f"Bearer {{token}}", "Content-Type": "application/json"}}
    body = {json.dumps(credential_body, indent=2)}
    resp = requests.put(url, headers=headers, json=body)
    print("Status:", resp.status_code)
    print(resp.text)

if __name__ == "__main__":
    main()
'''
    with open(filename, "w", encoding="utf-8") as f:
        f.write(script)
    print("Credential backup script created:", filename)

# ---------------------------
# Scan lifecycle operations (use validated endpoint)
# ---------------------------
def ensure_scan_exists(endpoint: str, token: str, datasource_name: str, scan_name: str, scan_body: Dict[str, Any]) -> Dict[str, Any]:
    get_path = f"/scan/datasources/{datasource_name}/scans/{scan_name}"
    params = {"api-version": API_VERSION}
    try:
        scan = call_purview("GET", endpoint, get_path, token, params=params)
        print("Scan exists:", scan_name)
        return scan
    except Exception:
        print("Scan not found, creating...")
        created = call_purview("PUT", endpoint, get_path, token, params=params, body=scan_body)
        print("Scan created/updated:", created.get("name", scan_name))
        return created


def run_scan(endpoint: str, token: str, datasource_name: str, scan_name: str, scan_level: Optional[str] = None) -> Dict[str, Any]:
    path = f"/scan/datasources/{datasource_name}/scans/{scan_name}:run"
    params = {"api-version": API_VERSION}
    if scan_level:
        params["scanLevel"] = scan_level
    return call_purview("POST", endpoint, path, token, params=params, body={})

def get_scan_run_status(endpoint: str, token: str, datasource_name: str, scan_name: str, run_id: str) -> Dict[str, Any]:
    path = f"/scan/datasources/{datasource_name}/scans/{scan_name}/runs/{run_id}"
    params = {"api-version": API_VERSION}
    return call_purview("GET", endpoint, path, token, params=params)

def generate_scan_backup_script(scan_name: str, datasource_name: str, scan_body: Dict[str, Any]):
    os.makedirs(BACKUP_DIR, exist_ok=True)
    filename = os.path.join(BACKUP_DIR, f"{scan_name}-scan.py")
    script = f'''#!/usr/bin/env python3
import json
import requests
from azure.identity import ClientSecretCredential
from authenticate import authenticate

creds = authenticate()
def get_token():
    cred = ClientSecretCredential(client_id=creds["client_id"], client_secret=creds["client_secret"], tenant_id=creds["tenant_id"])
    return cred.get_token("https://purview.azure.net/.default").token

def main():
    token = get_token()
    endpoint = f"https://{{creds['purview_account_name']}}.scan.purview.azure.com"
    path = f"/scan/datasources/{datasource_name}/scans/{scan_name}"
    url = endpoint.rstrip("/") + path + "?api-version={API_VERSION}"
    headers = {{"Authorization": f"Bearer {{token}}", "Content-Type": "application/json"}}
    body = {json.dumps(scan_body, indent=2)}
    resp = requests.put(url, headers=headers, json=body)
    print("Status:", resp.status_code)
    print(resp.text)

if __name__ == "__main__":
    main()
'''
    with open(filename, "w", encoding="utf-8") as f:
        f.write(script)
    print("Backup scan script created:", filename)

# ---------------------------
# Interactive builders (use validated endpoint)
# ---------------------------
def interactive_create_credential(endpoint: str, token: str) -> Dict[str, Any]:
    print("Create or replace a Purview credential.")
    name = input("Credential name: ").strip()
    print("Kinds: ServicePrincipal, AccountKey, SqlAuth, ManagedIdentity, BasicAuth, DelegatedAuth")
    kind = input("Choose kind: ").strip()
    properties: Dict[str, Any] = {}
    if kind.lower() in ("serviceprincipal", "service principal"):
        properties["tenantId"] = input("Tenant ID: ").strip()
        properties["servicePrincipalId"] = input("Service principal ID: ").strip()
        secret = input("Service principal secret (leave blank to use Key Vault): ").strip()
        if secret:
            properties["servicePrincipalKey"] = secret
    elif kind.lower() in ("accountkey", "account key"):
        properties["typeProperties"] = {"accountKey": input("Account key (or leave blank to use Key Vault): ").strip()}
    elif kind.lower() in ("sqlauth", "sql auth"):
        properties["typeProperties"] = {"user": input("SQL username: ").strip(), "password": input("SQL password: ").strip()}
    elif kind.lower() in ("managedidentity", "managed identity"):
        properties["typeProperties"] = {"resourceId": input("User-assigned managed identity resourceId (optional): ").strip()}
    else:
        raw = input("Paste credential properties JSON (or leave blank): ").strip()
        if raw:
            try:
                properties = json.loads(raw)
            except Exception:
                properties = {"raw": raw}
    body = {"kind": kind, "properties": properties}
    resp = create_or_replace_credential(endpoint, token, name, body)
    generate_credential_backup_script(name, body)
    record = {
        "credential_name": name,
        "kind": kind,
        "properties": json.dumps(properties),
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    append_csv_record(CRED_CSV, record, ["credential_name", "kind", "properties", "timestamp"])
    append_json_log(CRED_JSON, record)
    print("Credential created:", resp.get("name", name))
    return resp

def get_credential_type(endpoint: str, token: str, credential_name: str) -> Optional[str]:
    path = f"/scan/credentials/{credential_name}"
    params = {"api-version": API_VERSION}
    try:
        resp = call_purview("GET", endpoint, path, token, params=params)
        # Purview returns {"kind": "...", "properties": {...}}
        return resp.get("kind")
    except Exception as e:
        print("Failed to fetch credential type:", e)
        return None

def interactive_create_key_vault_connection(endpoint: str, token: str) -> Dict[str, Any]:
    print("Create or replace an Azure Key Vault connection.")
    kv_name = input("Key Vault connection name: ").strip()
    base_url = input("Key Vault baseUrl (https://<name>.vault.azure.net/): ").strip()
    description = input("Description (optional): ").strip()
    body = {"properties": {"baseUrl": base_url}}
    if description:
        body["properties"]["description"] = description
    try:
        resp = create_or_replace_key_vault_connection(endpoint, token, kv_name, body)
    except Exception as e:
        print("Key Vault creation failed:", e)
        raise
    record = {"kv_name": kv_name, "baseUrl": base_url, "description": description, "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    append_csv_record(CRED_CSV, record, ["kv_name", "baseUrl", "description", "timestamp"])
    append_json_log(os.path.join(os.getcwd(), "keyvaults_log.json"), record)
    print("Key Vault connection created:", resp.get("name", kv_name))
    return resp

def interactive_build_scan_body(endpoint: str, token: str, datasource_type: str) -> Dict[str, Any]:
    print(f"Building scan body for datasource type: {datasource_type}")
    use_cred = input("Use existing Purview credential? (y/n): ").strip().lower()
    credential_ref = None
    if use_cred == "y":
        credential_ref = input("Enter credential name: ").strip()
    else:
        cred_resp = interactive_create_credential(endpoint, token)
        credential_ref = cred_resp.get("name") or input("Enter credential name you created: ").strip()

    # Fetch credentialType from Purview
    credential_type = get_credential_type(endpoint, token, credential_ref) or "SqlAuth"

    props: Dict[str, Any] = {
        "scanRulesetName": None,
        "scanType": "Full",
        "scanRuleset": {},
        "scanTrigger": {},
        "scanLevel": "Full",
        "credential": {
            "credentialType": credential_type,
            "referenceName": credential_ref
        }
    }

    if "adls" in datasource_type.lower() or "storage" in datasource_type.lower():
        root_path = input("Enter root path to scan (container/folder) or leave blank: ").strip()
        if root_path:
            props["scanRuleset"]["scanPaths"] = [root_path]

    if "sql" in datasource_type.lower():
        db = input("Enter database name or leave blank: ").strip()
        if db:
            props["scanRuleset"]["database"] = db

    scan_body = {
        "kind": f"{datasource_type}Credential",
        "properties": props
    }
    return scan_body



# ---------------------------
# Log-driven operations
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
    superset_fields = ["scan_name","datasource_name","scan_type","scan_schedule","scan_config","scan_id","scan_run_id","scan_status","timestamp"]
    for e in entries:
        ds = e.get("datasource_name")
        scan = e.get("scan_name")
        if not ds or not scan:
            continue
        try:
            print(f"Starting run for {ds}/{scan}...")
            resp = run_scan(endpoint, token, ds, scan, e.get("scan_type"))
            run_id = None
            if isinstance(resp, dict):
                run_id = resp.get("runId") or resp.get("id")
            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
            record = {
                "scan_name": scan,
                "datasource_name": ds,
                "scan_type": e.get("scan_type",""),
                "scan_schedule": e.get("scan_schedule",""),
                "scan_config": e.get("scan_config",""),
                "scan_id": e.get("scan_id",""),
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
    # Resolve and validate endpoint
    endpoint = resolve_and_confirm_endpoint(creds)
    token = get_access_token(creds)
    print("Purview Scan Automation")
    print("1) Create credential  2) Create Key Vault connection  3) Create/Run scan  4) Run scans from log  5) Exit")
    while True:
        choice = input("Choose action (1/2/3/4/5): ").strip()
        if choice == "1":
            interactive_create_credential(endpoint, token)
        elif choice == "2":
            try:
                interactive_create_key_vault_connection(endpoint, token)
            except Exception as e:
                print("Key Vault creation failed. See debug output above for details.")
        elif choice == "3":
            datasource_name = input("Enter datasource referenceName (as registered in Purview): ").strip()
            scan_name = input("Enter scan name: ").strip()
            datasource_type = input("Enter datasource type (AdlsGen2, AzureSqlDatabase, AzureStorage, etc.): ").strip()
            scan_level = input("Enter scan level (Full/Incremental) or leave blank: ").strip() or None
            schedule = input("Enter schedule (cron or description) or leave blank: ").strip()
            scan_body = interactive_build_scan_body(endpoint, token, datasource_type)
            if schedule:
                scan_body.setdefault("properties", {}).setdefault("scanTrigger", {})["schedule"] = schedule
            try:
                scan_resource = ensure_scan_exists(endpoint, token, datasource_name, scan_name, scan_body)
            except Exception as e:
                print("Failed to create/ensure scan:", e)
                continue
            generate_scan_backup_script(scan_name, datasource_name, scan_body)
            try:
                run_resp = run_scan(endpoint, token, datasource_name, scan_name, scan_level)
                run_id = None
                if isinstance(run_resp, dict):
                    run_id = run_resp.get("runId") or run_resp.get("id")
                timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
                record = {
                    "scan_name": scan_name,
                    "datasource_name": datasource_name,
                    "scan_type": scan_body.get("properties", {}).get("scanType", ""),
                    "scan_schedule": schedule,
                    "scan_config": json.dumps(scan_body),
                    "scan_id": scan_resource.get("name") if isinstance(scan_resource, dict) else getattr(scan_resource, "name", ""),
                    "scan_run_id": run_id or "",
                    "scan_status": "Started",
                    "timestamp": timestamp
                }
                superset_fields = ["scan_name","datasource_name","scan_type","scan_schedule","scan_config","scan_id","scan_run_id","scan_status","timestamp"]
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
                        final_status = None
                        if isinstance(status_resp, dict):
                            final_status = status_resp.get("status") or status_resp.get("state")
                        else:
                            final_status = getattr(status_resp, "status", None) or getattr(status_resp, "state", None)
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
