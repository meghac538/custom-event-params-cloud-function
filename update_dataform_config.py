import requests
import base64
import re
from google.cloud import secretmanager, bigquery
from google.auth import default
from google.auth.transport.requests import Request as AuthRequest
from google.api_core.exceptions import NotFound, GoogleAPICallError
from config import (
    PROJECT_ID,
    WRITE_PROJECT_ID,
    TEMP_DATASET,
    TEMP_TABLE,
    REPO,
    FILE_PATH,
    BRANCH,
    COMMIT_MESSAGE,
    REGION,
    REPO_ID,
    RELEASE_ID,
    WORKFLOW_ID
)

def get_github_token():
    print("[INFO] Accessing GitHub token from Secret Manager...")
    client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{PROJECT_ID}/secrets/dataform-github-access-token/versions/latest"
    try:
        response = client.access_secret_version(request={"name": secret_name})
        token = response.payload.data.decode("utf-8").strip()
        print("[SUCCESS] GitHub token retrieved.")
        return token
    except Exception as e:
        raise Exception(f"[ERROR] Failed to access GitHub token from Secret Manager: {e}")

def fetch_missing_event_params():
    print("[INFO] Fetching missing event parameters from BigQuery...")
    client = bigquery.Client(project=WRITE_PROJECT_ID)
    query = f"""
        SELECT DISTINCT field_name, field_type
        FROM `{WRITE_PROJECT_ID}.{TEMP_DATASET}.{TEMP_TABLE}`
        WHERE field_type IS NOT NULL AND UPPER(field_type) != 'UNKNOWN'
    """
    try:
        result = client.query(query).result()
        params = [{"name": row["field_name"], "type": row["field_type"]} for row in result]
        print(f"[SUCCESS] Retrieved {len(params)} missing params.")
        return params
    except NotFound:
        raise Exception(f"[ERROR] Temp table `{TEMP_TABLE}` not found in dataset `{TEMP_DATASET}`.")
    except GoogleAPICallError as api_err:
        raise Exception(f"[ERROR] BigQuery API error: {api_err}")
    except Exception as e:
        raise Exception(f"[ERROR] General exception while fetching missing params: {e}")

def update_config_file_with_new_params():
    token = get_github_token()
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }

    get_url = f"https://api.github.com/repos/{REPO}/contents/{FILE_PATH}?ref={BRANCH}"
    print(f"[INFO] Fetching config.js from GitHub: {get_url}")

    try:
        resp = requests.get(get_url, headers=headers)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise Exception(f"[ERROR] Failed to fetch config.js: {e}")

    file_info = resp.json()
    sha = file_info["sha"]
    content = base64.b64decode(file_info["content"]).decode("utf-8")

    match = re.search(
        r'^(?!\s*//)\s*CUSTO?M_EVENT_PARAMS_ARRAY:\s*\[(.*?)\](\s*,)',
        content,
        re.DOTALL | re.MULTILINE
    )
    if not match:
        raise Exception("[ERROR] CUSTOM_EVENT_PARAMS_ARRAY not found in config.js.")

    existing_array_content = match.group(1)
    param_map = {}
    for entry in re.findall(
        r'\{\s*name:\s*"(.*?)"\s*,\s*type:\s*"(.*?)"(?:\s*,\s*renameTo:\s*"(.*?)")?\s*\}',
        existing_array_content
    ):
        name, p_type, rename_to = entry if len(entry) == 3 else (entry[0], entry[1], None)
        param_map[name] = {"name": name, "type": p_type, "renameTo": rename_to or name}

    new_params = fetch_missing_event_params()
    added_params = []

    # Type mapping from BigQuery to Dataform
    DATAFORM_TYPE_MAPPING = {
        "STRING": "string",
        "INT64": "int", 
        "INTEGER": "int",
        "FLOAT64": "decimal",
        "FLOAT": "decimal",
        "BOOL": "string",
        "BOOLEAN": "string"
    }
    
    for param in new_params:
        p_name, p_type = param["name"], param["type"]
        if not p_name or not p_type or p_type.strip().upper() == "UNKNOWN":
            continue
        if p_name in param_map:
            continue
        
        # Convert BigQuery type to Dataform type
        dataform_type = DATAFORM_TYPE_MAPPING.get(p_type.upper(), "string")
        
        param_map[p_name] = {"name": p_name, "type": dataform_type, "renameTo": p_name}
        added_params.append(param_map[p_name])

    if not added_params:
        print("[INFO] No new parameters to add.")
        return {
            "status": "NO_CHANGE",
            "message": "No updates made. Config is already up-to-date.",
            "new_params_added_count": 0,
            "total_unique_params_in_config": len(param_map)
        }

    sorted_params = sorted(param_map.values(), key=lambda x: x["name"])
    formatted = [
        f'    {{ name: "{p["name"]}", type: "{p["type"]}", renameTo: "{p["renameTo"]}" }}'
        for p in sorted_params
    ]
    new_array_block = f'CUSTOM_EVENT_PARAMS_ARRAY: [\n' + ",\n".join(formatted) + "\n],"

    updated_content, count = re.subn(
        r'^(?!\s*//)\s*CUSTO?M_EVENT_PARAMS_ARRAY:\s*\[.*?\](\s*,)',
        new_array_block,
        content,
        count=1,
        flags=re.DOTALL | re.MULTILINE
    )

    if updated_content == content:
        print("[INFO] Content unchanged after processing.")
        return {
            "status": "NO_CHANGE",
            "message": "Config unchanged. Format remained the same.",
            "new_params_added_count": 0,
            "new_params_added": [],
            "total_unique_params_in_config": len(sorted_params)
        }

    updated_b64 = base64.b64encode(updated_content.encode("utf-8")).decode("utf-8")

    try:
        print("[INFO] Committing updated config.js to GitHub...")
        put_resp = requests.put(get_url, headers=headers, json={
            "message": COMMIT_MESSAGE,
            "content": updated_b64,
            "sha": sha,
            "branch": BRANCH
        })
        put_resp.raise_for_status()
        print("[SUCCESS] GitHub config.js updated.")
    except requests.exceptions.RequestException as e:
        raise Exception(f"[ERROR] GitHub PUT request failed: {e}")

    try:
        print("[INFO] Syncing and invoking Dataform workflow...")
        sync_result = sync_and_execute_dataform()
        print("[SUCCESS] Dataform sync and workflow execution complete.")
    except Exception as sync_error:
        raise Exception(f"[ERROR] Config updated but Dataform sync failed: {sync_error}")

    return {
        "status": "SUCCESS",
        "message": "Config updated successfully and Dataform workflow triggered.",
        "new_params_added_count": len(added_params),
        "new_params_added": [p["name"] for p in added_params],
        "total_unique_params_in_config": len(sorted_params),
        "dataform_sync": sync_result
    }

def sync_and_execute_dataform():
    print("[DEBUG] Starting sync_and_execute_dataform()")

    try:
        creds, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        creds.refresh(AuthRequest())
        token = creds.token

        base_url = f"https://dataform.googleapis.com/v1beta1/projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPO_ID}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        print("[INFO] Invoking Dataform workflow using workflowInvocations API...")
        workflow_payload = {
            "workflowConfig": f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPO_ID}/workflowConfigs/{WORKFLOW_ID}"
        }

        workflow_url = f"{base_url}/workflowInvocations"
        workflow_resp = requests.post(workflow_url, headers=headers, json=workflow_payload)
        print(f"[DEBUG] Workflow invocation status: {workflow_resp.status_code}")
        print(f"[DEBUG] Response: {workflow_resp.text}")

        if workflow_resp.status_code != 200:
            raise Exception(f"[ERROR] Workflow invocation failed: {workflow_resp.status_code} - {workflow_resp.text}")

        return {
            "workflow_invocation_status": workflow_resp.status_code,
            "workflow_invocation_response": workflow_resp.json()
        }

    except Exception as e:
        print(f"[EXCEPTION] {str(e)}")
        raise
