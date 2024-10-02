import os
from dotenv import load_dotenv
import requests, json, time


# In this script:
# 1. The `get_host_url` function calls the first API to get the host URL.
# 2. The `create_artifact` function calls the second API, passing the [`capacityId`](command:_github.copilot.openSymbolFromReferences?%5B%22%22%2C%5B%7B%22uri%22%3A%7B%22scheme%22%3A%22file%22%2C%22authority%22%3A%22%22%2C%22path%22%3A%22%2FUsers%2Fdianaannie.jenosh%2FWork%2FFabric%20Generic%20mount%20creation%2FGenericMount.postman_collection.json%22%2C%22query%22%3A%22%22%2C%22fragment%22%3A%22%22%7D%2C%22pos%22%3A%7B%22line%22%3A226%2C%22character%22%3A11%7D%7D%5D%2C%22a9ea234a-026e-4b19-9d3f-056ed17829b4%22%5D "Go to definition") obtained from the first API's response.
# 3. The `main` function orchestrates the calls and prints the responses.

# Load environment variables from .env file
load_dotenv()

# Fetch workspaceId from environment variables
aad_token = os.getenv('AAD_TOKEN')
workspace_id = os.getenv('WORKSPACE_ID')
mirror_db_name = os.getenv('MIRROR_DB_NAME')

print("AAD Token:", aad_token)
print("Workspace ID:", workspace_id)
print("Mirror DB Name:", mirror_db_name)

# Make sure to replace `{{AADToken}}` with your actual token and update the endpoint in the `create_artifact` function with the correct URL.
# Define the base URL and headers
base_url = "https://api.powerbi.com"
headers = {
    "Authorization": f"Bearer {aad_token}",
    "Content-Type": "application/json"
}

# Function to get host URL
def get_host_url():
    url = f"{base_url}/metadata/cluster"
    response = requests.get(url, headers=headers)
#    print("url is:", url)
#    print("Headers is: ", headers) 
    response.raise_for_status() 
# Parse the response JSON to get the backendUrl
    response_json = response.json()
    backend_url = response_json.get('backendUrl')
    return backend_url

# Function to create the mirrorDB artifact
def create_artifact(backend_url):
    url = f"{backend_url}/metadata/workspaces/{workspace_id}/artifacts" 
    payload = { 
        "artifactType": "MountedRelationalDatabase",
        "displayName": mirror_db_name
    } 
    response = requests.post(url, headers=headers, json=payload) 
    response.raise_for_status() 
# Parse the response JSON to get the objectId
    response_json = response.json()
    objectId = response_json.get('objectId')
    return objectId

# Function to create user token
def create_user_token(backend_url, workspace_id):
    url = f"{backend_url}metadata/v201606/generatemwctokenv2" 
    payload = { 
        "workspaceObjectId": workspace_id,
        "workloadType": "DMS"
    } 
    response = requests.post(url, headers=headers, json=payload) 
    response.raise_for_status() 
# Parse the response JSON to get the special user token, tarhet uri host and capacity object id
    response_json = response.json()
    usertoken = response_json.get('Token')
    target_uri_host = response_json.get('TargetUriHost')
    capacity_obj_id = response_json.get('CapacityObjectId')
    return usertoken, target_uri_host, capacity_obj_id

# Function to upsert mount config - enable replication
def upsert_mount_config(usertoken, target_uri_host, capacity_obj_id, objectId, workspace_id):
    url = f"https://{target_uri_host}/webapi/capacities/{capacity_obj_id}/workloads/DMS/DmsService/automatic/datamarts/{objectId}/upsertmountingconfig" 
    
    # Create the replicatorPayload as a JSON string
    replicator_payload = json.dumps({
        'properties': {
            'source': {'type': 'GenericMirror'},
            'target': {'type': 'MountedRelationalDatabase', 'typeProperties': {'format': 'Delta'}}
        }
    })
    payload = {
        'replicatorPayload': replicator_payload,
        'extendedProperties': {
            'targetStatus': 'Running',
            'sourceType': 'GenericMirror'
        }
    }
    headers = {
    "Authorization": f"MwcToken {usertoken}",
    "Content-Type": "application/json",
    "x-ms-workload-resource-moniker": workspace_id
    }     

    print("++++Step 4: Url is: ", url)

    response = requests.post(url, headers=headers, json=payload) 
    response.raise_for_status() 
    return response

def main(): 
# Step 1: Get host URL 
    backend_url = get_host_url() 
    print(">>>>Step 1: Host URL Response:", backend_url)

# Step 2: Create Artifact
    objectId = create_artifact(backend_url)
    print(">>>>Step 2: Create Artifact Response:", objectId)

# Step 3: Create User token
    usertoken, target_uri_host, capacity_obj_id = create_user_token(backend_url, workspace_id)
    print(">>>>Step3: Create User Token Response usertoken, target uri n capacityId:", usertoken, target_uri_host, capacity_obj_id)

# Step 4: Add delay before making the 4th API call
    print(">>>>Waiting for 2 minutes before making the 4th API call...")
    time.sleep(120)  # Delay for 1 minute (60 seconds)

# Step 4: Upsert mount config- start replication
    final_response = upsert_mount_config(usertoken, target_uri_host, capacity_obj_id, objectId, workspace_id)
    print(">>>>Step4: Upsert mount config Response:", final_response)

    print(">>>>Done ! Mirroring Started For:", mirror_db_name)

if __name__ == "__main__":
    main()