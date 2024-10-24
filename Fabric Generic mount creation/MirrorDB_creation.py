import os
from dotenv import load_dotenv
import requests, json, time


# In this script:
# 1. The `get_host_url` function calls the first API to get the host URL.
# 2. The `create_artifact` function calls the second API, passing the host URL to create the mirrorDB artifact.
# 3. The 'create_user_token' function calls the third API, passing the host URL and workspace ID to create the user token.
# 4. The 'upsert_mount_config' function calls the fourth API, passing the user token, target uri host, capacity object id, object id and workspace id to start mirroring.
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

# Check the status of the mirrorDB artifact creation
def check_create_artifact(backend_url):
    url = f"{backend_url}/metadata/workspaces/{workspace_id}/artifacts" 
    response = requests.get(url, headers=headers) 
    response.raise_for_status() 
    response_json = response.json()

    provision_status = None
# Loop through the response JSON to find the mirrorDB artifact and extract the provisioningStatus    
    if isinstance(response_json, list):
       for item in response_json:
            display_name = item.get('displayName')
            print(f"Display name of mirroDB is: {display_name}")
            if display_name == mirror_db_name:
                print("Found the mirrorDB artifact")
                extended_properties = item.get('extendedProperties', {})
                print(f">>>extended properties>>>: {extended_properties}")
                if extended_properties is not None:
                    warehouse_properties_str = extended_properties.get('WarehouseProperties', '{}')
                    print(f">>>warehouse properties_str>>>: {warehouse_properties_str}")
                # Parse the WarehouseProperties JSON string into a dictionary
                    warehouse_properties = json.loads(warehouse_properties_str)
                    print(f">>>warehouse properties_dict>>>: {warehouse_properties}")
                # Get the provisioningStatus from the parsed dictionary
                    provision_status = warehouse_properties.get('provisioningStatus')
                    print(f">>>Provision Status>>>: {provision_status}")
                    break  # Exit the loop once the artifact is found
                else:
                    print("extendedProperties is None for item:", item)
    else:
        print("Response JSON is not a list.")
    return provision_status

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
            'source': {'type': 'GenericMirror','typeProperties': {}},
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

# Step 2: Check Create Artifact Status
    counter = 0
    provision_status = None
    while provision_status != "success" and counter < 10:
      provision_status = check_create_artifact(backend_url)
      print(">>>>Step 2: Status of Create Artifact Response is:", provision_status)
      print(">>>>Step 2: Counter of Create Artifact status check is:", counter)
      counter += 1
      time.sleep(10)  # Delay for 10 seconds

    if provision_status == "success":
      print("Artifact creation successful.")
    else:
      print("Artifact creation failed after 10 attempts.")   

# Step 3: Create User token
    if provision_status == "success":
      usertoken, target_uri_host, capacity_obj_id = create_user_token(backend_url, workspace_id)
      print(">>>>Step3: Create User Token Response usertoken, target uri n capacityId:", usertoken, target_uri_host, capacity_obj_id)

# Step 4: Upsert mount config- start replication
    if provision_status == "success":
      final_response = upsert_mount_config(usertoken, target_uri_host, capacity_obj_id, objectId, workspace_id)
      print(">>>>Step4: Upsert mount config Response:", final_response)
      print(">>>>Done ! Mirroring Started For:", mirror_db_name)

if __name__ == "__main__":
    main()