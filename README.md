# MongoDB to Microsoft Fabric Mirroring

This project is for the L1 Connector which can replicate MongoDB Atlas Data with Microsoft Fabric One Lake in real time.
It requires two main steps:
1. Creating the Fabric mount point also called Landing Zone or MirrorDB
2. Deploy Code to run in an App service using `Deploy to Azure` button below or to a VM by cloning the Git repo and running the `app.py` script.
One started, app will continously keep track of the changes in MongoDB Atlas and replicate them to the MirrorDB created in Step 1.

# Step1: Fabric Mount Generation
To create the mount for replication, we just need to run the `Fabric Generic mount creation/MirrorDB_creation.py` code.

## Pre-requisites for Step1:
1. Rename `Fabric Generic mount creation/.env_example_mount` to `Fabric Generic mount creation/.env`
2. To get the WORKSPACE_ID, Open the Fabric UI, select the Workspace where you want the Mirror DB to be created. The Workspace ID is part of the url. For e.g : In the url "https://app.fabric.microsoft.com/groups/daacbe4d-4fd8-4cbd-bc0b-b211356b30c5/list?experience=power-bi", `daacbe4d-4fd8-4cbd-bc0b-b211356b30c5` is the Workspace Id.
3. Open Developer Tools, check Network Activity, Select "Disable Cache", get any API (endpoint api or any other api). Copy the Bearer Token from the Request Header. This is also explained in the `Fabric Generic mount creation/Generic Mount Creation Steps.pdf` file.

## Step1 Execution
1. Make sure all environment variables are set in the `.env` file
1. To Start Fabric MirrorDB creation - simply run `Fabric Generic mount creation/MirrorDB_creation.py`

## Output Verification
1. The terminal prints will indicate the execution of 4 APIs. The 2nd API will create the Fabric mount and can verify that in Fabric.
2. After the 4th API is run, we can check in Fabric and see that Replication Status is `Running`. There is a delay set between 3rd API and 4th to give time for the artifact to be created. If it fails with "Bad Request Error", increase the delay and try again.

# Step2: Start Mirroring
Step 2 is basically executing the ARM template by clicking the `Deploy to Azure` button. But, we need to get the parameters to be provided to the ARM template ready beforehand.

## Pre-requisites for Step2:
1. Keep the MongoDB `Connection uri`, `Database name` and `Collection name` handy for input in ARM template.
2. Install Azure Storage explorer. Connect to Azure Storage by selecting `Attach to a resource` -> `ADLS Gen2 container or directory` -> `Sign in using Oauth`. Select your Azure login id and on next screen give the `Blob container or directory URL` as `https://onelake.blob.fabric.microsoft.com/<workspace name in Fabric>`. Once connected you can see the Workspace under `Storage Accounts` -> `(Attached Containers)` -> `Blob Containers`. Double click your Workspace, you should see the MirrorDB folder. In your Mirror DB folder -> `Files` -> `LandingZone` (create if not exists already) -> right click and choose `Copy URL` -> `With DFS Endpoint`

![image](https://github.com/user-attachments/assets/4c2ec669-4164-475a-b56c-b0bd2cadf940)

4. For authentication, its through Service Principal and so we need to go to `App Registrations` in Azure portal and register a new app. Also create a new secret in the App. Get the Tenant Id, App Id and the value of the secret for input in ARM template. the secret value should be copied when being created, you will not be able to see it later.

![image](https://github.com/user-attachments/assets/8cb68999-9784-4f46-bbd3-4eec16e29eae)

![image](https://github.com/user-attachments/assets/61bc393b-e3ed-41cb-bd37-5d366441f19b)

6. Go to Fabric Workspace -> Manage Access -> Type in the new App name -> Select the App and give it `admin` permissions to write to this workspace. Select `Add people or groups` if you donot already have the App in your list.
   
![image](https://github.com/user-attachments/assets/efc6b49a-33be-4257-a9db-d4024847a94a)

Refer to `.env_example` for the parameters needed. 

## Step2 Execution

### Option 1 (on local / Azure VM)
If running on local or a VM, clone this repo, populate all parameters in `.env_example` and rename it to `.env`. Then just run `python app.py`to start the app and thus start replication from MongoDB Atlas collection OneLake MirrorDB's table.

### Option 2 (on Azure App service)

Clicking below button, will take you to Azure portal, give in the values for the parameters along with the App service specific parameters and click Deploy. Once App service is deployed, `Go to the resource` and watch `Log stream` to see if the app had started. After the app service is deployed, it will take 10 - 15 mins for the app to be deployed onto the App service and started.

*Please don't click on the `App url` before the deployment is complete and logs from the App start showing up.*

Click below to start your App service for MongoDB to Fabric replication:

[![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fmongodb-partners%2FMongoDB_Fabric_Mirroring%2Fmain%2FARM_template.json)
