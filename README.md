# MongoDB to Microsoft Fabric Mirroring

This project is for enabling mirroring which can replicate MongoDB Atlas Data with Microsoft Fabric One Lake in near real time.\
It requires two main steps:
1. Creating the Fabric mount point also called Landing Zone or MirrorDB
2. Deploy Code to run in an App service using `Deploy to Azure` button below or to a VM by cloning the Git repo and running the `app.py` script.\
Once started, app will continously keep track of the changes in MongoDB Atlas and replicate them to the MirrorDB created in Step 1.

# Step1: MirrorDB Creation
To create the MirrorDB, we use the Fabric UI.

Follow the steps below for the MirrorDB (LZ) creation.
1. Click on the “+ New Item” in your workspace.
<img width="1726" alt="MirrorDB_1" src="https://github.com/user-attachments/assets/4c3719d4-30de-44f8-8aba-1088be22eda2" />
 
\
2. In the pop up window that opens, select “Mirrored Database (preview)”
<img width="1726" alt="MirrorDB_2" src="https://github.com/user-attachments/assets/1af1fcdd-b9bb-47f1-848d-3362897d8db0" />

\
3. Give a name for your new MirrorDB.
<img width="752" alt="MirrorDB_3" src="https://github.com/user-attachments/assets/90efb673-22a7-415f-bd6a-e65a6b856edc" />

\
4. The new MirrorDB creation will begin and the screen shows the progress of the same.
<img width="602" alt="MirrorDB_5" src="https://github.com/user-attachments/assets/c2a00531-a058-408f-b98e-bcb294f6694f" />   

5. The new MirrorDB creation is complete when you see the below screen showing that replication is “Running”. Note that you can get the LandingZone url also from this screen.
<img width="1722" alt="MirrorDB_6" src="https://github.com/user-attachments/assets/a0accf46-7fd1-439f-b389-692043523f77" />
Also, note that you may have to add “/” at the end of this url when you trigger the mirroring script.

\
6. You can also verify the LandingZone folder created within the MirrorDB in Azure storage explorer.
<img width="1725" alt="MirrorDB_7" src="https://github.com/user-attachments/assets/4184df8b-02aa-43cd-a364-8983f8082a35" />


# Step2: Start Mirroring
Step 2 is basically executing the ARM template by clicking the `Deploy to Azure` button. But, we need to get the parameters to be provided to the ARM template ready beforehand.

## Pre-requisites for Step2:
1. Keep the MongoDB `Connection uri`, `Database name` and `Collection name` handy for input in ARM template.
Note you can give multiple collections as an array `[col1, col2]` or can give `all` for all collections in a Database.
2. Install Azure Storage explorer. Connect to Azure Storage by selecting `Attach to a resource` -> `ADLS Gen2 container or directory` -> `Sign in using Oauth`. Select your Azure login id and on next screen give the `Blob container or directory URL` as `https://onelake.blob.fabric.microsoft.com/<workspace name in Fabric>`. Once connected you can see the Workspace under `Storage Accounts` -> `(Attached Containers)` -> `Blob Containers`. Double click your Workspace, you should see the MirrorDB folder. In your MirrorDB folder create a new folder called `LandingZone` within `Files` folder. Then right click `LandingZone` and choose `Copy URL` -> `With DFS Endpoint`

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
