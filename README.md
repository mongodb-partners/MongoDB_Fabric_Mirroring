# MongoDB to Microsoft Fabric Mirroring

This project is for enabling mirroring which can replicate MongoDB Atlas Data with Microsoft Fabric One Lake in near real time.\
It requires two main steps:
1. Creating the Fabric mount point also called Landing Zone or MirrorDB
2. Deploy Code to run in an App service using `Deploy to Azure` button below or to a VM by cloning the Git repo and running the `app.py` script.\
Once started, app will continously keep track of the changes in MongoDB Atlas and replicate them to the MirrorDB created in Step 1.

Follow below steps if you want to create manually using Fabric UI (Step 1) and by one click on "Deploy to Azure" below (Step 2) OR you can use the Terraform template provided in the **"terraform"** folder.\
The Terraform template also provides ability to create a **private connection between the App Service and MongoDB Atlas**.

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
**Also, note that you may have to add “/” at the end of this url when you trigger the mirroring script.**

\
6. You can also verify the LandingZone folder created within the MirrorDB in Azure storage explorer. How to access your LZ in Storage Explorer is detailed in Step2: Start Mirroring -> [Pre-requisites for Step2 -> Point #2](#pre-requisites-for-step2)
<img width="1725" alt="MirrorDB_7" src="https://github.com/user-attachments/assets/4184df8b-02aa-43cd-a364-8983f8082a35" />


# Step2: Start Mirroring
Step 2 is basically executing the ARM template by clicking the `Deploy to Azure` button. But, we need to get the parameters to be provided to the ARM template ready beforehand.

## Pre-requisites for Step2:
1. Keep the MongoDB `Connection uri`, `Database name` and `Collection name` handy for input in ARM template.
Note you can give multiple collections as an array `["col1", "col2"]` or can give `all` for all collections in a Database.
2. Install Azure Storage explorer. Connect to Azure Storage by selecting `Attach to a resource` -> `ADLS Gen2 container or directory` -> `Sign in using Oauth`. Select your Azure login id and on next screen give the `Blob container or directory URL` as `https://onelake.blob.fabric.microsoft.com/<workspace name in Fabric>`. Once connected you can see the Workspace under `Storage Accounts` -> `(Attached Containers)` -> `Blob Containers`. Double click your Workspace, you should see the MirrorDB folder. You should also have a `LandingZone` folder within `Files` folder. You can always check for parquet files in this folder which will get replicated to OneLake and shown as tables in OneLake.
   
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

## Best Practices and Troubleshooting
1. Please note the code actually creates two threads for each collection (one for initial_sync and one for delta_sync) and thus if we have large collections (~10 Million+ records), we should be judicous in selecting the compute size of the App service or VM. As a high level bench mark, a compute of 4 CPUs, 16 GiB of memory might work for 5 such collections with a high throughput of say 1000 records/second. Beyond, that we should really monitor the performance and threads and check the CPU usage.
2. Azure Storage explorer is your point to start the troubleshooting. Use below files that start with an underscore to get vital information. (They are not copied to OneLake as they start with underscore"_"). Also note these are pickle files and you can view them using command "python -mpickle _maxid.pkl” in terminal.      
   a. _max_id file: Will tell you what was the maximum _id field that was captured before initial sync begain. Any _id > this _id from _max_id is coming from real time sync. All records with _id <= this _max_id are copied as part of initial_sync   
   b. _resume_token: Contains the last resume token of the real time change event copied to LZ. Thus, you see this file only if atleast one real time changes parquet file was written in LZ.     
   c. _initial_sync_status: Indicates initial_sync is complete or not. "Y" in this file will indicate that initial_sync is complete.   
   d. _metadata.json: Has the primary key which is always "_id". This file should exist in a replicated folder/ table for mirroring to work.   
   e. _last_id: This is the "_id" value of the last record of the last initial sync batch file written to LZ. This file is deleted when initial sync is completed.   
   f. _internal_schema: This is one of the very first files written and has the schema as of the records in the collection being replicated.  
3. The restartability of the App service/ replication is guaranteed if _resume_token file is present. This is because if initial sync is not completed and we restart the App service, the delta changes that came in the interim were being accumulated in a TEMP parquet files in the App service which will be lost. Thus, as a best practice, if the process fails before initial sync is completed, it is advised to delete all files in the collection folder using Azure Storage Explorer and restrart the process so that it can get the new max _id and start initial_sync. Once initial_sync is completed and _resume_token file is created we can restart without any worries as it will pick up changes from the last resume_token from the change stream.
4. Please note that this solution is based on MongoDB Atlas changestreams to capture the real time changes and sync them to Fabric OneLake. And because [changestreams are not yet supported for Timeseries collections](https://www.mongodb.com/docs/manual/core/timeseries/timeseries-limitations/), the current solution will not work for Timeseries collections.
5. Also, if you are using App service option to host the solution and are observing that App Service is not reflecting the latest code or not starting the code, an observation shared to us by one of the customers was that in App configuration settings - "Always On" needs to be on, and "Session Affinity" needs to be off. Please validate this for yourself and check if it is making the App Service behave nicely :-)
     
