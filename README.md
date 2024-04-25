# MongoDB to Microsoft Fabric Mirroring

This project utilize the L0 Landing Zone of a Generic Mirroring service in Microsoft Fabric, enabling synchronization from MongoDB databases to Fabric.

## Temporary Landing Zone Creation Process

Currently the only way to create such a Generic Mirroring L0 Landing Zone is through a manual method involving Fabric web UI, browser's built-in dev tool, Microsoft Azure Storage Explorer and an API testing tool like Postman or Insomnia.

Following these steps to create a Generic Mirror L0 Landing Zone:
1. Open Fabric in browser, and press F12 to open the browser's developer tool
1. In your Fabric workspace, try to create a `Mirrored Snowflake (preview)`, give it a name and click Create
1. In the browser's developer tool, go to network and find a POST request ends in `generatemwctokenv2`, go to its response and copy everything (should contains a "Token") to a text editor
1. Also in the browser's developer tool, in the console section, find an URL ending with `mountingconfig`, copy the URL
1. Paste the URL to your text editor, replace the ending `mountingconfig` with `upsertmountingconfig`
1. Open an API testing tool (like Postman, Insomnia), create a new `POST` request, and use the altered URL from previous step
1. In the header section, add a new header `x-ms-workload-resource-moniker`, and for the value of it go back to the browser with Fabric open, the URL of the Fabric page should looks like `https://app.fabric.microsoft.com/groups/YOUR_WORKSPACE_ID/YOUR_SERVICE_NAME/ANYTHING_ELSE`. Copy the `YOUR_WORKSPACE_ID` section, and paste it to the value of the header
1. switch to API key section of the testing tool, in the value enter "`Mwctoken `" (notice the ending space), and then copy the "Token" captured from the network dev tool (step 3) and paste it after "`Mwctoken `", and make sure to choose the "add to header" option
1. for the payload choose JSON format and paste the following in (not sure how to get the sourceConnectionId though):
    ```json
    {
        "extendedProperties": {
            "sourceType": "Snowflake",
            "targetStatus": "Running",
            "sourceConnectionId": "b359cd42-87e7-467a-ae5d-da0ffe8a5d8e"
        },
        "replicatorPayload": "{\"properties\":{\"source\":{\"type\":\"GenericMirror\"},\"target\":{\"type\":\"MountedRelationalDatabase\",\"typeProperties\":{\"format\":\"Delta\"}}}}"
    }
    ```
1. send the POST request to create the Landing Zone

## How To Use
The `run_template.py` file contains an example of how you can run this tool. Make sure to provide all required parameters.