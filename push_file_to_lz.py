import os
from dotenv import load_dotenv
import requests
import json


def push_file_to_lz(
    filepath: str,
    lz_url: str,
    table_name: str,
    lz_app_id: str,
    lz_secret: str,
    lz_tenant_id: str,
):
    access_token = __get_access_token(lz_app_id, lz_secret, lz_tenant_id)
    __patch_file(access_token, filepath, lz_url, table_name)
    # TODO: identify if any other parquet files in the dir, and remove them, leaving only the last one we just pushed


def __get_access_token(app_id,client_secret,directory_id):
    """It will create a access token to access the mail apis"""
    app_id = app_id      #Application Id - on the azure app overview page
    client_secret = client_secret
    directory_id = directory_id
    token_url = "https://login.microsoftonline.com/"+directory_id+"/oauth2/v2.0/token"
    token_data = {
    "grant_type": "client_credentials",
    "client_id": app_id,
    "client_secret": client_secret,
    "scope":"https://storage.azure.com/.default"
    }
    token_headers={
      "Content-Type":"application/x-www-form-urlencoded"
    }
    # print(token_url)
    token_response = requests.post(token_url,data=token_data,headers=token_headers)
    token_response_dict = json.loads(token_response.text)

    # print(token_response.text)

    token = token_response_dict.get("access_token")

    if token == None :
      print("Unable to get access token")
      print(str(token_response_dict))
      raise Exception("Error in getting in access token")
    else:
    #   print("Token is:" + token)
      return token


def __patch_file(access_token, file_path, lz_url, table_name): 
    file_name = os.path.basename(file_path)
    base_url = lz_url + table_name + "/"
    token_url = base_url + file_name + "?resource=file"
    token_headers={
        "Authorization" : "Bearer " + access_token,
        "content-length" : "0"
    }
    print("creating file in lake")

    # Code to create file in lakehouse
    response = requests.put(token_url, data={}, headers=token_headers)
    print(response)
    
    token_url = base_url + file_name + "?position=0&action=append&flush=true"
    token_headers={
        "Authorization" : "Bearer " + access_token,
        "x-ms-file-name": file_name
    }
    print(token_url)
    print("pushing data to file in lake")

    # file_path = file_name
    #Code to push Data to Lakehouse 
    with open(file_path, 'rb') as file:
        file_contents = file.read()
        response = requests.patch(token_url, data=file_contents, headers=token_headers)
    print(response)