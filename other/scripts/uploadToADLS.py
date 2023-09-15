### pip install azure-storage-blob 
### https://learn.microsoft.com/en-us/rest/api/storageservices/blob-service-rest-api

#importing necessary libaries
import os
from azure.storage.blob import BlobServiceClient
import time
import argparse

#to pass storage account key during execution through CLI
parser = argparse.ArgumentParser(description="Access Azure Storage using account key.")
parser.add_argument("--account-key", required=True, help="Azure Storage account key")
args = parser.parse_args()

key = args.account_key

print("Processing...")
time.sleep(2)

#define the Azure storage account configurations
storage_account = 'wetelco'
connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account};AccountKey={key};EndpointSuffix=core.windows.net"
container_name = "wetelcodump/raw/"

#establish connecion with the storage
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

#create container if not exists
container_client = blob_service_client.get_container_client(container_name)

#iterating over the local directory where files are stored
local_folder_path = "../data_dump/"
for filename in os.listdir(local_folder_path):
    local_file_path = os.path.join(local_folder_path, filename)
    blob_client = blob_service_client.get_blob_client(container_name, filename)

    #opening the files to upload in binary mode (rb)
    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(data)

    print(f"Uploaded {filename} to Azure Data Lake Storage.") 

print("Process Complete.")
