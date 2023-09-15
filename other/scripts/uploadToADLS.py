### pip install azure-storage-blob 

import os
from azure.storage.blob import BlobServiceClient
import time
import argparse

parser = argparse.ArgumentParser(description="Access Azure Storage using account key.")
parser.add_argument("--account-key", required=True, help="Azure Storage account key")
args = parser.parse_args()

key = args.account_key

print("Processing...")
time.sleep(2)

storage_account = 'wetelco'
connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account};AccountKey={key};EndpointSuffix=core.windows.net"
container_name = "wetelcodump/raw/"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

local_folder_path = "./data_dump/"
for filename in os.listdir(local_folder_path):
    if filename.endswith(".csv"):
        local_file_path = os.path.join(local_folder_path, filename)
        blob_name = filename

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        with open(local_file_path, "rb") as data:
            blob_client.upload_blob(data)

        print(f"Uploaded {filename} to Azure Data Lake Storage.") 
