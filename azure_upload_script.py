#!/usr/bin/env python
# coding: utf-8

# In[2]:


import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# === Azure Storage Credentials ===
account_name = "salesdata2025advika"
access_key = "your-azure-storage-account-access-key-here"
container_name = "salesdata"
file_path = "sales_data.csv"  # Make sure this file is in the same folder as this script
blob_name = "sales_data.csv"  # This is the name it will have in the Azure container

# === Create BlobServiceClient ===
connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# === Get ContainerClient ===
container_client = blob_service_client.get_container_client(container_name)

# === Upload file ===
try:
    with open(file_path, "rb") as data:
        container_client.upload_blob(name=blob_name, data=data, overwrite=True)
    print(f"✅ Upload successful: '{file_path}' uploaded as '{blob_name}' to container '{container_name}'.")
except Exception as e:
    print(f"❌ Upload failed: {e}")

