from azure.storage.blob import BlobServiceClient, ContainerClient
import pandas as pd
from io import StringIO
import os

# Replace with your Azure connection string
connection_string = "your_connection_string"  # Paste your connection string here

# Container and blob details
input_container_name = "your-input-container"  # Container where CSV is stored
input_blob_name = "Weather_Data.csv"

# NEW container for storing Parquet files
parquet_container_name = "your-parquet-container"  # Create this container in Azure if it doesn't exist
output_blob_name = "Modified_Weather_Data.parquet"

# Define the local path to store the Parquet file temporarily
local_parquet_path = r"C:\Users\Monowar\Documents\Modified_Weather_Data.parquet"
modified_by_value = "Monowar Mukul"  # Modified by value

# Step 1: Connect to the Blob Service
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Input container client to download CSV
input_container_client = blob_service_client.get_container_client(input_container_name)

# Parquet container client to upload Parquet
parquet_container_client = blob_service_client.get_container_client(parquet_container_name)

# Step 2: Download the CSV file from Azure Blob Storage
print("Downloading Weather_Data.csv from Azure Blob Storage...")
input_blob_client = input_container_client.get_blob_client(input_blob_name)
csv_data = input_blob_client.download_blob().content_as_text()

# Step 3: Load the CSV data into a DataFrame
df = pd.read_csv(StringIO(csv_data))
print("Original DataFrame:\n", df.head())

# Step 4: Add a new column 'modified_by'
df['modified_by'] = modified_by_value
print("Modified DataFrame:\n", df.head())

# Step 5: Convert the DataFrame to Parquet format and save it locally
print(f"Saving Parquet file to {local_parquet_path}...")
df.to_parquet(local_parquet_path, index=False)

# Step 6: Upload the Parquet file from the local path to the new container
print(f"Uploading {output_blob_name} to {parquet_container_name}...")

# Ensure the Parquet container exists (create if needed)
if not parquet_container_client.exists():
    parquet_container_client.create_container()

# Upload the Parquet file
with open(local_parquet_path, "rb") as data:
    parquet_container_client.upload_blob(name=output_blob_name, data=data, overwrite=True)

print(f"Successfully uploaded {output_blob_name} to {parquet_container_name}.")
