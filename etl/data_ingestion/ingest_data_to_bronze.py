import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Load environment variables
load_dotenv()

# Read values from .env
connection_string = os.getenv("CONNECTION_STRING")
container_name = os.getenv("CONTAINER_NAME")

# Connect to Azure Data Lake
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

print("Connected to Azure Data Lake")

# Local dataset paths
files_to_upload = [
    "data/batch_data/ibrd_batchdata.csv",
    "data/batch_data/ibrd_country_wise_loan_summary.csv"
]

for file_path in files_to_upload:

    if os.path.exists(file_path):

        file_name = os.path.basename(file_path)

        blob_path = f"bronze/{file_name}"

        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_path
        )

        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        print(f"{file_name} uploaded successfully")

    else:
        print(f"File not found: {file_path}")

print("Upload completed successfully")