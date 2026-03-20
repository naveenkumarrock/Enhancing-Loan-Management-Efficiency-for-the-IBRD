import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import pandas as pd
from utils.logging import setup_logger

logger = setup_logger("Bronze")

# ==========================================================
# 2. Load Environment Variables
# ==========================================================
load_dotenv()

connection_string = os.getenv("CONNECTION_STRING")
container_name = os.getenv("CONTAINER_NAME")

# ==========================================================
# 3. Connect to Azure Data Lake
# ==========================================================
try:
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    logger.info("Connected to Azure Data Lake")
except Exception as e:
    logger.error(f"Connection failed: {e}")
    raise

# ==========================================================
# 4. Files to Upload
# ==========================================================
files_to_upload = [
    "data/batch_data/ibrd_batchdata.csv"
]

# ==========================================================
# 5. Upload Process
# ==========================================================
for file_path in files_to_upload:
    try:
        logger.info(f"Processing file: {file_path}")

        if os.path.exists(file_path):

            file_name = os.path.basename(file_path)
            blob_path = f"bronze/{file_name}"

            blob_client = blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )

            # Upload file
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            # Count rows
            df = pd.read_csv(file_path)
            row_count = len(df)

            logger.info(f"{file_name} uploaded successfully with {row_count} rows")

        else:
            logger.error(f"File not found: {file_path}")

    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")

# ==========================================================
# 6. Completion
# ==========================================================
logger.info("Upload completed successfully")