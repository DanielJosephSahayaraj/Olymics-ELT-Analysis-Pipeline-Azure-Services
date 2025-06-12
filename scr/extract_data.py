import pandas as pd
import json
from azure.storage.blob import BlobServiceClient
from datetime import datetime

# Load configuration from JSON file
with open("config/azure_config.json", "r") as config_file:
    config = json.load(config_file)
    CONNECTION_STRING = config["connection_string"]
    CONTAINER_NAME = config["container_name"]
    BLOB_NAME = f"raw/olympics_{datetime.now().strftime('%Y%m%d')}.csv"

def extract_olympic_data():
    # Load Olympic data (e.g., from Kaggle or API)
    # Placeholder: Replace with actual data source
    data = [
        {"athlete_id": "123", "name": "Usain Bolt", "bio": "World-class sprinter", "medals": 8, "team": "Jamaica"},
        {"athlete_id": "456", "name": "Simone Biles", "bio": "Inspiring gymnast", "medals": 7, "team": "USA"}
    ]
    df = pd.DataFrame(data)
    return df

def save_to_blob(df):
    # Initialize BlobServiceClient using connection string from config
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
    blob_client.upload_blob(df.to_csv(index=False), overwrite=True)
    print(f"Saved data to {CONTAINER_NAME}/{BLOB_NAME}")

if __name__ == "__main__":
    df = extract_olympic_data()
    save_to_blob(df)