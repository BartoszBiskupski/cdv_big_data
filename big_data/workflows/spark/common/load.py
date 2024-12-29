import json
import os
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient

class Load_API:
    def __init__(self, connection_string, container_name):
        self.connection_string = connection_string
        self.container_name = container_name
        self.service_client = DataLakeServiceClient.from_connection_string(self.connection_string)
        self.file_system_client = self.service_client.get_file_system_client(self.container_name)

    def save_to_adls(self, parameters):
        json_input = parameters.get("json_input")
        
        if not json_input:
            raise ValueError("The provided JSON input is empty.")
        
        today = datetime.today().strftime('%Y-%m-%d')
        snapshot_date = parameters.get("snapshot_date", today)
        dataset_name = parameters.get("dataset_name", None)
        
        if not dataset_name:
            raise ValueError("The dataset name is missing.")
        
        file_path = f"mnt/landing/run_date={today}/{dataset_name}/snapshot={snapshot_date}/{dataset_name}.json"
        
        file_client = self.file_system_client.get_file_client(file_path)
        
        # Convert JSON to string
        json_str = json.dumps(json_input)
        
        # Overwrite if the file already exists
        file_client.upload_data(json_str, overwrite=True)
        print(f"Data successfully saved to {file_path}")
