from azure.storage.filedatalake import DataLakeServiceClient

def mount_containers(storage_account_name, storage_account_key, containers):
    try:
        service_client = DataLakeServiceClient(account_url=f"https://{storage_account_name}.dfs.core.windows.net", credential=storage_account_key)
        
        for container in containers:
            file_system_client = service_client.get_file_system_client(container)
            file_system_client.create_file_system()

        print("Containers mounted successfully!")
    except Exception as e:
        print(f"Error mounting containers: {str(e)}")

# Example usage
storage_account_name = "your_storage_account_name"
storage_account_key = "your_storage_account_key"
containers = ["container1", "container2", "container3"]

mount_containers(storage_account_name, storage_account_key, containers)