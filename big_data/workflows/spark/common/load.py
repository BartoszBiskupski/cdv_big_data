import json
import os
from datetime import datetime
from pyspark.sql import SparkSession

class Load_API:
    def __init__(self, ec):
        self.zone = ec.get_config()["load"]["zone"]
        self.extract_name = ec.get_config()["extract"]["name"]
        self.json_input = ec.get_config()["data"][self.extract_name]
        self.schema = ec.get_config()["load"]["schema"]
        self.table_name = ec.get_config()["load"]["table_name"]
        
        self.spark = SparkSession.builder.getOrCreate()

    def save_to_adls(self):
        
        if not self.json_input:
            raise ValueError("The provided JSON input is empty.")
        
        today = datetime.today().strftime('%Y-%m-%d')
        snapshot_date = ec.get_config()["load"]["snapshot_date"]
        dataset_name = f"{self.schema}.{self.table_name}"
        
        if not dataset_name:
            raise ValueError("The dataset name is missing.")
        
        file_path = f"{self.zone}/run_date={today}/{dataset_name}/snapshot={snapshot_date}/{dataset_name}.json"
        # Convert JSON input to DataFrame
        df = self.spark.read.json(self.spark.sparkContext.parallelize([json.dumps(self.json_input)]))
        
        # Write DataFrame to ADLS
        (df.write
            .mode("overwrite")
            .json(file_path))
        
