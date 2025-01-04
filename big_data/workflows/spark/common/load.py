import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from big_data.workflows.spark.common.utils.config_loader import ExecutionContext

class Load_API:
    def __init__(self, ec):
        self.zone = ec.config["load"]["params"]["zone"]
        self.df_input = ec.config["data"]["df_transform"]
        self.schema = ec.config["load"]["schema"]
        self.table_name = ec.config["load"]["table_name"]
        self.spark = SparkSession.builder.getOrCreate()
        self.save_to_adls = self.save_to_adls()

        

    def save_to_adls(self):
        
        if not self.df_input:
            raise ValueError("The provided JSON input is empty.")
        
        today = datetime.today().strftime('%Y-%m-%d')
        dataset_name = f"{self.schema}_{self.table_name}"
        
        if not dataset_name:
            raise ValueError("The dataset name is missing.")
        
        file_path = f"{self.zone}/{dataset_name}/"
        df_final = (self.df_input
                    .withColumn("run_date", lit(today))
                    )
        if "snapshot_date" not in self.df_input.columns:
            snapshot_date = today
            df_final = (df_final
                    .withColumn("snapshot_date", lit(snapshot_date))
                        )
        # Write DataFrame to ADLS
        (df_final.write
            .format("parquet")
            .mode("overwrite")
            .option("optimizerWrite", "True")
            .option("autoCompact", "True")
            .partitionBy(["run_date", "snapshot_date"])
            .save(file_path))
        print(f"Data saved to {file_path}")
        
