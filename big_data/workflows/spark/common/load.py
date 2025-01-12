import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from big_data.workflows.spark.common.utils.config_loader import ExecutionContext

class Load_API:
    """
    depreciated
    """
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
            .option("overwriteSchema", "true")
            .option("optimizerWrite", "True")
            .option("autoCompact", "True")
            .partitionBy(["run_date", "snapshot_date"])
            .save(file_path))
        print(f"Data saved to {file_path}")
        
class Load_DataFrame:
    """
    A class that represents the process of loading a DataFrame into a specified location.

    Args:
        ec (object): An object that contains the configuration settings.

    Attributes:
        df_extract_name (str): The name of the DataFrame extract.
        df_extract (DataFrame): The DataFrame extract.
        data (dict): The data configuration settings.
        harmonized (dict): The harmonized configuration settings.
        df_input (DataFrame): The input DataFrame to be loaded.
        source_name (str): The name of the data source.
        table_name (str): The name of the table.
        base_uri (str): The base URI for the location.
        catalog (str): The catalog name.
        format (str): The format of the data.
        full_load (bool): Indicates whether to perform a full load or append to existing data.
        run_time (str): The run time of the process.
        spark (SparkSession): The SparkSession object.

    Methods:
        save_to_adls: Saves the DataFrame to the specified location in ADLS.

    """

    def __init__(self, ec):
        self.df_extract_name = ec.config["extract"][0]["name"]
        self.df_extract = ec.config["data"][self.df_extract_name]
        self.data = ec.config["data"]
        self.harmonized = ec.config
        self.df_input = self.data.get("df_transform", self.df_extract) # for tasks without transform step
        self.source_name = ec.config["load"]["params"]["source_name"]
        self.table_name = ec.config["load"]["params"]["table_name"]
        self.base_uri = ec.config["load"]["params"]["base_uri"]
        self.catalog = ec.config["load"]["params"]["catalog"]
        self.format = ec.config["load"]["params"]["format"]
        self.full_load = ec.config["load"]["params"]["full_load"]
        self.run_time = "2025-01-09" #hardcoded for now
        self.spark = SparkSession.builder.getOrCreate()

        self.full_table_name = f"{self.catalog}.{self.source_name}.{self.table_name}"
        print(self.full_table_name)
        self.full_uri = f"{self.base_uri}{self.source_name}/{self.table_name}/"
        self.save_to_adls = self.save_to_adls()

    def save_to_adls(self):
        """
        Saves the DataFrame to the specified location in ADLS as EXTERNAL table.

        Raises:
            ValueError: If the provided df_input is empty.

        """
        if not self.df_input:
            raise ValueError("The provided df_input is empty.")
        
        writer_kwargs = {"format": self.format,
                        "path": self.full_uri,
                        "overwriteSchema": "true",
                        }
        if self.full_load:
            writer_kwargs["mode"] = "overwrite"
        else:
            writer_kwargs["mode"] = "append"
        
        print(f"Writing kwargs:  {writer_kwargs}")
        
        if "snapshot_date" not in self.df_input.columns:
            snapshot_date = self.run_time
            df_final = (self.df_input
                    .withColumn("snapshot_date", lit(snapshot_date))
                        )
        print(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.source_name} MANAGED LOCATION `{self.base_uri}`")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.source_name} MANAGED LOCATION '{self.base_uri}'")
        # Write DataFrame to ADLS
        df_final = df_final.coalesce(1)
        (df_final.write
            .saveAsTable(self.full_table_name, **writer_kwargs)
        )
        print(f"Table {self.full_table_name} saved to {self.full_uri}")
        
