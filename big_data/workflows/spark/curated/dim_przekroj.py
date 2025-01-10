from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from big_data.workflows.spark.common.utils.config_loader import ExecutionContext

spark = SparkSession.builder.getOrCreate()

def transform(ec):
    
    df_periods = ec.config["data"]["dbw_variable_section_periods_extract"]

  
    df_transform = (df_periods.alias("per")
                    .select(
                        F.col("per.id_przekroj").alias("id_przekroj"),
                        F.col("per.nazwa_przekroj").alias("nazwa_przekroj")
                    )
                    ).distinct()
    
    ec.update_config("df_transform", df_transform)
    print(f"Added df_transform to the config")
