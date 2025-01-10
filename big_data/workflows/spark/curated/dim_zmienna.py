from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from big_data.workflows.spark.common.utils.config_loader import ExecutionContext

spark = SparkSession.builder.getOrCreate()

def transform(ec):
    
    df_periods = ec.config["data"]["dbw_variable_section_periods_extract"]

  
    df_transform = (df_periods.alias("per")
                    .select(
                        F.col("per.id_zmienna").alias("id_zmienna"),
                        F.col("per.nazwa_zmienna").alias("nazwa_zmienna")
                    )
                    ).distinct()
    
    ec.update_config("df_transform", df_transform)
    print(f"Added df_transform to the config")
