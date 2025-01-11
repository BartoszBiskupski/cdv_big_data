from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from big_data.workflows.spark.common.utils.config_loader import ExecutionContext

spark = SparkSession.builder.getOrCreate()

def transform(ec):
    
    df_periods = ec.config["data"]["dbw_variable_section_periods_extract"]

  
    df_transform = (df_periods.alias("per")
                    .withColumn("id_dim_zmienna", F.concat(F.col("id_zmienna"), F.col("id_przekroj"), F.col("id_okres")).cast("string"))
                    .select(
                        F.col("id_dim_zmienna").cast("string").alias("id_dim_zmienna"),
                        F.col("per.id_zmienna").cast("string").alias("id_zmienna"),
                        F.col("per.nazwa_zmienna").cast("string").alias("nazwa_zmienna"),
                        F.col("per.id_przekroj").cast("string").alias("id_przekroj"),
                        F.col("per.nazwa_przekroj").cast("string").alias("nazwa_przekroj"),
                        F.col("per.id_okres").cast("string").alias("id_okres"),
                        F.col("per.nazwa_okres").cast("string").alias("nazwa_okres")
                    )
                    ).distinct()
    
    ec.update_config("df_transform", df_transform)
    print(f"Added df_transform to the config")
