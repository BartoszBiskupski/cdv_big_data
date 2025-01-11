from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from big_data.workflows.spark.common.utils.config_loader import ExecutionContext

spark = SparkSession.builder.getOrCreate()

def transform(ec):
    
    df_periods = ec.config["data"]["dbw_periods_dictionary_extract"]

  
    df_transform = (df_periods.alias("per")
                    .select(
                        F.col("per.id_okres").cast("int").alias("id_okres"),
                        F.col("per.symbol").cast("string").alias("symbol"),
                        F.col("per.opis").cast("string").alias("opis"),
                        F.col("per.id_czestotliwosc").cast("int").alias("id_czestotliwosc"),
                        F.col("per.nazwa_czestotliwosc").cast("string").alias("nazwa_czestotliwosc"),
                        F.col("per.id_typ").cast("int").alias("id_typ"),
                        F.col("per.nazwa_typ").cast("string").alias("nazwa_typ")
                    )
                    ).distinct()
    
    ec.update_config("df_transform", df_transform)
    print(f"Added df_transform to the config")
