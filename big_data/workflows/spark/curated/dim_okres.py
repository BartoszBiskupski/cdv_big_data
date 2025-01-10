from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from big_data.workflows.spark.common.utils.config_loader import ExecutionContext

spark = SparkSession.builder.getOrCreate()

def transform(ec):
    
    df_periods = ec.config["data"]["dbw_periods_dictionary_extract"]

  
    df_transform = (df_periods.alias("per")
                    .select(
                        F.col("per.id_okres").alias("id_okres"),
                        F.col("per.symbol").alias("symbol"),
                        F.col("per.opis").alias("opis"),
                        F.col("per.id_czestotliwosc").alias("id_czestotliwosc"),
                        F.col("per.nazwa_czestotliwosc").alias("nazwa_czestotliwosc"),
                        F.col("per.id_typ").alias("id_typ"),
                        F.col("per.nazwa_typ").alias("nazwa_typ")
                    )
                    ).distinct()
    
    ec.update_config("df_transform", df_transform)
    print(f"Added df_transform to the config")
