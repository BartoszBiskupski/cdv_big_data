from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from big_data.workflows.spark.common.utils.config_loader import ExecutionContext

spark = SparkSession.builder.getOrCreate()

def transform(ec):
    
    df_variables = ec.config["data"]["dbw_variables_extract"]
    
    df_transform = (df_variables
                    .withColumn("id_dim_1", F.concat(F.col("id_wymiar_1"), F.col("id_pozycja_1")))
                    .withColumn("id_dim_2", F.concat(F.col("id_wymiar_2"), F.col("id_pozycja_2")))
                    .withColumn("id_dim_3", F.concat(F.col("id_wymiar_3"), F.col("id_pozycja_3")))
                    .withColumn("id_dim_4", F.concat(F.col("id_wymiar_4"), F.col("id_pozycja_4")))
                    select(
                        F.col("id_zmienna").alias("id_zmienna"),
                        F.col("id_przekroj").alias("id_przekroj"),
                        F.col("id_dim_1").alias("id_dim_1"),
                        F.col("id_dim_2").alias("id_dim_2"),
                        F.col("id_dim_3").alias("id_dim_3"),
                        F.col("id_dim_4").alias("id_dim_4"),
                        F.col("id_okres").alias("id_okres"),
                        F.col("id_sposob_prezentacji_miara").alias("id_sposob_prezentacji_miara"),
                        F.col("id_daty").alias("id_daty"),
                        F.col("id_brak_wartosci").alias("id_brak_wartosci"),
                        F.col("id_tajnosci").alias("id_tajnosci"),
                        F.col("id_flaga").alias("id_flaga"),
                        F.col("wartosc").alias("wartosc"),
                        F.col("wartosc_opisowa").alias("wartosc_opisowa"),
                        F.col("precyzja").alias("precyzja")                       
                    )
    )
    ec.update_config("df_transform", df_transform)
    print(f"Added df_transform to the config")
