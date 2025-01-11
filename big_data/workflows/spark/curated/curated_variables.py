from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from big_data.workflows.spark.common.utils.config_loader import ExecutionContext

spark = SparkSession.builder.getOrCreate()

def transform(ec):
    
    df_variables = ec.config["data"]["dbw_variables_extract"]
    
    df_variables = spark.table("cdv_big_data_adb.gus_dbw.dbw_variables")
        
    df_transform = (df_variables
                    .withColumn("id_dim_1", F.concat(F.col("id_wymiar_1"), F.col("id_pozycja_1")).cast("string"))
                    .withColumn("id_dim_2", F.concat(F.col("id_wymiar_2"), F.col("id_pozycja_2")).cast("string"))
                    .withColumn("id_dim_3", F.concat(F.col("id_wymiar_3"), F.col("id_pozycja_3")).cast("string"))
                    .withColumn("id_dim_4", F.concat(F.col("id_wymiar_4"), F.col("id_pozycja_4")).cast("string"))
                    .withColumn("id_dim_zmienna", F.concat(F.col("id_zmienna"), F.col("id_przekroj"), F.col("id_okres")).cast("string"))
                    .select(
                        F.col("id_zmienna").alias("id_zmienna"),
                        F.col("id_dim_zmienna").alias("id_dim_zmienna"),
                        F.col("id_przekroj").alias("id_przekroj"),
                        F.col("id_dim_1").cast("int").alias("id_dim_1"),
                        F.col("id_dim_2").alias("id_dim_2"),
                        F.col("id_dim_3").cast("int").alias("id_dim_3"),
                        F.col("id_dim_4").cast("int").alias("id_dim_4"),
                        F.col("id_okres").cast("int").alias("id_okres"),
                        F.col("id_sposob_prezentacji_miara").cast("int").alias("id_sposob_prezentacji_miara"),
                        F.col("id_daty").cast("int").alias("id_daty"),
                        F.col("id_brak_wartosci").cast("int").alias("id_brak_wartosci"),
                        F.col("id_tajnosci").cast("int").alias("id_tajnosci"),
                        F.col("id_flaga").cast("int").alias("id_flaga"),
                        F.col("wartosc").cast("long").alias("wartosc"),
                        F.col("wartosc_opisowa").cast("string").alias("wartosc_opisowa"),
                        F.col("precyzja").cast("string").alias("precyzja")                       
                    )
    )
    ec.update_config("df_transform", df_transform)
    print(f"Added df_transform to the config")
