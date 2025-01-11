from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from big_data.workflows.spark.common.utils.config_loader import ExecutionContext

spark = SparkSession.builder.getOrCreate()

def transform(ec):
    
    df_variables = ec.config["data"]["dbw_variables_extract"]
    df_variable_section_position = ec.config["data"]["dbw_variable_section_position"]
    
    join_cond = [
        F.col("var.id_wymiar_4") == F.col("sec.id_wymiar"),
        F.col("var.id_pozycja_4") == F.col("sec.id_pozycja"),
    ]

    df_transform = (df_variable_section_position.alias("sec")
                    .join(df_variables.alias("var"), join_cond, "left")
                    .filter(F.col("var.id_wymiar_4").isNotNull())
                    .select(
                        F.col("var.id_wymiar_4").cast("string").alias("id_wymiar"),
                        F.col("var.id_pozycja_4").cast("string").alias("id_pozycja"),
                        F.col("sec.nazwa_wymiar").cast("string").alias("nazwa_wymiar"),
                        F.col("sec.nazwa_pozycja").cast("string").alias("nazwa_pozycja"),
                    )
                    ).distinct()
    df_transform = df_transform.withColumn("id_dim_4", F.concat(F.col("id_wymiar"), F.col("id_pozycja")).cast("string"))
    ec.update_config("df_transform", df_transform)
    print(f"Added df_transform to the config")
