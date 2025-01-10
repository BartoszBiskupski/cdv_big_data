from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from big_data.workflows.spark.common.utils.config_loader import ExecutionContext

spark = SparkSession.builder.getOrCreate()

def transform(ec):
    
    df_variables = ec.config["data"]["dbw_variables_extract"]
    df_variable_section_position = ec.config["data"]["dbw_variable_section_position"]
    
    join_cond = ["var.id_wymiar_1 == sec.id_wymiar", "var.id_pozycja_1 == sec.id_pozycja"]
    
    df_transform = (df_variables.alias("var").
                    join(df_variable_section_position.alias("sec"), join_cond, "left")
                    .filter(F.col("sec.id_wymiar").isNotNull())
                    .select(
                        F.col("var.id_wymiar_1").alias("id_wymiar"),
                        F.col("var.id_pozycja_1").alias("id_pozycja"),
                        F.col("sec.nazwa_wymiar").alias("nazwa_wymiar"),
                        F.col("sec.nazwa_pozycja").alias("nazwa_pozycja"),
                    )
                    ).distinct()
    df_transform = df_transform.withColumn("id_dim_1", F.concat(F.col("id_wymiar"), F.col("id_pozycja")))
    
    ec.update_config("df_transform", df_transform)
    print(f"Added df_transform to the config")
