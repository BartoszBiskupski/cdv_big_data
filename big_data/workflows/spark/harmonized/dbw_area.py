from big_data.workflows.spark.common.utils.config_loader import ExecutionContext
from pyspark.sql.types import StringType, StructType, StructField, BooleanType
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def transform(ec):
    
    dbw_data = ec.config["data"]["dbw_area_extract"]
    
    schema = StructType([
        StructField("id",StringType(),True),
        StructField("nazwa",StringType(),True),
        StructField("id-nadrzedny-element",StringType(),True),
        StructField("id-poziom",StringType(),True),
        StructField("nazwa-poziom",StringType(),True),
        StructField("czy-zmienne",BooleanType(),True),

    ])
    
    df_input = spark.createDataFrame(dbw_data, schema)

    ec.update_config("df_transform", df_input)
    