import dlt
from pyspark.sql.functions import *

RAW_PATH = "s3://kapardi-kafka-transactions/transactions/"

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, ArrayType
)

schema = StructType([
    StructField("amount", DoubleType(), True),
    StructField("category", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("items", ArrayType(
        StructType([
            StructField("item_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", LongType(), True)
        ])
    ), True),
    StructField("timestamp", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("user", StructType([
        StructField("address", StringType(), True),
        StructField("age", LongType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("email", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("name", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("user_id", LongType(), True)
    ]), True)
])


@dlt.table(
    name = "bronze_transactions"
)

def bronze_transactions():
    df = spark.readStream.format("cloudfiles")\
        .option("cloudFiles.format", "json")\
            .schema(schema)\
                .load(RAW_PATH)\
                    .withColumn("ingest_ts", current_timestamp())
    
    return df