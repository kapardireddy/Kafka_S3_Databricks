import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_daily_metrics",
    comment="Daily revenue and transaction volume"
)
def gold_daily_metrics():
    df = dlt.read("silver_transactions")
    return df.withColumn("date", to_date("timestamp")).groupBy("date").agg(
        count("*").alias("daily_transactions"),
        sum("amount").alias("daily_revenue")
    )
