import dlt
from pyspark.sql.functions import *

@dlt.table(
    name= "gold_user_summary"
    )

def gold_user_summary():
    df = dlt.read("silver_transactions")
    return df.groupBy("user_id").agg(
        count("*").alias("total_transactions"),
        sum("amount").alias("total_spent"),
        avg("amount").alias("avg_transaction_value")
    )