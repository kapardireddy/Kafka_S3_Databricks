# silver_items =  transaction_id, item[item_id, name, price, qty, category]

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = "silver_items"
)

def silver_items():
    df = dlt.readStream("bronze_transactions")
    #the reson we are exploding the column instead of directly selecting the files is because it will infer the item columns as arrays instead of the actual data types of the column
    silver_items_df = df.withColumn("item", explode("items"))\
                .select(
                        "transaction_id",
                        col("item.item_id").alias("item_id"),
                        col("item.name").alias("name"),
                        col("item.price").alias("price"),
                        col("item.quantity").alias("quantity"),
                        "category"
                        )\
                            .dropDuplicates(["transaction_id", "item_id"])
    return silver_items_df