import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_product_summary",
    comment="Product-level sales insights"
)
def gold_product_summary():
    df = dlt.read("silver_items")
    return df.groupBy("item_id", "name").agg(
        count("*").alias("times_purchased"),
        sum("price").alias("total_revenue")
    )
