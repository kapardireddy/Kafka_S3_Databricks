import dlt
from pyspark.sql.functions import *

@dlt.table(
    name  = "silver_transactions"
)

def silver_transactions():
    silver_transactions_df = dlt.read_stream("bronze_transactions")
    silver_transactions_df = silver_transactions_df.select("transaction_id", "amount", "currency", "timestamp", "user.user_id")\
        .dropDuplicates(["transaction_id"])
    return silver_transactions_df
