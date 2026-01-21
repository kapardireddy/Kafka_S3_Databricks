import dlt
from pyspark.sql.functions import *

@dlt.table(
    name = "silver_users"
)
def silver_users():
    silver_users_df = dlt.read_stream("bronze_transactions")
    silver_users_df = silver_users_df.select("user.user_id","user.name","user.age","user.email","user.address")\
        .dropDuplicates(["user_id"])
    return silver_users_df
