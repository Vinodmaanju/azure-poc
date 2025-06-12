
-- gold layer

# Gold Layer - Aggregates
from pyspark.sql.functions import sum, count, col

df_silver = spark.read.parquet("abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/silver/")

df_silver.show()

df_daily_revenue = (
    df_silver.groupBy("event_date")
    .agg(sum("amount").alias("daily_revenue"), count("*").alias("total_purchases"))
)

df_daily_revenue.write.mode("overwrite").parquet("abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/gold/")


