# Bronze Layer - Ingest JSON from ADLS
from pyspark.sql.functions import col, to_date, lower
df_bronze = spark.read.parquet("abfss://folder_name@<container_name>.dfs.core.windows.net/bronze/Vinodmaanju/azure-poc/refs/heads/main/retail_transactions_bronze.parquet")
df_bronze.show()
