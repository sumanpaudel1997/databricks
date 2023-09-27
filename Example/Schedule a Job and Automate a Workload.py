# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# define variables and file paths
file_path = "/databricks-datasets/structured-streaming/events"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_etl_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"




# COMMAND ----------

# drop table if exists
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# COMMAND ----------

# Configure AutoLoader to Ingest JSON to delta table
(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","json")
 .option("cloudFiles.schemaLocation",checkpoint_path)
 .load(file_path)
 .select("*",col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
 .writeStream
 .option("checkpointLocation",checkpoint_path)
 .trigger(availableNow=True)
 .toTable(table_name)
 )

# COMMAND ----------

df = spark.read.table(table_name)

# COMMAND ----------

display(df)

# COMMAND ----------


