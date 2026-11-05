# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze ingestion with Auto Loader
# MAGIC Reads JSON files incrementally from the landing zone into a Bronze Delta table.

# COMMAND ----------

from pyspark.sql import functions as F

# Adjust to your environment (Unity Catalog volumes shown as example)
LANDING_PATH    = "/Volumes/main/banking/landing/transactions/"
CHECKPOINT_PATH = "/Volumes/main/banking/_checkpoints/bronze_transactions/"
SCHEMA_PATH     = "/Volumes/main/banking/_schemas/bronze_transactions/"
BRONZE_TABLE    = "main.banking.bronze_transactions"

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    # Columns that do not match the schema are stored in _rescued_data
    .option("rescuedDataColumn", "_rescued_data")
    .load(LANDING_PATH)
)

bronze_df = (
    raw_stream
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------

query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)   # process all available files, then stop
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()

# COMMAND ----------

# Quick check: rows that landed in _rescued_data
display(
    spark.table(BRONZE_TABLE)
    .filter(F.col("_rescued_data").isNotNull())
    .limit(20)
)

