# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - API to Volume Ingestion
# MAGIC 
# MAGIC **Purpose**: Read JSON from Volume - Write to Bronze Delta table via Autoloader
# MAGIC
# MAGIC **Pattern**: Autoloader (cloudFiles) with schema inference and evolution
# MAGIC
# MAGIC **Cloud**: Azure Databricks (ADLS Gen2 backend for Volumes)

# COMMAND -----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND -----------

from pyspark.sql import functions as F

# Paths
CATALOG = "workspace"
SOURCE_PATH = f"/Volumes/{CATALOG}/raw/chi311_landing"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/bronze/chi311_landing/autoloader"
BRONZE_TABLE = f"{CATALOG}.bronze.bronze_raw_311_requests"

# COMMAND -----------

# MAGIC %md
# MAGIC Autoloader Ingestion

# COMMAND -----------

# Check if table exists to determine initial vs incremental
table_exists = spark.catalog.tableExists(BRONZE_TABLE)
processing_mode = "incremental" if table_exists else "initial"
print(f"Processing mode: {processing_mode}")

# COMMAND -----------

# Read from Volume using Autoloader
df_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema")
    .option("recursiveFileLookup", "true")
    .load(SOURCE_PATH)
)

# Add ingestion metadata
df_enriched = (
    df_raw
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingestion_date", F.current_date())
)

# COMMAND -----------

# Write to Bronze Delta Table
write_query = {
    df_enriched.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/bronze")
    .option("mergeSchema", "true")
    .option("append")
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
}

# Wait for completion
write_query.awaitTermination()
print(f"Bronze load complete: {BRONZE_TABLE}")

# COMMAND -----------

# MAGIC %md
# MAFIC ## Verify

# COMMAND -----------

df_bronze_check = spark.read.table(BRONZE_TABLE)
record_count = df_bronze_check.count()
print(f"Total Records: {record_count:,}")

# Show recent ingestions
print("\nRecent Ingestions:")
display(
    df_bronze_check
    .groupBy(F.date_trunc("hour", "_ingestion_timestamp").alias("ingestion_hour"))
    .count()
    .orderBy(F.desc("ingestion_hour"))
    .limit(10)
)

# COMMAND -----------

display(df_bronze_check.limit(5))

# COMMAND -----------

# Show Delta table history
display(spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE}"))

# COMMAND -----------

# MAGIC %md
# MAGIC **Next Step**:
# MAGIC - Run Lakeflow DLT Pipeline for Silver + Gold transformations
# MAGIC - or run `03_data_quality/01_data_quality_checks.py` for validation