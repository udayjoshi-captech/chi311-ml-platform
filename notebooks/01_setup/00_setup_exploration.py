# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup & Exploration
# MAGIC
# MAGIC **Purpose**: Create Unity Catalog structure, Volumes and perform initial EDA
# MAGIC
# MAGIC **Cloud**: Azure Databricks
# MAGIC
# MAGIC **Prerequisites**:
# MAGIC - Azure Databricks workspace with Unity Catalog enabled
# MAGIC - Serverless compute or cluster attached

# COMMAND -----------

# MAGIC %md
# MAGIC ## 1. Unity Catalog Setup

# COMMAND -----------

# Configuration
CATALOG_NAME = "workspace"

SCHEMAS = {
    "raw": "landing zone for raw API data",
    "bronze": "Raw ingested data with metadata",
    "silver": "Cleaned, validated, SCD2 history",
    "gold": "Business-ready aggregations and ML features",
    "ml": "ML models, predictions, experiments" 
}

VOLUMES = {
    "raw": {
        "chi311_landing": "Landing zone for Chicago 311 JSON files"
    },
    "bronze": {
        "chi311_checkpoint": "Autoloader checkpoint directory"
    }
}

# COMMAND -----------

# Create schemas
for schema_name, comment in SCHEMAS.items():
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema_name}
        COMMENT '{comment}'
    """)
    print(f"Schema: {CATALOG_NAME}.{schema_name}")

# COMMAND -----------

# Create volumes
for schema_name, volumes in VOLUMES.items():
    for volume_name, comment in volumes.items():
        spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{schema_name}.{volume_name}
        COMMENT '{comment}'
    """)
    print(f"Schema: {CATALOG_NAME}.{schema_name}.{volume_name}")

# COMMAND -----------

# Create subdirectories in landing page volume
landing_path = f"Volumes/{CATALOG_NAME}/raw/chi311_landing"
dbutils.fs.mkdirs(f"{landing_path}/initial")
dbutils.fs.mkdirs(f"{landing_path}/incremental")
print(f"Created initial/ and incremental/ under {landing_path}")

# Create checkpoint directory
checkpoint_path = f"Volumes/{CATALOG_NAME}/bronze/chi311_checkpoint/autoloader"
dbutils.fs.mkdirs(checkpoint_path)
print(f"Created autoloader checkpoint at {checkpoint_path}")

# COMMAND -----------

# MAGIC %md
# MAGIC ## 2. Verify Setup

# COMMAND -----------

# List schemas
print("Schemas in catalog:")
print("-" * 60)
schema_df = spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}")
display(schema_df)

# COMMAND -----------

# List Volumes
for schema_name in VOLUMES.keys():
    print(f"\nVolumes in {schema_name}:")
    print("-" * 60)
    volumes_df = spark.sql(f"SHOW VOLUMES IN {CATALOG_NAME}.{schema_name}")
    display(volumes_df)

# COMMAND -----------

# MAGIC %md
# MAGIC ## 3. Quick Data Exploration (API Sample)

# COMMAND -----------

import requests
import json

API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"
params = {"$limit": 1000, "$order": "created_date DESC"}

response = requests.get(API_URL, params=params)
records = response.json()
print(f"Fetched {len(records)} sample records from API")

# COMMAND -----------

# Convert to Spark DataFrame for exploration
df = spark.createDataFrame(records)
print(f"Schema:")
df.printSchema()

# COMMAND -----------

display(df.limit(10))

# COMMAND -----------

# Status distribution
display(
    df.groupBy("status").count().orderBy("count", ascending=False)
)

# COMMAND -----------

# MAGIC %md
# MAGIC ## Volume Paths Reference
# MAGIC
# MAGIC Use these paths in your notebooks:
# MAGIC
# MAGIC ```python
# MAGIC # Landing zone for raw files
# MAGIC LANDING_PATH = "/Volumes/workspace/raw/chi311_landing"
# MAGIC
# MAGIC # Initial load directory
# MAGIC INITIAL_PATH = f"{LANDING_PATH}/initial"
# MAGIC
# MAGIC # Incremental load directory
# MAGIC INCREMENTAL_PATH = f"{LANDING_PATH}/incremental"
# MAGIC
# MAGIC # Autoloader checkpoint 
# MAGIC CHECKPOINTS_PATH = "/Volumes/workspace/bronze/chi311_checkpoint/autoloader"
# MAGIC ```
# MAGIC
# MAGIC **Next Step**: Run `02_ingestion_01_api_to_volume.py` to fetch data from API