# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - API to Volume Ingestion
# MAGIC 
# MAGIC **Purpose**: Fetch data from Chicago 311 API and save as JSON files in Volume
# MAGIC
# MAGIC **Pattern**: API - JSON File - Volume (Landing Zone)
# MAGIC
# MAGIC **Schedule**: Daily (fetches previous day's data)
# MAGIC
# MAGIC **Source**: https://data.cityofchicago.org/resource/v6vf-nfxy.json

# COMMAND -----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND -----------

import requests
import json
from datetime import datetime, timedelta
from pyspark.sql import functions as F

# API Configuration
API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"
API_LIMIT = 50000 # Max records per request

# Volume paths
CATALOG = "workspace"
LANDING_PATH = f"/Volumes/{CATALOG}/raw/chi311_landing"
INITIAL_PATH = f"{LANDING_PATH}/initial"
INCREMENTAL_PATH = f"{LANDING_PATH}/incremental"

# COMMAND -----------

# MAGIC %md
# MAGIC ## Ingestion Functions

# COMMAND -----------

def fetch_311_data(start_date: str, end_date: str, offset: int=0) -> list:
    """fetch Chicago 311 data from Socrata API with pagination"""
    params = {
        "$where": f"created_date >= '{start_date}' AND created_date < {end_date}'",
        "$limit": API_LIMIT,
        "$offset": offset,
        "$order": "created_date ASC"
    }

    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    return response.json()

def fetch_all_pages(start_date: str, end_date: str) -> list:
    """Fetch all pages of data for a date range"""
    all_records = []
    offset = 0

    while True:
        batch = fetch_311_data(start_date, end_date, offset)
        if not batch:
            break
        all_records.extend(batch)
        print(f"  Fetched {len(batch)} records (total: {len(all_records)})")

        if len(batch) < API_LIMIT:
            break
        offset += API_LIMIT

    return all_records

def save_to_volume(records: list, path:str, filename: str) -> str:
    """Save records as JSON file in Databricks Volume."""
    filepath = f"{path}/{filename}"

    # Write JSON to dbutils
    json_str = json.dumps(records, indent=None)
    dbutils.fs.put(filepath, json_str, overwrite=True)

    print(f"Saved {len(records):,} records to {filepath}")

# COMMAND -----------

# MAGIC %md
# MAGIC ## Run Ingestion

# COMMAND -----------

# Determine mode: initial vs incremental
try:
    existing_files = dbutils.fs.ls(INITIAL_PATH)
    has_initial = any(f.name.endswith(".json") for f in existing_files)
except Exception:
    has_initial = False

if not has_initial:
    # Initial load: fetch 90 days
    processing_mode = "initial"
    end_date = datetime.now().strftime("Y-%m-%dT00:00:00")
    start_date = (datetime.now() - timedelta(days=90)).strftime("Y-%m-%dT00:00:00")
    target_path = INITIAL_PATH
    print(f"INITIAL LOAD: {start_date} to {end_date}")
else:
    # Incremental fetch yesterday's data
    processing_mode = "incremental"
    yesterday = datetime.now() - timedelta(days=1)
    start_date = yesterday.strftime("Y-%m-%dT00:00:00")
    end_date = datetime.now().strftime("Y-%m-%dT00:00:00")
    target_path = INCREMENTAL_PATH
    print(f"INCREMENTAL LOAD: {start_date} to {end_date}")

# COMMAND -----------

# Fetch data
records = fetch_all_pages(start_date, end_date)
print(f"\nTotal records fetched: {len(records):,}")

# COMMAND -----------

# Save to Volume
if records:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"chi311_{processing_mode}_{timestamp}.json"
    save_to_volume(records, target_path, filename)
else:
    print("No records fetched for the date range")

# COMMAND -----------

# MAGIC %md
# MAGIC ## Verify Ingestion

# COMMAND -----------

# List files in landing zone
print(f"\nFiles in {target_path}:")
for f in dbutils.fs.ls(target_path):
    size_mb = f.size / (1024 * 1024)
    print(f"  {f.name} ({size_mb: .2f} MB)")

# COMMAND -----------

# MAGIC %md
# MAGIC **Next Steps**:
# MAGIC - Run `02_bronze_autoloader.py` to load JSON - Bronze Delta table
# MAGIC - Or configure Lakeflow pipeline to read from Volume automatically

