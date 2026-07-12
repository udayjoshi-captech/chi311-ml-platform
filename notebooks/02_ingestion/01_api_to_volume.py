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
import os
from datetime import datetime, timedelta
from pyspark.sql import functions as F

# API Configuration
API_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"
API_LIMIT = 50000 # Max records per request

# Initial load window in days (default: 2 years). Override via INITIAL_LOAD_DAYS.
# Kept as a self-contained constant so the notebook needs no packaged deps.
INITIAL_LOAD_DAYS = int(os.getenv("INITIAL_LOAD_DAYS", "730"))
CATALOG = os.getenv("CHI311_CATALOG", "chi311")

# Volume paths
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
        "$where": f"created_date >= '{start_date}' AND created_date < '{end_date}'",
        "$limit": API_LIMIT,
        "$offset": offset,
        "$order": "created_date ASC"
    }

    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    return response.json()

def fetch_and_save_pages(start_date: str, end_date: str, path: str,
                         mode: str, run_ts: str) -> int:
    """Fetch all pages and write each page to its own JSON file.

    Streaming approach: each 50k-record page is written to the Volume
    immediately and then released, so driver memory stays constant
    regardless of total record count (avoids OOM on multi-year loads).
    Autoloader / Bronze reads every JSON file in the directory, so
    splitting across page files is fully compatible downstream.

    Returns the total number of records fetched and written.
    """
    offset = 0
    page = 0
    total = 0

    while True:
        batch = fetch_311_data(start_date, end_date, offset)
        if not batch:
            break

        # Write this page to its own file, then let it be garbage collected
        filename = f"chi311_{mode}_{run_ts}_p{page:04d}.json"
        filepath = f"{path}/{filename}"
        dbutils.fs.put(filepath, json.dumps(batch, indent=None), overwrite=True)

        total += len(batch)
        print(f"  Page {page}: wrote {len(batch)} records to {filename} (total: {total:,})")

        page += 1
        if len(batch) < API_LIMIT:
            break
        offset += API_LIMIT

    return total

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
    # Initial load: fetch INITIAL_LOAD_DAYS from config (default: 2 years / 730 days)
    processing_mode = "initial"
    end_date = datetime.now().strftime("%Y-%m-%dT00:00:00")
    start_date = (datetime.now() - timedelta(days=INITIAL_LOAD_DAYS)).strftime("%Y-%m-%dT00:00:00")
    target_path = INITIAL_PATH
    print(f"INITIAL LOAD: {start_date} to {end_date} ({INITIAL_LOAD_DAYS} days)")
else:
    # Incremental fetch yesterday's data
    processing_mode = "incremental"
    yesterday = datetime.now() - timedelta(days=1)
    start_date = yesterday.strftime("%Y-%m-%dT00:00:00")
    end_date = datetime.now().strftime("%Y-%m-%dT00:00:00")
    target_path = INCREMENTAL_PATH
    print(f"INCREMENTAL LOAD: {start_date} to {end_date}")

# COMMAND -----------

# Fetch and save data page-by-page (constant memory — safe for multi-year loads)
run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
total_records = fetch_and_save_pages(
    start_date, end_date, target_path, processing_mode, run_ts
)
print(f"\nTotal records fetched and written: {total_records:,}")

if total_records == 0:
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

