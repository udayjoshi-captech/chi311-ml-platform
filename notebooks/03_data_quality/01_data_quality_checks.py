# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Data Quality Checks (Great Expectations)
# MAGIC 
# MAGIC **Purpose**: Validate data quality at Bronze and Silver layers
# MAGIC
# MAGIC **Framework**: Great Expectations with PySpark
# MAGIC
# MAGIC **Cloud**: Azure Databricks

# COMMAND -----------

# MAGIC %pip install great-expectations==0.18.8
# MAGIC %restart_python

# COMMAND -----------

import great_expectations as gx
from great_expectations.core.expectations_suite import ExpectationSuite
from great_expectations.expectations.expectation import Expectation
from pyspark.sql import functions as F
from datetime import datetime

# Configuration
CATALOG = dbutils.widgets.get("catalog") if "dbutils" in dir() else "chi311"  # noqa: F821
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account") if "dbutils" in dir() else ""  # noqa: F821
BRONZE_TABLE = f"{CATALOG}.bronze.bronze_raw_311_requests"
DQ_RESULTS_TABLE = f"{CATALOG}.gold.dq_checkpoint_results"

# GE data docs will be written here so results survive across runs
GE_DOCS_ROOT = (
    f"abfss://checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net/great_expectations"
    if STORAGE_ACCOUNT
    else "/tmp/great_expectations"
)

# COMMAND -----------

# MAGIC %md
# MAGIC ## Bronze Layer Expectations

# COMMAND -----------

context = gx.get_context()

# Configure ADLS-backed data docs store so HTML results persist across runs
context.add_store(
    store_name="adls_validations_store",
    store_config={
        "class_name": "ValidationsStore",
        "store_backend": {
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": f"{GE_DOCS_ROOT}/validations",
        },
    },
)
context.add_data_docs_site(
    site_name="adls_site",
    site_config={
        "class_name": "SiteBuilder",
        "store_backend": {
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": f"{GE_DOCS_ROOT}/data_docs",
        },
        "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
    },
)

# Connect to Bronze table
df_bronze = spark.read.table(BRONZE_TABLE)
bronze_ds = context.sources.add_or_update_spark(name="bronze_source").add_dataframe_asset(
    name="bronze_311", dataframe=df_bronze
)
bronze_batch = bronze_ds.build_batch_request()

# COMMAND -----------

# Define Bronze expectations
bronze_suite = ExpectationSuite(name="bronze_311_suite")

# Critical field expectations
bronze_expectations = [
    # Non-null checks
    {"type": "expect_column_to_exist", "kwargs": {"column": "sr_number"}},
    {"type": "expect_column_to_exist", "kwargs": {"column": "sr_type"}},
    {"type": "expect_column_to_exist", "kwargs": {"column": "status"}},
    {"type": "expect_column_to_exist", "kwargs": {"column": "created_date"}},

    # Null rate thresholds
    {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "sr_number", "mostly": 0.99}},
    {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "sr_type", "mostly": 0.99}},
    {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "created_date", "mostly": 0.99}},
    {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "status", "mostly": 0.95}},

    # Value domain checks
    {"type": "expect_column_values_to_be_in_set", "kwargs": {
        "column": "status", "value_set": ["Open", "Completed", "Canceled"], "mostly": 0.90
    }},

    # Uniqueness
     {"type": "expect_column_values_to_be_unique", "kwargs": {"column": "sr_number", "mostly": 0.90}},

    # Row count sanity
     {"type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 1000, "max_value": 5000000}},
]

for exp in expectations:
    bronze_suite.add_expectation(
        gx.expectations.registry.get_expectation_impl(exp["type"])(**exp["kwargs"])
    )

# COMMAND -----------

# Validate Bronze
bronze_validator = context.get_validator(
    batch_request=bronze_batch,
    expectation_suite=bronze_suite
)
bronze_results = bronze_validator.validate()

# COMMAND -----------

# Display results
print("=" * 60)
print("BRONZE LAYER DATA QUALITY RESULTS")
print("=" * 60)
print(f"Success: {bronze_results.success}")
print(f"Evaluated: {bronze_results.statistics['evaluated_expectations']}")
print(f"Passed: {bronze_results.statistics['successful_expectations']}")
print(f"Failed: {bronze_results.statistics['unsuccessful_expectations']}")
print(f"Pass Rate: {bronze_results.statistics['success_percent']:.1f}%")

if not bronze_results.success:
    print("\nFailed Expectations:")
    for result in bronze_results.results:
        if not result.success:
            print(f"{result.expectation_config.type}: {result.expectation_config.kwargs}")

# COMMAND -----------

# MAGIC %md
# MAGIC ## Silver Layer Expectations

# COMMAND -----------

# Check if Silver table exists
SILVER_TABLE = f"{CATALOG}.silver.silver_current_311_requests"

try:
    df_silver = spark.read.table(SILVER_TABLE)
    silver_exists = True
    print(f"Silver table found: {df_silver.count():,} records")
except Exception:
    silver_exists = False
    print("Silver table not yet available. Run Lakeflow pipeline first.")

# COMMAND -----------

if silver_exists:
    silver_ds = context.sources.add_or_update_spark(name="silver_source").add_dataframe_asset(
        name="silver_311", dataframe=df_silver
    )
    silver_batch = silver_ds.build_batch_request()

    silver_suite = ExpectationSuite(name="silver_311_suite")

    silver_expectations = [
    # Null rate thresholds
    {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "sr_number", "mostly": 1.00}},
    {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "sr_type", "mostly": 1.00}},
    {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "created_date", "mostly": 1.00}},
    
    # Value domain checks
    {"type": "expect_column_values_to_be_in_set", "kwargs": {
        "column": "status", "value_set": ["Open", "Completed", "Canceled"], "mostly": 0.99
    }},

    # Value domain checks
    {"type": "expect_column_values_to_be_in_set", "kwargs": {
        "column": "is_info_call", "value_set": [True, False], "mostly": 1.0
    }},
    ]

    for exp in silver_expectations:
        silver_suite.add_expectation(
            gx.expectations.registry.get_expectation_impl(exp["type"])(**exp["kwargs"])
        )

    silver_validator = context.get_validator(
        batch_request=silver_batch,
        expectation_suite=silver_suite
    )
    silver_results = silver_validator.validate()

    print("=" * 60)
    print("SILVER LAYER DATA QUALITY RESULTS")
    print("=" * 60)
    print(f"Success: {silver_results.success}")
    print(f"Pass Rate: {silver_results.statistics['success_percent']:.1f}%")

# COMMAND -----------

# MAGIC %md
# MAGIC ## Persist Checkpoint Results to Delta

# COMMAND -----------

def _build_results_row(layer: str, results) -> dict:
    """Extract summary stats from a GE ValidationResult."""
    stats = results.statistics
    return {
        "run_date": datetime.now().date().isoformat(),
        "layer": layer,
        "success": bool(results.success),
        "expectations_evaluated": int(stats["evaluated_expectations"]),
        "expectations_passed": int(stats["successful_expectations"]),
        "expectations_failed": int(stats["unsuccessful_expectations"]),
        "pass_rate_pct": round(float(stats.get("success_percent", 0.0)), 2),
        "logged_at": datetime.now().isoformat(),
    }


rows = [_build_results_row("bronze", bronze_results)]
if silver_exists:
    rows.append(_build_results_row("silver", silver_results))

df_results = spark.createDataFrame(rows)  # noqa: F821
df_results.write.format("delta").mode("append").saveAsTable(DQ_RESULTS_TABLE)
print(f"DQ results written to {DQ_RESULTS_TABLE}")

# Build / update HTML data docs on ADLS
context.build_data_docs(site_names=["adls_site"])
print(f"Data docs updated at: {GE_DOCS_ROOT}/data_docs")

# COMMAND -----------

# MAGIC %md
# MAGIC **Next Step**: Run Lakeflow pipeline, then ML notebooks

