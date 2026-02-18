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
CATALOG = "workspace"
BRONZE_TABLE = f"{CATALOG}.bronze.bronze_raw_311_requests"

# COMMAND -----------

# MAGIC %md
# MAGIC ## Bronze Layer Expectations

# COMMAND -----------

context = gx.get_context()

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
# MAGIC **Next Step**: Run Lakeflow pipeline, then ML notebooks

