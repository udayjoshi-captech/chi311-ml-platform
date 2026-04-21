-- =========================================================================
-- Chicago 311 Intelligence Platform - Lakeflow Pipeline with SCD Type 2
-- =========================================================================
--
-- Architecture:
--      Bronze (Autoloader) -> Silver (DLT + SCD2) -> Gold (DLT Aggregates)
--
-- Note: Bronze ingestion is handled by Autoloader notebooker (02_bronze_autoloader.py)
--       This pipeline handles Silver and Gold transformations
--
-- Cloud: Azure Databricks
-- Storage: ADLS Gen 2 / Unity Catalog Volumes
--
-- Unity Catalog Structure:
--  workspace.bronze.bronze_raw_311_requests (source from Autoloader)
--  workspace.silver.silver_*                (this pipeline)
--  workspace.gold.gold_*                    (this pipeline)
--
-- Chicago 311 API: https://data.cityofchicago.org/resource/v6vf-nfxy.json
--
-- Key Finding from Exploration:
--      - Status values: Open, Completed, Canceled 
--      - Info calls: 40% of volume ("311 INFORMATION ONLY CALL")
--      - Daily service requests: ~3,000 (excluding info calls)
--      - Anomaly threshold: 4,851 (mean + 2SD)
--      - Weekend drop: 35-40%
--      - Ward 28: 39% of requests (info call administrative ward)
--
--  To run this pipeline:
--  1. Go to Workflows -> Lakeflow Declarative Pipelines
--  2. Create new pipeline
--  3. Point to this file
--  4. Set Target catalog: workspace, Target schema: silver
--  5. Run in Development mode first
-- =============================================================================

-- =============================================================================
-- SILVER LAYER: Staging View (Clean + Validate)
-- =============================================================================

CREATE STREAMING LIVE VIEW silver_staged_311_requests
COMMENT "Staged and cleaned 311 requests for SCD2 processing"
AS 
SELECT
    -- Primary key
    TRIM(CAST(sr_number AS STRING)) AS sr_number,

    -- Core fields (trim whitespace, normalize status casing)
    TRIM(CAST(sr_type AS STRING)) AS sr_type,
    TRIM(CAST(sr_short_code AS STRING)) AS sr_short_code,
    INITCAP(TRIM(CAST(status AS STRING))) AS status,

    -- Location (trim all string fields)
    TRIM(CAST(street_address AS STRING)) AS street_address,
    TRIM(CAST(city AS STRING)) AS city,
    TRIM(CAST(state AS STRING)) AS state,
    TRIM(CAST(zip_code AS STRING)) AS zip_code,
    CAST(ward AS INT) AS ward,
    CAST(community_area AS INT) AS community_area,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,

    -- Dates
    CAST(created_date AS TIMESTAMP) AS created_date,
    CAST(closed_date AS TIMESTAMP) AS closed_date,
    CAST(last_modified_date AS TIMESTAMP) AS last_modified_date,

    -- Owner & Department
    TRIM(CAST(owner_department AS STRING)) AS owner_department,

    -- Derived fields
    CASE
        WHEN UPPER(TRIM(sr_type)) = '311 INFORMATION ONLY CALL' THEN TRUE
        ELSE FALSE
    END AS is_info_call,

    CASE
        WHEN ward = 28 AND UPPER(TRIM(sr_type)) = '311 INFORMATION ONLY CALL' THEN TRUE
        ELSE FALSE
    END AS is_admin_ward_info_call,

    -- SCD2 sequence key (for ordering changes)
    COALESCE(last_modified_date, created_date) AS _sequence_timestamp,

    -- Audit
    current_timestamp() AS _processed_timestamp

FROM STREAM(workspace.bronze.bronze_raw_311_requests)

WHERE
    sr_number IS NOT NULL AND TRIM(sr_number) != ''
    AND created_date IS NOT NULL
    AND sr_type IS NOT NULL AND TRIM(sr_type) != '';


-- =============================================================================
-- SILVER LAYER: SCD Type 2 History Table
-- =============================================================================

CREATE STREAMING TABLE silver_scd2_311_requests (
    CONSTRAINT valid_sr_number EXPECT (sr_number IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_created_date EXPECT (created_date IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_status EXPECT (status IN ('Open', 'Completed', 'Canceled')) ON VIOLATION WARN
)
COMMENT "SCD2 Type 2 history of 311 requests - tracks all status changes over time"
TBLPROPERTIES (
    "quality" = "silver",
    "pipelines.autoOptimize.managed" = "true"
);

APPLY CHANGES INTO LIVE.silver_scd2_311_requests
FROM STREAM(LIVE.silver_staged_311_requests)
KEYS (sr_number)
SEQUENCE BY _sequence_timestamp
COLUMNS * EXCEPT (_sequence_timestamp, _processed_timestamp)
STORED AS SCD TYPE 2;

-- =============================================================================
-- SILVER LAYER: Current State View (latest version of each request)
-- =============================================================================

CREATE LIVE TABLE silver_current_311_requests
COMMENT "Current state of all 311 requests (latest SCD2 version)"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
    sr_number,
    sr_type,
    sr_short_code,
    status,
    street_address,
    city,
    zip_code,
    ward,
    community_area,
    latitude,
    longitude,
    created_date,
    closed_date,
    last_modified_date,
    owner_department,
    is_info_call,
    is_admin_ward_info_call,
    _START_AT AS valid_from,
    _END_AT AS valid_to
FROM LIVE.silver_scd2_311_requests
WHERE _END_AT IS NULL;

-- =============================================================================
-- GOLD LAYER: Daily Aggregates (Service Requests Only)
-- =============================================================================

CREATE LIVE TABLE gold_daily_service_request_summary (
    CONSTRAINT valid_request_date EXPECT (request_date IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT positive_request_count EXPECT (total_requests > 0) ON VIOLATION WARN
)
COMMENT "Daily aggregates of 311 service requests (excluding info calls)"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
    DATE(created_date) AS request_date,
    COUNT(*) AS total_requests,

-- Status breakdown
SUM(CASE WHEN status = 'Open' THEN 1 ELSE 0 END) AS open_count,
SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) AS completed_count,
SUM(CASE WHEN status = 'Canceled' THEN 1 ELSE 0 END) AS canceled_count,

-- Top request types (for features)
COUNT(DISTINCT sr_type) AS unique_request_types,

-- Resolution metrics
AVG(
    CASE WHEN closed_date IS NOT NULL
    THEN TIMESTAMPDIFF(HOUR, created_date, closed_date)
    END
) AS avg_resolution_hours

-- Location spread
COUNT(DISTINCT ward) AS unique_wards,
COUNT(DISTINCT community_area) AS unique_community_areas,

-- Day of week (for seasonality features)
DAYOFWEEK(created_date) AS day_of_week_num,
CASE WHEN DAYOFWEEK(created_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,

-- Month (for seasonality)
MONTH(created_date) AS month_num,
YEAR(created_date) AS year_num

FROM LIVE.silver_current_311_requests
WHERE is_info_call = FALSE
GROUP BY DATE(created_date), DAYOFWEEK(created_date), MONTH(created_date), YEAR(created_date);

-- =============================================================================
-- GOLD LAYER: Request Type Daily Summary
-- =============================================================================

CREATE LIVE TABLE gold_request_type_daily_summary
COMMENT "Daily request counts by type (excluding info calls)"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
    DATE(created_date) AS request_date,
    sr_type,
    sr_short_code,
    COUNT(*) AS total_requests,

    SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) AS completed_count,

    AVG(
        CASE WHEN closed_date IS NOT NULL
        THEN TIMESTAMPDIFF(HOUR, created_date, closed_date)
        END
    ) AS avg_resolution_hours

FROM LIVE.silver_current_311_requests
WHERE is_info_call = FALSE
GROUP BY DATE(created_date), sr_type, sr_short_code;

-- =============================================================================
-- GOLD LAYER: SCD2 Lifecycle Analytics
-- =============================================================================

CREATE LIVE TABLE gold_lifecycle_analytics
COMMENT "Time-in-status analytics from SCD2 history"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
    sr_number,
    sr_type,
    status,
    ward,
    _START_AT AS status_start,
    _END_AT AS status_end,

    -- Time in this status (hours)
    CASE
        WHEN _END_AT IS NOT NULL
        THEN TIMESTAMPDIFF(HOUR, _START_AT, _END_AT)
        ELSE TIMESTAMPDIFF(HOUR, _START_AT, current_timestamp())
    END AS hours_in_status,

    -- Is this the current version?
    CASE WHEN _END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current

FROM LIVE.silver_scd2_311_requests
WHERE is_info_call = FALSE;

-- =============================================================================
-- DATA QUALITY VIEWS
-- =============================================================================

CREATE LIVE VIEW dq_null_check
COMMENT "Data quality: check for null critical fields in current requests"
AS
SELECT
    'silver_current' AS layer,
    COUNT(*) AS total_records,
    SUM(CASE WHEN sr_number IS NULL THEN 1 ELSE 0 END) AS null_sr_number,
    SUM(CASE WHEN created_date IS NULL THEN 1 ELSE 0 END) AS null_created_date,
    SUM(CASE WHEN sr_type IS NULL THEN 1 ELSE 0 END) AS null_sr_type,
    SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) AS null_status,
    ROUND(
        (COUNT(*) - SUM(CASE WHEN sr_number IS NULL OR created_date IS NULL
         OR sr_type IS NULL OR status IS NULL THEN 1 ELSE 0 END)) / COUNT(*) * 100, 2
    ) AS completeness_pct
FROM LIVE.silver_current_311_requests;


CREATE LIVE VIEW dq_daily_volume_check
COMMENT "Daily quality: daily volume for detecting anomalies in ingestion"
AS
SELECT
    DATE(created_date) AS request_date,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN is_info_call THEN 1 ELSE 0 END) AS info_calls,
    SUM(CASE WHEN NOT is_info_call THEN 1 ELSE 0 END) AS service_requests,
    CASE
        WHEN COUNT(*) > 15000 THEN 'HIGH - possible duplicate ingestion'
        WHEN COUNT(*) < 1000 THEN 'LOW - possible missing data'
        ELSE 'NORMAL'
    END AS volume_status
FROM LIVE.silver_current_311_requests
GROUP BY DATE(created_date)
ORDER BY request_date DESC;
LIMIT 30;