# 🏙️ Chicago 311 Service Request Intelligence Platform

> **A Production-Grade ML Portfolio Project** demonstrating end-to-end machine learning engineering on **Azure Databricks** with Lakeflow, SCD Type 2, MLflow, and a full CI/CD pipeline.

[![CI/CD Pipeline](https://github.com/udayjoshi-captech/chi311-ml-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/udayjoshi-captech/chi311-ml-platform/actions/workflows/ci.yml)
[![Azure](https://img.shields.io/badge/Azure-Cloud-0078D4?logo=microsoftazure)](https://azure.microsoft.com)
[![Databricks](https://img.shields.io/badge/Databricks-Sandbox-FF3621?logo=databricks)](https://databricks.com)
[![MLflow](https://img.shields.io/badge/MLflow-Experiment%20Tracking-0194E2?logo=mlflow)](https://mlflow.org)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python)](https://python.org)
[![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?logo=terraform)](https://terraform.io)

---

## 📋 Table of Contents

1. [Project Overview](#-project-overview)
2. [Architecture](#-architecture)
3. [Technology Stack](#-technology-stack)
4. [Project Structure](#-project-structure)
5. [Quick Start](#-quick-start)
6. [CI/CD Pipeline](#-cicd-pipeline)
7. [Databricks Asset Bundle](#-databricks-asset-bundle)
8. [Monitoring & Observability](#-monitoring--observability)
9. [Data Engineering Practices](#-data-engineering-practices)
10. [Cost Management](#-cost-management)
11. [Key Findings](#-key-findings)
12. [ML Portfolio Framework Alignment](#-ml-portfolio-framework-alignment)

---

## 🎯 Project Overview

### Problem Statement

Chicago's 311 service handles ~3,000 non-emergency service requests daily. Operations teams lack proactive tools to detect demand spikes before they overwhelm resources, resulting in degraded response times and inefficient staffing.

### Solution

An end-to-end ML platform that:
- **Forecasts** 7-day service request volumes for staffing optimisation
- **Detects anomalies** using statistical thresholds (mean + 2σ)
- **Tracks request lifecycles** via SCD Type 2 for time-in-status analytics
- **Validates data quality** at every pipeline stage with Great Expectations
- **Monitors pipeline health** with per-task row counts, duration, and drift detection

### Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                        DATA INGESTION                            │
│  Chicago 311 API (Socrata) → Chi311APIClient → ADLS Gen2 Volume  │
└──────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│               DATA QUALITY (Great Expectations)                  │
│  Bronze expectations → Silver expectations                       │
│  Results persisted to gold.dq_checkpoint_results + ADLS docs     │
└──────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│          LAKEFLOW PIPELINE (Medallion + SCD Type 2)              │
│  Bronze (Autoloader) → Silver (SCD2 history) → Gold (aggregates) │
│  CONSTRAINT expectations on every table                          │
└──────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                     ML PIPELINE (MLflow)                         │
│  Feature Engineering → Prophet Training → Anomaly Detection      │
│  Experiments tracked in MLflow, metrics logged per run           │
└──────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                  SERVING & MONITORING                            │
│  Databricks SQL Dashboard ← Batch Predictions ← PredictionLogger │
│  PipelineMetrics → gold.pipeline_run_log (observability table)   │
│  Azure Monitor alert → email on Databricks job failure           │
└──────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Cloud** | Microsoft Azure | Subscription-based sandbox |
| **Compute** | Azure Databricks (Premium SKU) | Managed Spark, Unity Catalog, MLflow |
| **Storage** | ADLS Gen2 + Delta Lake | ACID transactions, time travel, medallion architecture |
| **ETL** | Lakeflow (SQL Declarative Pipelines) | Declarative SCD2 via `APPLY CHANGES INTO` |
| **Data Quality** | Great Expectations 0.18 | Declarative validation with persistent data docs |
| **ML Tracking** | MLflow | Experiment management, model registry |
| **Forecasting** | Prophet | Handles seasonality, holidays, missing data |
| **Feature Store** | Custom feature engineering library | Temporal, lag, rolling features |
| **Dashboard** | Databricks SQL Dashboard | Native monitoring with auto-refresh and permissions |
| **IaC** | Terraform (azurerm ~3.80) | Reproducible Azure resource provisioning |
| **CI/CD** | GitHub Actions | Linting, unit tests, automated bundle deploy |
| **Orchestration** | Databricks Asset Bundle (DAB) | Jobs + Lakeflow pipeline lifecycle management |
| **Alerting** | Azure Monitor scheduled query rules | Email on job failure via Log Analytics |

---

## 📁 Project Structure

```
chi311-ml-platform/
│
├── README.md
├── databricks.yml                       # Databricks Asset Bundle (jobs + pipeline)
├── requirements.txt                     # Production dependencies
├── requirements-dev.txt                 # Dev dependencies (ruff, black, mypy, pytest)
├── setup.py
├── pytest.ini
├── .gitignore
│
├── notebooks/
│   ├── 01_setup/
│   │   └── 00_setup_exploration.py      # Catalog, schemas, volumes, EDA
│   ├── 02_ingestion/
│   │   ├── 01_api_to_volume.py          # Chicago 311 API → ADLS Volume
│   │   └── 02_bronze_autoloader.py      # Autoloader: Volume → Bronze Delta table
│   ├── 03_data_quality/
│   │   └── 01_data_quality_checks.py    # GE validation + checkpoint persistence
│   └── 04_ml/
│       ├── 01_forecasting.py            # Prophet training + MLflow tracking
│       └── 02_anomaly_detection.py      # Statistical anomaly detection
│
├── pipelines/
│   └── chi311_scd2_pipeline.sql         # Lakeflow: Silver SCD2 + Gold aggregates
│                                        # Includes CONSTRAINT expectations on every table
│
├── src/chi311/
│   ├── ingestion/
│   │   └── api_client.py                # Paginated Socrata client with retry + logging
│   ├── features/
│   │   └── feature_engineering.py       # Temporal, lag, rolling features with logging
│   ├── models/
│   │   └── prophet_forecaster.py        # Prophet wrapper: prepare_data, train, predict
│   └── monitoring/
│       ├── prediction_logger.py         # Delta MERGE upsert + drift detection
│       └── pipeline_metrics.py          # Per-task row counts + duration logging
│
├── tests/
│   ├── unit/
│   │   └── test_feature_engineering.py  # 7 tests — all passing in CI
│   └── integration/
│       └── tests_pipeline_e2e.py
│
├── dashboards/
│   ├── queries.sql                      # SQL queries for Databricks dashboard
│   ├── setup_dashboard.py               # Notebook for programmatic setup
│   └── docs/databricks-dashboard-setup.md  # Manual setup guide
│
├── infrastructure/terraform/
│   ├── main.tf                          # All Azure resources + Monitor alert
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars                 # (gitignored) secrets + workspace IDs
│
└── .github/workflows/
    └── ci.yml                           # 4-stage CI/CD pipeline
```

---

## 🚀 Quick Start

### Prerequisites

- Azure subscription with Databricks workspace
- Python 3.11+
- Terraform CLI ≥ 1.5
- Azure CLI (`az login`)

### 1 — Clone and install

```bash
git clone https://github.com/udayjoshi-captech/chi311-ml-platform.git
cd chi311-ml-platform
python -m venv venv && source venv/bin/activate  # Windows: venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
pip install -e .
```

### 2 — Deploy Azure infrastructure

```bash
cd infrastructure/terraform
az login
terraform init
terraform apply
```

Minimum `terraform.tfvars`:

```hcl
azure_subscription_id            = "<your-subscription-id>"
owner_email                      = "<your-email>"
alert_email                      = "<your-email>"
monthly_budget                   = 100
databricks_workspace_url         = "https://adb-<id>.azuredatabricks.net"
databricks_workspace_resource_id = "/subscriptions/.../workspaces/<name>"
```

### 3 — Deploy Databricks Asset Bundle

```bash
cd ../..
databricks bundle deploy -t dev
```

Creates 3 jobs and 1 Lakeflow pipeline in the dev workspace.

### 4 — Run ingestion and ML jobs

```bash
databricks bundle run -t dev daily_ingestion
databricks bundle run -t dev data_quality
databricks bundle run -t dev ml_training
```

### 5 — Launch dashboard locally

```bash
cd app
pip install -r requirements.txt
DATABRICKS_HOST=https://adb-<id>.azuredatabricks.net \
DATABRICKS_TOKEN=<your-pat> \
DATABRICKS_CATALOG=workspace \
streamlit run dashboard.py
```

---

## ⚙️ CI/CD Pipeline

The GitHub Actions workflow (`.github/workflows/ci.yml`) runs on every push to `main`:

```
Code Quality → Unit Tests → Deploy to Dev → Deploy to Prod (disabled)
```

| Stage | What it does |
|---|---|
| **Code Quality** | `ruff check` + `black --check` + `mypy` (warn-only) on `src/` and `tests/` |
| **Unit Tests** | `pytest tests/unit` with `--cov=src/chi311` coverage report |
| **Deploy to Dev** | `databricks bundle deploy -t dev` using environment secrets |
| **Deploy to Prod** | Disabled (`if: false`) — enable by adding secrets to `production` environment |

### Required GitHub environment secrets (Settings → Environments → dev)

| Secret | Value |
|---|---|
| `DATABRICKS_HOST` | `https://adb-<workspace-id>.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Databricks PAT (workspace → Settings → Developer → Access tokens) |

---

## 📦 Databricks Asset Bundle

`databricks.yml` configures all dev and prod resources. Dev clusters use **single-node spot pricing** for cost efficiency.

### Dev jobs

| Job | Schedule | Cluster |
|---|---|---|
| `[DEV] Chi311 Daily Ingestion` | 6 AM daily (paused) | `Standard_DS3_v2`, single-node, spot |
| `[DEV] Chi311 Data Quality` | On-demand | `Standard_DS3_v2`, single-node, spot |
| `[DEV] Chi311 ML Training` | Monday 8 AM (paused) | `Standard_DS3_v2`, single-node, spot |

### Lakeflow pipeline

| Setting | Value |
|---|---|
| Name | `[DEV] Chi311 Lakeflow Pipeline` |
| Source | `pipelines/chi311_scd2_pipeline.sql` |
| Cluster | `Standard_DS3_v2`, 1 worker, spot pricing |
| Mode | Development |

---

## 🔍 Monitoring & Observability

### 1. Pipeline Run Metrics — `gold.pipeline_run_log`

Wrap every notebook task with `PipelineMetrics` to record row counts, duration, and status:

```python
from chi311.monitoring import PipelineMetrics

metrics = PipelineMetrics(catalog="workspace")
metrics.start(task_name="bronze_autoloader", run_id="<run-id>")
try:
    # ... transformation logic ...
    PipelineMetrics.assert_non_empty(df_out, "bronze output")  # raises on empty
    metrics.finish(rows_in=rows_in, rows_out=df_out.count(), spark_session=spark)
except Exception as e:
    metrics.fail(str(e), spark_session=spark)
    raise
```

Columns: `run_id`, `task_name`, `status`, `rows_in`, `rows_out`, `rows_dropped`, `duration_seconds`, `error_message`, `logged_at`.

### 2. Prediction Drift Detection — `gold.gold_prediction_log`

`PredictionLogger` upserts predictions via Delta MERGE on `(ds, model_version)` — idempotent on reruns. `check_drift()` computes MAPE against actuals and emits `logger.warning` when the threshold (default 20%) is exceeded.

### 3. Data Quality Checkpoints — `gold.dq_checkpoint_results`

After every DQ notebook run, Great Expectations results (evaluated / passed / failed / pass rate %) are persisted to a queryable Delta table. HTML data docs are built to the ADLS `checkpoints` container.

### 4. Azure Monitor Alert

A scheduled query rule polls Log Analytics every 15 minutes for `DatabricksJobs runFailed` events and sends an email to `alert_email` on any failure. Provisioned automatically by Terraform.

### 5. Databricks SQL Dashboard — three tabs

| Tab | Contents |
|---|---|
| 📊 Overview | KPI cards (total requests, avg daily, MAPE), daily volume trends, day-of-week patterns, recent pipeline runs |
| 🔮 Forecasts | 7-day forecast with confidence intervals, prediction vs actual scatter plot, model MAPE trends over time |
| 🔍 Monitoring | Data quality metrics, pipeline health status, anomaly detection results, drift monitoring, task duration trends |

**Setup:** See `docs/databricks-dashboard-setup.md` for manual setup or run `dashboards/setup_dashboard.py` for automated creation.

**Benefits over Streamlit:**
- ✅ No separate deployment infrastructure
- ✅ Native Unity Catalog permissions
- ✅ Auto-refresh scheduling built-in  
- ✅ Lower cost (uses shared SQL Warehouse)
- ✅ Email/Slack subscriptions included

---

## 🏗️ Data Engineering Practices

| Practice | Implementation |
|---|---|
| **Structured logging** | `logging.getLogger(__name__)` in all modules; row counts logged at each stage |
| **Idempotent writes** | Delta MERGE on natural keys — safe to rerun without duplicates |
| **Schema enforcement** | Lakeflow `CONSTRAINT` on silver/gold (`ON VIOLATION DROP ROW` or `WARN`) |
| **Empty dataset guards** | `PipelineMetrics.assert_non_empty()` raises if any stage produces 0 rows |
| **Retry with backoff** | `Chi311APIClient` retries with exponential backoff; raises after exhausting retries |
| **No hardcoded config** | Catalog names, workspace URLs, tokens all from environment variables or secrets |
| **Spot pricing** | All dev clusters use `SPOT_WITH_FALLBACK_AZURE` — 60–80% cost reduction |
| **Single-node clusters** | `num_workers: 0` with `local[*]` Spark — eliminates worker VM cost in dev |

---

## 💰 Cost Management

Monthly budget alert: **$100** (`chi311-dev-budget`, Azure Cost Management).

| Component | Estimated Monthly Cost |
|---|---|
| Databricks workspace (idle overhead) | ~$30–50 |
| Compute per job run (`DS3_v2` spot, ~10 min) | ~$0.05–0.10 per run |
| ADLS Gen2 storage (~15 GB) | ~$0.30 |
| Log Analytics (30-day retention minimum) | ~$1–3 |
| Azure Monitor alert rule | ~$0.10 |
| **Total (light dev usage)** | **~$35–60/month** |

Single-node spot clusters cut per-run compute cost by ~80% vs the original two-node on-demand configuration.

---

## 🔑 Key Findings from Data Exploration

| Finding | Value | Impact |
|---|---|---|
| Daily service requests | ~3,000 (excl. info calls) | Forecasting baseline |
| Info calls share | ~40% (`"311 INFORMATION ONLY CALL"`) | Must filter before ML |
| Status values | Open, Completed, Canceled | SCD2 tracking targets |
| Anomaly threshold | 4,851 (mean + 2σ) | Detection baseline |
| Weekend drop | 35–40% | Seasonality feature |
| Ward 28 dominance | 39% of requests | Info call admin ward — excluded |

---

## 🏗️ ML Portfolio Framework Alignment

| Framework Element | Implementation | Status |
|---|---|---|
| **Problem Framing** | 311 demand forecasting & anomaly detection | ✅ |
| **Data Sourcing** | Chicago 311 API (Socrata) with paginated incremental loads | ✅ |
| **Data Quality** | Great Expectations at Bronze/Silver + Lakeflow constraints at Silver/Gold | ✅ |
| **Feature Engineering** | Temporal, lag, rolling features with row-count logging | ✅ |
| **Model Development** | Prophet + MLflow experiment tracking | ✅ |
| **Deployment** | Databricks Asset Bundle (3 jobs + 1 Lakeflow pipeline) deployed via CI | ✅ |
| **Monitoring** | PipelineMetrics, PredictionLogger, DQ checkpoints, Azure Monitor alerts | ✅ |
| **CI/CD** | GitHub Actions: lint → unit tests → deploy-dev | ✅ |
| **IaC** | All Azure resources managed by Terraform, idempotent plan | ✅ |

---

## 📄 License

This project is for portfolio/educational purposes. Data sourced from the [Chicago Open Data Portal](https://data.cityofchicago.org/) under open data licence.
