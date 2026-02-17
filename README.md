# 🏙️ Chicago 311 Service Request Intelligence Platform

> **A Production-Grade ML Portfolio Project** demonstrating end-to-end machine learning engineering on **Azure Databricks** with Lakeflow Declarative Pipelines, SCD Type 2, and MLflow.

[![Azure](https://img.shields.io/badge/Azure-Cloud-0078D4?logo=microsoftazure)](https://azure.microsoft.com)
[![Databricks](https://img.shields.io/badge/Databricks-Sandbox-FF3621?logo=databricks)](https://databricks.com)
[![MLflow](https://img.shields.io/badge/MLflow-Experiment%20Tracking-0194E2?logo=mlflow)](https://mlflow.org)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python)](https://python.org)

---

## 📋 Table of Contents

1. [Project Overview](#-project-overview)
2. [ML Portfolio Framework Alignment](#-ml-portfolio-framework-alignment)
3. [Architecture](#-architecture)
4. [Quick Start](#-quick-start)
5. [Component Deep Dives](#-component-deep-dives)
6. [Project Structure](#-project-structure)
7. [Success Metrics](#-success-metrics)
8. [Cost Analysis](#-cost-analysis)

---

## 🎯 Project Overview

### Problem Statement

Chicago's 311 service handles thousands of non-emergency requests daily. Operations teams lack proactive tools to detect demand spikes before they overwhelm resources, resulting in degraded response times and inefficient staffing.

### Solution

An end-to-end ML platform that:
- **Forecasts** 7-day service request volumes for staffing optimization
- **Detects anomalies** 2+ hours before spikes overwhelm resources
- **Tracks request lifecycles** via SCD Type 2 for time-in-status analytics
- **Validates data quality** at every pipeline stage with Great Expectations

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA INGESTION                              │
│  Chicago 311 API → API Client → Azure Databricks Volume         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  DATA QUALITY (Great Expectations)              │
│  Bronze Expectations → Silver Expectations → Gold Expectations  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│               LAKEFLOW PIPELINE (Medallion + SCD2)              │
│  Bronze (Raw) → Silver (SCD2 History) → Gold (Aggregates)       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ML PIPELINE (MLflow)                         │
│  Feature Engineering → Prophet Training → Anomaly Detection     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   SERVING & MONITORING                          │
│  Streamlit Dashboard ← Batch Predictions ← Prediction Logging   │
└─────────────────────────────────────────────────────────────────┘
```

### Key Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Cloud** | Azure + Databricks | Paid sandbox with full feature access |
| **History Tracking** | SCD Type 2 | Enables lifecycle analytics (time-in-status, transitions) |
| **Data Quality** | Great Expectations | Declarative validation with data docs |
| **Pipeline** | Lakeflow Declarative (SQL) | Simplicity, built-in SCD2 via `APPLY CHANGES INTO` |
| **ML Tracking** | MLflow | Native Databricks integration |
| **Forecasting** | Prophet | Handles seasonality, missing data, holidays |
| **Storage** | ADLS Gen2 + Delta Lake | ACID transactions, time travel, Unity Catalog |
| **IaC** | Terraform (azurerm) | Reproducible Azure infrastructure |

### Technology Stack

| Layer | Technology | Why This Choice |
|-------|------------|-----------------|
| **Cloud** | Microsoft Azure | Paid sandbox environment with full capabilities |
| **Compute** | Azure Databricks | Managed Spark, Lakeflow, Unity Catalog |
| **Storage** | ADLS Gen2 + Delta Lake | ACID transactions, time travel, governance |
| **ETL** | Lakeflow Declarative Pipelines | Declarative SQL, auto-optimization |
| **SCD2** | `APPLY CHANGES INTO` | Built-in CDC handling for history tracking |
| **ML Tracking** | MLflow | Native Databricks integration, experiment management |
| **Forecasting** | Prophet | Handles seasonality, missing data, outliers |
| **Dashboard** | Streamlit | Fast prototyping, Python native |
| **IaC** | Terraform (azurerm) | Azure resource provisioning |
| **CI/CD** | GitHub Actions | Free tier, Databricks CLI integration |

---

## 🏗️ ML Portfolio Framework Alignment

| Framework Element | Implementation | Status |
|-------------------|----------------|--------|
| **Problem Framing** | 311 demand forecasting & anomaly detection | ✅ |
| **Data Sourcing** | Chicago 311 API (Socrata) with incremental loads | ✅ |
| **Data Quality** | Great Expectations at Bronze/Silver/Gold layers | ✅ |
| **Feature Engineering** | Temporal, lag, rolling, calendar features | ✅ |
| **Model Development** | Prophet + MLflow experiment tracking | ✅ |
| **Deployment** | Batch predictions + Streamlit dashboard | ✅ |
| **Monitoring** | Prediction logging, data drift, DQ dashboards | ✅ |
| **Documentation** | Project scoping, architecture decisions, runbooks | ✅ |

---

## 🚀 Quick Start

### Prerequisites

- Azure subscription with Databricks sandbox workspace
- Python 3.11+
- Git
- Terraform CLI (for infrastructure provisioning)
- Azure CLI (`az login`)

### Step 1: Clone Repository

```bash
git clone https://github.com/udayjoshi-captech/chi311-ml-platform.git
cd chi311-intelligence-platform
```

### Step 2: Deploy Azure Infrastructure

```bash
cd infrastructure/terraform
az login
terraform init
terraform plan -var="environment=dev"
terraform apply -var="environment=dev"
```

### Step 3: Configure Databricks Workspace

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure with your Azure Databricks workspace URL
databricks configure --token
# Enter your workspace URL: https://adb-XXXXX.azuredatabricks.net
# Enter your PAT token

# Import notebooks
databricks workspace import_dir ./notebooks /Workspace/Users/YOUR_EMAIL/chi311
```

### Step 4: Set Up Unity Catalog Volumes

1. Open `notebooks/01_setup/00_setup_exploration.py` in Databricks
2. Run all cells to create catalog, schemas, and volumes

### Step 5: Run Data Ingestion

1. Open `notebooks/02_ingestion/01_api_to_volume.py`
2. Run to fetch data from Chicago 311 API into Databricks Volume

### Step 6: Create Lakeflow Pipeline

1. Navigate to **Workflows** → **Lakeflow Declarative Pipelines**
2. Click **Create Pipeline**
3. Point to `pipelines/chi311_scd2_pipeline.sql`
4. Configure: **Serverless** or cluster compute, **Development mode**
5. Run the pipeline

### Step 7: Run ML Training

1. Open `notebooks/04_ml/01_forecasting.py`
2. Attach to compute
3. Run all cells — MLflow tracks experiments automatically

### Step 8: Launch Dashboard (Local)

```bash
cd app
pip install -r requirements.txt
streamlit run dashboard.py
```

---

## 📁 Project Structure

```
chi311-intelligence-platform/
│
├── README.md                           # This file
├── SETUP_GUIDE.md                      # Detailed Azure setup instructions
├── databricks.yml                      # Databricks Asset Bundle config
├── requirements.txt                    # Production dependencies
├── requirements-dev.txt                # Development dependencies
├── setup.py                            # Python package config
├── pytest.ini                          # Test configuration
├── .gitignore                          # Git ignore rules
│
├── docs/
│   ├── PROJECT_SCOPING.md              # Problem framing & success metrics
│   ├── DATA_SOURCING.md                # Data source documentation
│   ├── ARCHITECTURE_DECISIONS.md       # ADRs and alternatives considered
│   ├── PIPELINE_EXPLAINED.md           # Beginner-friendly pipeline guide
│   └── COST_ANALYSIS.md               # Azure cost breakdown
│
├── notebooks/
│   ├── 01_setup/
│   │   └── 00_setup_exploration.py     # Catalog, schemas, volumes, EDA
│   ├── 02_ingestion/
│   │   ├── 01_api_to_volume.py         # Chicago 311 API → Volume
│   │   └── 02_bronze_autoloader.py     # Autoloader: Volume → Bronze table
│   ├── 03_data_quality/
│   │   └── 01_data_quality_checks.py   # Great Expectations validation
│   └── 04_ml/
│       ├── 01_forecasting.py           # Prophet + MLflow training
│       └── 02_anomaly_detection.py     # Anomaly detection pipeline
│
├── pipelines/
│   └── chi311_scd2_pipeline.sql        # Lakeflow DLT: Silver SCD2 + Gold
│
├── src/chi311/
│   ├── __init__.py
│   ├── ingestion/
│   │   ├── __init__.py
│   │   └── api_client.py               # Robust Socrata API client
│   ├── features/
│   │   ├── __init__.py
│   │   └── feature_engineering.py       # Temporal & lag features
│   ├── models/
│   │   ├── __init__.py
│   │   └── prophet_forecaster.py        # Prophet wrapper with MLflow
│   └── monitoring/
│       ├── __init__.py
│       └── prediction_logger.py         # Prediction logging & drift
│
├── tests/
│   ├── unit/
│   │   ├── test_api_client.py
│   │   └── test_feature_engineering.py
│   └── integration/
│       └── test_pipeline_e2e.py
│
├── app/
│   ├── dashboard.py                     # Streamlit dashboard
│   ├── requirements.txt                 # Dashboard dependencies
│   └── Dockerfile                       # Container for deployment
│
├── infrastructure/
│   └── terraform/
│       ├── main.tf                      # Azure resources (RG, ADLS, Databricks)
│       ├── variables.tf                 # Terraform variables
│       └── outputs.tf                   # Terraform outputs
│
└── .github/
    └── workflows/
        └── ci.yml                       # CI/CD pipeline
```

---

## 📊 Success Metrics

| Metric Type | Metric | Target | Current |
|-------------|--------|--------|---------|
| **Business** | Anomaly Detection Lead Time | >2 hours before spike | TBD |
| **Business** | False Positive Rate | <15% | TBD |
| **ML** | Forecast MAPE | <15% | TBD |
| **ML** | Anomaly Precision | >0.80 | TBD |
| **Data** | Pipeline Freshness | <4 hours | TBD |
| **Data** | DQ Pass Rate | >95% | TBD |

---

## 💰 Cost Analysis

See [docs/COST_ANALYSIS.md](docs/COST_ANALYSIS.md) for detailed breakdown.

| Component | Estimated Monthly Cost |
|-----------|----------------------|
| Azure Databricks Compute (sandbox) | Included in subscription |
| ADLS Gen2 Storage (~15 GB) | ~$0.30 |
| Azure Monitor / Log Analytics | ~$1.00 |
| Streamlit Dashboard (local) | $0.00 |
| **Total Infrastructure** | **~$1.30/month** |

> **Note**: With a paid Azure Databricks sandbox, compute costs are covered by the subscription. Infrastructure costs are minimal — primarily storage and monitoring.

---

## 🔑 Key Findings from Data Exploration

| Finding | Value | Impact |
|---------|-------|--------|
| Daily service requests | ~3,000 (excl. info calls) | Baseline for forecasting |
| Info calls share | ~40% ("311 INFORMATION ONLY CALL") | Must filter for ML |
| Status values | Open, Completed, Canceled | SCD2 tracking targets |
| Anomaly threshold | 4,851 (mean + 2σ) | Detection baseline |
| Weekend drop | 35-40% | Seasonality feature |
| Ward 28 dominance | 39% of requests | Info call admin ward |

---

## 📄 License

This project is for portfolio/educational purposes. Data sourced from [Chicago Open Data Portal](https://data.cityofchicago.org/) under open data license.
