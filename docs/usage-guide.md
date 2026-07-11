# Chi311 Platform Usage Guide

## Quick Start

### 1. Installation

```bash
pip install -e .
```

### 2. Configuration

Set environment variables:

```bash
export CHI311_CATALOG=chi311
export MLFLOW_EXPERIMENT=/Shared/chi311-forecasting
export API_TIMEOUT=30
```

Or use Python configuration:

```python
from chi311.config import get_config

configs = get_config("all")
print(configs["api"])
```

### 3. Fetch Data

```python
from chi311.config import APIConfig
from chi311.ingestion.api_client import Chi311APIClient

config = APIConfig()
with Chi311APIClient(config) as client:
    records = client.fetch_all(
        start_date="2024-01-01T00:00:00",
        end_date="2024-01-31T23:59:59"
    )
    print(f"Fetched {len(records)} records")
```

### 4. Train Model

```python
from chi311.config import ProphetConfig
from chi311.models.prophet_forecaster import Chi311Forecaster
import pandas as pd

# Prepare data
df = pd.DataFrame({
    "ds": pd.date_range("2024-01-01", periods=90, freq="D"),
    "y": [3000, 3100, 3200, ...]  # Your data
})

# Train model
prophet_config = ProphetConfig()
forecaster = Chi311Forecaster(
    changepoint_prior_scale=prophet_config.changepoint_prior_scale,
    seasonality_prior_scale=prophet_config.seasonality_prior_scale,
)

metrics = forecaster.train(df, log_to_mlflow=True)
print(f"MAPE: {metrics['mape']:.2f}%")

# Generate predictions
forecast = forecaster.predict(periods=7)
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CHI311_CATALOG` | `chi311` | Unity Catalog name |
| `MLFLOW_EXPERIMENT` | `/Shared/chi311-forecasting` | MLflow experiment |
| `API_TIMEOUT` | `30` | API request timeout (seconds) |
| `API_MAX_RETRIES` | `3` | Maximum retry attempts |
| `MIN_TRAINING_POINTS` | `10` | Minimum data points for training |
| `VALIDATION_SPLIT` | `0.2` | Validation set fraction |
| `ANOMALY_THRESHOLD_SIGMA` | `2.0` | Sigma for anomaly detection |
| `DRIFT_MAPE_THRESHOLD` | `0.20` | MAPE threshold for drift |

### Python Configuration

```python
from chi311.config import APIConfig, ProphetConfig, PipelineConfig

# Custom API configuration
api_config = APIConfig(
    timeout=60,
    max_retries=5,
    page_size=10000
)

# Custom Prophet configuration
prophet_config = ProphetConfig(
    changepoint_prior_scale=0.2,
    seasonality_mode="additive"
)

# Custom pipeline configuration
pipeline_config = PipelineConfig(
    catalog="chi311_dev",
    initial_load_days=30
)
```

## Error Handling

All functions raise appropriate exceptions:

```python
from chi311.ingestion.api_client import Chi311APIClient
from chi311.config import APIConfig

config = APIConfig()

try:
    with Chi311APIClient(config) as client:
        records = client.fetch_all("2024-01-01T00:00:00", "2024-01-31T23:59:59")
except ValueError as e:
    print(f"Invalid input or malformed response: {e}")
except ConnectionError as e:
    print(f"Network or API connection issue: {e}")
except RuntimeError as e:
    print(f"Operation failed after retries: {e}")
```

## Testing

Run tests:

```bash
# All tests
pytest

# Unit tests only
pytest tests/unit

# With coverage
pytest --cov=src/chi311 --cov-report=html

# Specific test file
pytest tests/unit/test_prophet_forecaster.py -v
```

## Monitoring

### Pipeline Metrics

```python
from chi311.monitoring.pipeline_metrics import PipelineMetrics

metrics = PipelineMetrics(catalog="chi311")
metrics.start(task_name="bronze_load", run_id="run123")

try:
    # Your data processing
    result = process_data()
    
    metrics.finish(
        rows_in=1000,
        rows_out=950,
        rows_dropped=50,
        spark_session=spark
    )
except Exception as e:
    metrics.fail(str(e), spark_session=spark)
    raise
```

### Prediction Logging

```python
from chi311.monitoring.prediction_logger import PredictionLogger
import pandas as pd

logger = PredictionLogger(catalog="chi311")

predictions = pd.DataFrame({
    "ds": pd.date_range("2024-01-01", periods=7, freq="D"),
    "y_pred": [3000, 3100, 3200, 3150, 3300, 2900, 2800]
})

logger.log_predictions(predictions, model_version="v1.0", spark_session=spark)
```

## Common Patterns

### Using Context Managers

Always use context managers for resource cleanup:

```python
# API Client
with Chi311APIClient(config) as client:
    data = client.fetch_all(start, end)
# Session automatically closed

# Pipeline Metrics
metrics = PipelineMetrics(catalog="chi311")
metrics.start("task", "run_id")

with metrics:
    # Do work
    result = process()
    metrics.finish(100, 95, spark_session=spark)
# Automatically fails if exception occurs
```

### Validation

Validate inputs early:

```python
from chi311.utils import validate_schema

validate_schema(
    df,
    required_columns=["ds", "y"],
    name="training_data"
)
```

## Troubleshooting

### Common Issues

**Issue**: `ValueError: Missing required columns`
- **Solution**: Ensure DataFrame has correct column names (`ds`, `y` for forecasting)

**Issue**: `TypeError: Column 'ds' must be datetime type`
- **Solution**: Convert to datetime: `df["ds"] = pd.to_datetime(df["ds"])`

**Issue**: `RuntimeError: Metrics logging failed`
- **Solution**: Check SparkSession is active and catalog/schema exist

**Issue**: `ValueError: API returned non-JSON response`
- **Solution**: Check API endpoint and network connectivity

### Debug Mode

Enable debug logging:

```python
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
```
