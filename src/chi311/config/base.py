"""Base configuration classes for chi311 platform."""
import os
from dataclasses import dataclass, field
from typing import Literal


def _parse_int_env(var_name: str, default: str, config_class: str) -> int:
    """Parse integer from environment variable with descriptive errors."""
    value = os.getenv(var_name, default)
    try:
        return int(value)
    except ValueError as e:
        raise ValueError(
            f"{config_class}.{var_name}: Invalid integer value '{value}'. "
            f"Expected numeric value."
        ) from e


def _parse_float_env(var_name: str, default: str, config_class: str) -> float:
    """Parse float from environment variable with descriptive errors."""
    value = os.getenv(var_name, default)
    try:
        return float(value)
    except ValueError as e:
        raise ValueError(
            f"{config_class}.{var_name}: Invalid float value '{value}'. "
            f"Expected numeric value."
        ) from e


@dataclass
class APIConfig:
    """Configuration for Chicago 311 API client.

    Attributes:
        base_url: Socrata API endpoint
        app_token: API token for authentication (optional)
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries in seconds
        page_size: Number of records per API request
    """
    base_url: str = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"
    app_token: str | None = field(
        default_factory=lambda: os.getenv("CHI311_API_TOKEN")
    )
    timeout: int = field(default_factory=lambda: _parse_int_env("API_TIMEOUT", "30", "APIConfig"))
    max_retries: int = field(default_factory=lambda: _parse_int_env("API_MAX_RETRIES", "3", "APIConfig"))
    retry_delay: float = field(default_factory=lambda: _parse_float_env("API_RETRY_DELAY", "2.0", "APIConfig"))
    page_size: int = field(default_factory=lambda: _parse_int_env("API_PAGE_SIZE", "50000", "APIConfig"))

    def __post_init__(self):
        """Validate configuration values."""
        if self.timeout <= 0:
            raise ValueError(f"timeout must be positive, got {self.timeout}")
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be non-negative, got {self.max_retries}")
        if self.retry_delay <= 0:
            raise ValueError(f"retry_delay must be positive, got {self.retry_delay}")
        if self.page_size <= 0:
            raise ValueError(f"page_size must be positive, got {self.page_size}")


@dataclass
class ProphetConfig:
    """Configuration for Prophet forecasting model.

    Attributes:
        changepoint_prior_scale: Flexibility of trend changes
        seasonality_prior_scale: Strength of seasonality
        seasonality_mode: Additive or multiplicative seasonality
        daily_seasonality: Enable daily patterns
        weekly_seasonality: Enable weekly patterns
        yearly_seasonality: Enable yearly patterns
        country_holidays: Country code for holidays (e.g., "US")
    """
    changepoint_prior_scale: float = field(
        default_factory=lambda: _parse_float_env("PROPHET_CHANGEPOINT_SCALE", "0.1", "ProphetConfig")
    )
    seasonality_prior_scale: float = field(
        default_factory=lambda: _parse_float_env("PROPHET_SEASONALITY_SCALE", "10.0", "ProphetConfig")
    )
    seasonality_mode: Literal["multiplicative", "additive"] = "multiplicative"
    daily_seasonality: bool = False
    weekly_seasonality: bool = True
    yearly_seasonality: bool = True
    country_holidays: str = "US"

    def __post_init__(self):
        """Validate Prophet parameters."""
        if self.changepoint_prior_scale <= 0:
            raise ValueError("changepoint_prior_scale must be positive")
        if self.seasonality_prior_scale <= 0:
            raise ValueError("seasonality_prior_scale must be positive")

    def to_prophet_kwargs(self) -> dict:
        """Convert to Prophet constructor kwargs."""
        return {
            "changepoint_prior_scale": self.changepoint_prior_scale,
            "seasonality_prior_scale": self.seasonality_prior_scale,
            "seasonality_mode": self.seasonality_mode,
            "daily_seasonality": self.daily_seasonality,
            "weekly_seasonality": self.weekly_seasonality,
            "yearly_seasonality": self.yearly_seasonality,
        }


@dataclass
class MLflowConfig:
    """Configuration for MLflow tracking.

    Attributes:
        tracking_uri: MLflow tracking server URI
        registry_uri: MLflow model registry URI
        experiment_name: Default experiment name
    """
    tracking_uri: str = field(
        default_factory=lambda: os.getenv("MLFLOW_TRACKING_URI", "databricks")
    )
    registry_uri: str = field(
        default_factory=lambda: os.getenv("MLFLOW_REGISTRY_URI", "databricks-uc")
    )
    experiment_name: str = field(
        default_factory=lambda: os.getenv("MLFLOW_EXPERIMENT", "/Shared/chi311-forecasting")
    )


@dataclass
class TrainingConfig:
    """Configuration for model training.

    Attributes:
        min_training_points: Minimum data points required for training
        validation_split: Fraction of data to use for validation
        truncate_to_yesterday: Remove future dates from training data
    """
    min_training_points: int = field(
        default_factory=lambda: _parse_int_env("MIN_TRAINING_POINTS", "10", "TrainingConfig")
    )
    validation_split: float = field(
        default_factory=lambda: _parse_float_env("VALIDATION_SPLIT", "0.2", "TrainingConfig")
    )
    truncate_to_yesterday: bool = field(
        default_factory=lambda: os.getenv("TRUNCATE_TO_YESTERDAY", "true").lower() == "true"
    )

    def __post_init__(self):
        """Validate training configuration."""
        if self.min_training_points < 1:
            raise ValueError("min_training_points must be >= 1")
        if not 0 < self.validation_split < 1:
            raise ValueError("validation_split must be between 0 and 1 (exclusive)")


@dataclass
class PipelineConfig:
    """Configuration for pipeline execution.

    Attributes:
        catalog: Unity Catalog name
        initial_load_days: Days to load on first run
        incremental_lookback_days: Days to look back on incremental loads
        anomaly_threshold_sigma: Sigma multiplier for anomaly detection
        drift_mape_threshold: MAPE threshold for drift detection
    """
    catalog: str = field(default_factory=lambda: os.getenv("CHI311_CATALOG", "chi311"))
    initial_load_days: int = field(
        default_factory=lambda: _parse_int_env("INITIAL_LOAD_DAYS", "730", "PipelineConfig")
    )
    incremental_lookback_days: int = field(
        default_factory=lambda: _parse_int_env("INCREMENTAL_LOOKBACK_DAYS", "1", "PipelineConfig")
    )
    anomaly_threshold_sigma: float = field(
        default_factory=lambda: _parse_float_env("ANOMALY_THRESHOLD_SIGMA", "2.0", "PipelineConfig")
    )
    drift_mape_threshold: float = field(
        default_factory=lambda: _parse_float_env("DRIFT_MAPE_THRESHOLD", "0.20", "PipelineConfig")
    )


def get_config(config_type: str = "all"):
    """Get configuration object(s).

    Args:
        config_type: Type of config to get ("api", "prophet", "mlflow", "training", "pipeline", "all")

    Returns:
        Configuration object or dict of all configs
    """
    configs = {
        "api": APIConfig(),
        "prophet": ProphetConfig(),
        "mlflow": MLflowConfig(),
        "training": TrainingConfig(),
        "pipeline": PipelineConfig(),
    }

    if config_type == "all":
        return configs
    elif config_type in configs:
        return configs[config_type]
    else:
        raise ValueError(f"Unknown config type: {config_type}")
