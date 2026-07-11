"""Production environment configuration."""
from .base import APIConfig, ProphetConfig, MLflowConfig, TrainingConfig, PipelineConfig


# Production configurations with stricter settings
PROD_API_CONFIG = APIConfig(
    timeout=30,
    max_retries=3,
)

PROD_TRAINING_CONFIG = TrainingConfig(
    min_training_points=30,  # Require more data for production models
    validation_split=0.2,
)

PROD_PIPELINE_CONFIG = PipelineConfig(
    catalog="chi311",
    initial_load_days=365,  # Full year for production initial load
)
