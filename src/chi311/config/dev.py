"""Development environment configuration."""
from .base import APIConfig, TrainingConfig, PipelineConfig


# Override defaults for development
DEV_API_CONFIG = APIConfig(
    timeout=60,  # Longer timeout for debugging
    max_retries=5,
)

DEV_TRAINING_CONFIG = TrainingConfig(
    min_training_points=5,  # Allow smaller datasets for testing
    validation_split=0.3,  # More validation data for dev
)

DEV_PIPELINE_CONFIG = PipelineConfig(
    catalog="chi311_dev",  # Separate dev catalog
    initial_load_days=30,  # Smaller initial load for dev
)
