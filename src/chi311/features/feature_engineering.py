"""
Feature engineering for Prophet forecasting.
Generates temporal, lag and rolling features from Gold daily summaries.
"""

import logging
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add temporal features from datetime column 'ds'.

    Args:
        df: DataFrame with 'ds' column (datetime type)

    Returns:
        DataFrame with 7 additional temporal features

    Raises:
        ValueError: If 'ds' column is missing or DataFrame is empty
        TypeError: If 'ds' column is not datetime type
    """
    if df.empty:
        raise ValueError("Cannot add temporal features to empty DataFrame")

    if "ds" not in df.columns:
        raise ValueError(
            f"Missing required column: 'ds'. Available columns: {list(df.columns)}"
        )

    if not pd.api.types.is_datetime64_any_dtype(df["ds"]):
        raise TypeError(
            f"Column 'ds' must be datetime type, got {df['ds'].dtype}. "
            "Convert with: df['ds'] = pd.to_datetime(df['ds'])"
        )

    df = df.copy()
    df["day_of_week"] = df["ds"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    df["month"] = df["ds"].dt.month
    df["day_of_month"] = df["ds"].dt.day
    df["week_of_year"] = df["ds"].dt.isocalendar().week.astype(int)
    df["is_month_start"] = df["ds"].dt.is_month_start.astype(int)
    df["is_month_end"] = df["ds"].dt.is_month_end.astype(int)
    logger.info("add_temporal_features: %d rows, 7 features added", len(df))
    return df


def add_lag_features(
    df: pd.DataFrame,
    lags: Optional[List[int]] = None
) -> pd.DataFrame:
    """Add lag features for time series forecasting.

    Args:
        df: DataFrame with 'y' column (numeric)
        lags: List of lag periods in days (default: [1, 7, 14, 28])

    Returns:
        DataFrame with lag_{n} columns added

    Raises:
        ValueError: If 'y' column missing, DataFrame empty, or invalid lags
        TypeError: If 'y' column is non-numeric or lags are non-integers
    """
    if lags is None:
        lags = [1, 7, 14, 28]

    if df.empty:
        raise ValueError("Cannot add lag features to empty DataFrame")

    if "y" not in df.columns:
        raise ValueError(
            f"Missing required column: 'y'. Available columns: {list(df.columns)}"
        )

    if not pd.api.types.is_numeric_dtype(df["y"]):
        raise TypeError(
            f"Column 'y' must be numeric type, got {df['y'].dtype}"
        )

    if not all(isinstance(lag, int) and lag > 0 for lag in lags):
        raise ValueError(
            f"All lags must be positive integers, got {lags}"
        )

    df = df.copy()
    for lag in lags:
        df[f"lag_{lag}"] = df["y"].shift(lag)
    logger.info("add_lag_features: %d rows, lags=%s", len(df), lags)
    return df


def add_rolling_features(df: pd.DataFrame, windows: list = None) -> pd.DataFrame:
    """Add rolling mean and std features"""
    if windows is None:
        windows = [7, 14, 30]
    df = df.copy()
    for w in windows:
        df[f"rolling_mean_{w}d"] = df["y"].rolling(w, min_periods=1).mean()
        df[f"rolling_std_{w}d"] = df["y"].rolling(w, min_periods=1).std()
    logger.info("add_rolling_features: %d rows, windows=%s", len(df), windows)
    return df


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    "Full feature engineering pipeline"
    logger.info("prepare_features: input %d rows", len(df))
    df = add_temporal_features(df)
    df = add_lag_features(df)
    df = add_rolling_features(df)
    logger.info(
        "prepare_features: output %d rows, %d columns", len(df), len(df.columns)
    )
    return df
