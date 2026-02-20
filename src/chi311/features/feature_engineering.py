"""
Feature engineering for Prophet forecasting.
Generates temporal, lag and rolling features from Gold daily summaries.
"""
import pandas as pd
import numpy as np

def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add temporal features to daily summary DataFrame."""
    df = df.copy()
    df["day_of_week"] = df["ds"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    df["month"] = df["ds"].dt.month
    df["day_of_month"] = df["ds"].dt.day
    df["week_of_year"] = df["ds"].dt.isocalendar().week.astype(int)
    df["is_month_start"] = df["ds"].dt.is_month_start.astype(int)
    df["is_month_end"] = df["ds"].dt.is_month_end.astype(int)
    return df

def add_lag_features(df: pd.DataFrame, lags: list = None) -> pd.DataFrame:
    """Add lag features for time series"""
    if lags is None:
        lags = [1, 7, 14, 28]
    df = df.copy()
    for lag in lags:
        df[f"lag_{lag}"] = df["y"].shift(lag)
    return df

def add_rolling_features(df: pd.DataFrame, windows: list = None) -> pd.DataFrame:
    """Add rolling mean and std features"""
    if windows is None:
        windows = [7, 14, 30]
    df = df.copy()
    for w in windows:
        df[f"rolling_mean_{w}d"] = df["y"].rolling(w, min_periods=1).mean()
        df[f"rolling_std_{w}d"] = df["y"].rolling(w, min_periods=1).std()

def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    "Full feature engineering pipeline"
    df = add_temporal_features(df)
    df = add_lag_features(df)
    df = add_rolling_features(df)
    return df