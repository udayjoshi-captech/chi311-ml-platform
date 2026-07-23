"""DataFrame conversion and validation utilities."""
import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def pandas_to_spark(pdf: pd.DataFrame, spark_session: Any) -> Any:
    """Convert Pandas DataFrame to Spark DataFrame with error handling.

    Args:
        pdf: Pandas DataFrame to convert
        spark_session: Active SparkSession

    Returns:
        Spark DataFrame

    Raises:
        ValueError: If conversion fails due to schema issues
        ConnectionError: If SparkSession is not active
    """
    try:
        spark_session.catalog.currentDatabase()
    except Exception as e:
        raise ConnectionError(f"SparkSession is not active: {e}") from e

    try:
        return spark_session.createDataFrame(pdf)
    except Exception as e:
        logger.exception(
            "Failed to convert Pandas to Spark DataFrame. Schema: %s",
            pdf.dtypes,
        )
        raise ValueError(f"DataFrame conversion failed: {e}") from e


def spark_to_pandas(sdf: Any) -> pd.DataFrame:
    """Convert Spark DataFrame to Pandas DataFrame safely.

    Args:
        sdf: Spark DataFrame to convert

    Returns:
        Pandas DataFrame

    Raises:
        ValueError: If conversion fails
    """
    try:
        pdf = sdf.toPandas()
        logger.debug("Converted Spark DataFrame to Pandas: %d rows", len(pdf))
        return pdf
    except Exception as e:
        logger.exception("Failed to convert Spark to Pandas DataFrame")
        raise ValueError(f"DataFrame conversion failed: {e}") from e


def validate_schema(
    df: pd.DataFrame,
    required_columns: list[str],
    name: str | None = None
) -> None:
    """Validate DataFrame has required columns.

    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        name: Optional name for error messages

    Raises:
        ValueError: If required columns are missing
    """
    missing = set(required_columns) - set(df.columns)
    if missing:
        name_str = f" '{name}'" if name else ""
        raise ValueError(
            f"DataFrame{name_str} missing required columns: {missing}. "
            f"Available: {list(df.columns)}"
        )

    logger.debug("Schema validation passed for DataFrame%s", f" '{name}'" if name else "")
