"""Retry utilities with exponential backoff."""
import logging
import time
from collections.abc import Callable
from functools import wraps
from typing import Any

logger = logging.getLogger(__name__)


def retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_multiplier: float = 2.0,
    exceptions: tuple[type[Exception], ...] = (Exception,)
):
    """Decorator for retrying function with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        backoff_multiplier: Multiplier for exponential backoff
        exceptions: Tuple of exceptions to catch and retry

    Returns:
        Decorated function with retry logic

    Example:
        @retry_with_backoff(max_retries=3, initial_delay=2.0)
        def fetch_data():
            # Function that might fail
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            delay = initial_delay

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries - 1:
                        logger.error(
                            "%s: All %d retries exhausted. Final error: %s",
                            func.__name__,
                            max_retries,
                            e
                        )
                        raise

                    logger.warning(
                        "%s: Attempt %d/%d failed: %s. Retrying in %.1fs...",
                        func.__name__,
                        attempt + 1,
                        max_retries,
                        e,
                        delay
                    )
                    time.sleep(delay)
                    delay *= backoff_multiplier

            # Should never reach here
            raise RuntimeError("Retry logic error")

        return wrapper
    return decorator
