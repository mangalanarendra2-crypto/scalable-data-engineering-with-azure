"""
src/utils/helpers.py
--------------------
Shared helper utilities: retry logic, date helpers, schema validation,
partitioning helpers, and Azure path builders.
"""

from __future__ import annotations

import functools
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable, Generator, Iterable, Iterator, TypeVar

from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.utils.logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Retry helpers
# ---------------------------------------------------------------------------

def with_retry(
    max_attempts: int = 3,
    min_wait: float = 1.0,
    max_wait: float = 60.0,
    exceptions: tuple = (Exception,),
) -> Callable:
    """Decorator that adds exponential-backoff retry to any function."""
    def decorator(func: Callable) -> Callable:
        @retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
            retry=retry_if_exception_type(exceptions),
            reraise=True,
        )
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)
        return wrapper
    return decorator


def retry_on_transient(func: Callable) -> Callable:
    """Pre-configured retry for transient Azure errors."""
    return with_retry(
        max_attempts=5,
        min_wait=2.0,
        max_wait=120.0,
        exceptions=(ConnectionError, TimeoutError, OSError),
    )(func)


# ---------------------------------------------------------------------------
# Date & partition helpers
# ---------------------------------------------------------------------------

def date_range(start: date, end: date) -> Generator[date, None, None]:
    """Yield each date from start to end inclusive."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def partition_path(base: str, dt: date | datetime, **extra: str) -> str:
    """
    Build a Hive-style partition path.

    Example:
        partition_path("data/events", date(2026,5,4), region="us-east")
        → "data/events/year=2026/month=05/day=04/region=us-east"
    """
    if isinstance(dt, datetime):
        dt = dt.date()
    parts = [
        base,
        f"year={dt.year:04d}",
        f"month={dt.month:02d}",
        f"day={dt.day:02d}",
    ]
    for k, v in extra.items():
        parts.append(f"{k}={v}")
    return "/".join(parts)


def adls_path(container: str, *path_parts: str) -> str:
    """Build an ADLS Gen2 abfss:// path."""
    path = "/".join(p.strip("/") for p in path_parts if p)
    return f"abfss://{container}@{{account}}.dfs.core.windows.net/{path}"


# ---------------------------------------------------------------------------
# Batch helpers
# ---------------------------------------------------------------------------

def chunked(iterable: Iterable[T], size: int) -> Iterator[list[T]]:
    """Split an iterable into chunks of given size."""
    chunk: list[T] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


# ---------------------------------------------------------------------------
# Timing / profiling
# ---------------------------------------------------------------------------

class Timer:
    """Context manager for measuring elapsed time."""

    def __init__(self, label: str = "operation") -> None:
        self.label = label
        self.elapsed: float = 0.0

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_: Any) -> None:
        self.elapsed = time.perf_counter() - self._start
        logger.info("timer", label=self.label, elapsed_seconds=round(self.elapsed, 3))


# ---------------------------------------------------------------------------
# Schema / type utilities
# ---------------------------------------------------------------------------

def flatten_dict(d: dict, parent_key: str = "", sep: str = ".") -> dict:
    """Recursively flatten a nested dict."""
    items: list = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def safe_cast(value: Any, target_type: type, default: Any = None) -> Any:
    """Safely cast a value to target_type, returning default on failure."""
    try:
        return target_type(value)
    except (ValueError, TypeError):
        return default
