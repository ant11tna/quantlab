"""Utility functions for market data module."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import numpy as np


def now_utc_iso() -> str:
    """Return current UTC time as ISO8601 string."""
    return datetime.now(timezone.utc).isoformat()


def ensure_dirs(path: Path) -> Path:
    """Ensure parent directories exist, return path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def coerce_ts(df: pd.DataFrame, col: str = "ts") -> pd.DataFrame:
    """Coerce timestamp column to pandas datetime.
    
    Args:
        df: DataFrame with timestamp column
        col: Column name to coerce (default: "ts")
        
    Returns:
        DataFrame with coerced timestamp column
    """
    df = df.copy()
    if col in df.columns:
        # Handle various date formats
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def schema_empty_df(cols: dict[str, str]) -> pd.DataFrame:
    """Create empty DataFrame with specified schema.
    
    Args:
        cols: Dictionary of column name -> dtype
        
    Returns:
        Empty DataFrame with correct dtypes
    """
    df = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in cols.items()})
    return df


def validate_curated_df(df: pd.DataFrame) -> tuple[bool, str]:
    """Validate curated DataFrame has required columns.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        (is_valid, error_message)
    """
    from .types import REQUIRED_CURATED_COLS
    
    missing = [col for col in REQUIRED_CURATED_COLS if col not in df.columns]
    if missing:
        return False, f"Missing required columns: {missing}"
    
    if df.empty:
        return False, "Empty DataFrame"
    
    # Check for nulls in required columns
    for col in REQUIRED_CURATED_COLS:
        if df[col].isna().all():
            return False, f"Column '{col}' has all null values"
    
    return True, "OK"


def generate_part_filename(listing_id: str, ts: datetime) -> str:
    """Generate partition filename.
    
    Format: part-<YYYYMMDDHHMMSS>-<listing_id_hash>.parquet
    
    Args:
        listing_id: Listing ID
        ts: Timestamp for the partition
        
    Returns:
        Filename string
    """
    ts_str = ts.strftime("%Y%m%d%H%M%S")
    # Use hash of listing_id for uniqueness
    listing_hash = hex(hash(listing_id) & 0xFFFFFFFF)[2:8]
    return f"part-{ts_str}-{listing_hash}.parquet"


def dedupe_by_ts(df: pd.DataFrame, subset: str = "ts") -> pd.DataFrame:
    """Deduplicate DataFrame by timestamp, keeping last.
    
    Args:
        df: DataFrame with timestamp column
        subset: Column to deduplicate on
        
    Returns:
        Deduplicated DataFrame
    """
    if subset not in df.columns:
        return df
    return df.drop_duplicates(subset=[subset], keep="last")
