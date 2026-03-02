"""Curated market data schema definitions and validation.

Defines the contract for curated OHLCV data including trading regime fields.
"""

from __future__ import annotations

from typing import Dict, List, Set
import pandas as pd
from loguru import logger


# Schema versions
SCHEMA_RAW_MINIMAL = "raw_minimal"  # Basic OHLCV only
SCHEMA_CURATED_V1 = "curated_v1"    # Full trading regime support

# Column definitions
REQUIRED_COLUMNS_BASE: List[str] = [
    "ts",       # Timestamp (datetime64)
    "symbol",   # Trading symbol
    "open",     # Open price
    "high",     # High price
    "low",      # Low price
    "close",    # Close price
    "volume",   # Trading volume
]

# Curated schema extensions for trading regime support
REQUIRED_COLUMNS_CURATED: List[str] = REQUIRED_COLUMNS_BASE + [
    "prev_close",       # Previous close price (for limit calculation)
    "is_suspended",     # Trading suspended (bool)
    "is_limit_up",      # At limit up (bool)
    "is_limit_down",    # At limit down (bool)
    "can_buy",          # Can buy this bar (bool)
    "can_sell",         # Can sell this bar (bool)
    "adj_factor",       # Adjustment factor (float, NaN if N/A)
]

# Expected dtypes (flexible for int/float)
EXPECTED_DTYPES: Dict[str, type] = {
    "ts": pd.Timestamp,
    "symbol": str,
    "open": (int, float),
    "high": (int, float),
    "low": (int, float),
    "close": (int, float),
    "volume": (int, float),
    "prev_close": (int, float),
    "is_suspended": bool,
    "is_limit_up": bool,
    "is_limit_down": bool,
    "can_buy": bool,
    "can_sell": bool,
    "adj_factor": (int, float),
}


def validate_bars_df(df: pd.DataFrame, strict: bool = False) -> tuple[bool, str, str]:
    """Validate bars DataFrame against schema contract.
    
    Args:
        df: Input DataFrame to validate
        strict: If True, requires all curated columns. If False, only base columns.
        
    Returns:
        Tuple of (is_valid: bool, schema_version: str, message: str)
        
    Examples:
        >>> valid, version, msg = validate_bars_df(df, strict=False)
        >>> if not valid:
        ...     raise ValueError(msg)
    """
    if df.empty:
        return False, "invalid", "Empty DataFrame"
    
    required = REQUIRED_COLUMNS_CURATED if strict else REQUIRED_COLUMNS_BASE
    missing = [col for col in required if col not in df.columns]
    
    if missing:
        if strict:
            # In strict mode, missing curated columns = fallback to raw
            base_missing = [col for col in REQUIRED_COLUMNS_BASE if col not in df.columns]
            if base_missing:
                return False, "invalid", f"Missing base columns: {base_missing}"
            return True, SCHEMA_RAW_MINIMAL, f"Curated columns missing: {missing}"
        else:
            return False, "invalid", f"Missing columns: {missing}"
    
    # Check ts is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["ts"]):
        return False, "invalid", "Column 'ts' must be datetime64 type"
    
    # Check numeric columns are numeric
    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        if not pd.api.types.is_numeric_dtype(df[col]):
            return False, "invalid", f"Column '{col}' must be numeric"
    
    # Schema version detection
    if all(col in df.columns for col in REQUIRED_COLUMNS_CURATED):
        return True, SCHEMA_CURATED_V1, "Valid curated schema v1"
    else:
        return True, SCHEMA_RAW_MINIMAL, "Valid raw minimal schema"


def infer_schema_version(df: pd.DataFrame) -> str:
    """Infer schema version from DataFrame columns.
    
    Returns:
        Schema version string (raw_minimal or curated_v1)
    """
    if all(col in df.columns for col in REQUIRED_COLUMNS_CURATED):
        return SCHEMA_CURATED_V1
    elif all(col in df.columns for col in REQUIRED_COLUMNS_BASE):
        return SCHEMA_RAW_MINIMAL
    else:
        return "invalid"


def get_missing_columns(df: pd.DataFrame, target_schema: str = SCHEMA_CURATED_V1) -> List[str]:
    """Get list of missing columns to reach target schema.
    
    Args:
        df: Input DataFrame
        target_schema: Target schema version
        
    Returns:
        List of missing column names
    """
    if target_schema == SCHEMA_CURATED_V1:
        required = REQUIRED_COLUMNS_CURATED
    else:
        required = REQUIRED_COLUMNS_BASE
    
    return [col for col in required if col not in df.columns]


def ensure_schema_columns(df: pd.DataFrame, schema: str = SCHEMA_CURATED_V1) -> pd.DataFrame:
    """Ensure DataFrame has all schema columns, adding missing ones with defaults.
    
    This is a convenience function for gradual migration - it adds missing columns
    with sensible defaults so code doesn't break, but logs warnings.
    
    Args:
        df: Input DataFrame
        schema: Target schema version
        
    Returns:
        DataFrame with all schema columns present
    """
    df = df.copy()
    
    # Add missing curated columns with defaults
    defaults = {
        "prev_close": float("nan"),
        "is_suspended": False,
        "is_limit_up": False,
        "is_limit_down": False,
        "can_buy": True,
        "can_sell": True,
        "adj_factor": float("nan"),
    }
    
    missing = get_missing_columns(df, schema)
    for col in missing:
        if col in defaults:
            df[col] = defaults[col]
            logger.warning(f"Added missing column '{col}' with default value")
    
    return df


# Column ordering for consistent output
COLUMN_ORDER_CURATED: List[str] = [
    "ts", "symbol",
    "open", "high", "low", "close", "volume",
    "prev_close",
    "is_suspended", "is_limit_up", "is_limit_down",
    "can_buy", "can_sell",
    "adj_factor",
]


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder DataFrame columns to standard curated order.
    
    Keeps any extra columns at the end.
    """
    # Get curated columns that exist in df, in order
    curated_cols = [c for c in COLUMN_ORDER_CURATED if c in df.columns]
    # Get any extra columns
    extra_cols = [c for c in df.columns if c not in set(COLUMN_ORDER_CURATED)]
    
    return df[curated_cols + extra_cols]
