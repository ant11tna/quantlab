"""Data transformation utilities.

Handles adjustment factors, alignment, and cleaning.
"""

from __future__ import annotations

from typing import List, Optional, Union

import pandas as pd
import numpy as np
from loguru import logger


def apply_adjustments(
    df: pd.DataFrame,
    adj_factor_col: str = "adj_factor",
    price_cols: Optional[List[str]] = None
) -> pd.DataFrame:
    """Apply adjustment factors to price columns.
    
    Args:
        df: DataFrame with price data
        adj_factor_col: Column name for adjustment factor
        price_cols: Columns to adjust (default: open, high, low, close)
        
    Returns:
        DataFrame with adjusted prices
    """
    if adj_factor_col not in df.columns:
        logger.warning(f"Adjustment factor column '{adj_factor_col}' not found")
        return df
    
    if price_cols is None:
        price_cols = ["open", "high", "low", "close"]
    
    df = df.copy()
    current_factor = df[adj_factor_col].iloc[-1]
    
    for col in price_cols:
        if col in df.columns:
            df[f"{col}_raw"] = df[col]  # Keep raw prices
            df[col] = df[col] * df[adj_factor_col] / current_factor
    
    return df


def align_symbols(
    data_dict: dict[str, pd.DataFrame],
    date_col: str = "ts",
    method: str = "outer"
) -> pd.DataFrame:
    """Align multiple symbols to common date index.
    
    Args:
        data_dict: Dict of symbol -> DataFrame
        date_col: Date column name
        method: Join method (outer, inner, left, right)
        
    Returns:
        Aligned DataFrame with multi-column structure
    """
    # Create common date index
    all_dates = set()
    for df in data_dict.values():
        all_dates.update(df[date_col].values)
    
    common_index = pd.DatetimeIndex(sorted(all_dates))
    
    # Reindex each symbol
    aligned_data = {}
    for symbol, df in data_dict.items():
        df = df.set_index(date_col)
        df_aligned = df.reindex(common_index)
        
        # Prefix columns with symbol
        df_aligned.columns = [f"{symbol}_{c}" for c in df_aligned.columns]
        aligned_data[symbol] = df_aligned
    
    # Join all
    result = pd.concat(aligned_data.values(), axis=1)
    result.index.name = date_col
    result = result.reset_index()
    
    return result


def fill_missing(
    df: pd.DataFrame,
    method: str = "ffill",
    limit: Optional[int] = None
) -> pd.DataFrame:
    """Fill missing values in DataFrame.
    
    Args:
        df: Input DataFrame
        method: Fill method (ffill, bfill, interpolate)
        limit: Maximum consecutive NaNs to fill
        
    Returns:
        DataFrame with filled values
    """
    df = df.copy()
    
    if method == "ffill":
        df = df.ffill(limit=limit)
    elif method == "bfill":
        df = df.bfill(limit=limit)
    elif method == "interpolate":
        df = df.interpolate(limit=limit)
    else:
        raise ValueError(f"Unknown fill method: {method}")
    
    return df


def detect_outliers(
    df: pd.DataFrame,
    price_col: str = "close",
    zscore_threshold: float = 4.0
) -> pd.DataFrame:
    """Detect price outliers using Z-score.
    
    Args:
        df: DataFrame with price data
        price_col: Price column to check
        zscore_threshold: Z-score threshold for outlier detection
        
    Returns:
        DataFrame with outlier flags
    """
    df = df.copy()
    
    # Calculate returns
    df["returns"] = df[price_col].pct_change()
    
    # Calculate rolling Z-score
    mean = df["returns"].rolling(window=252, min_periods=30).mean()
    std = df["returns"].rolling(window=252, min_periods=30).std()
    df["zscore"] = (df["returns"] - mean) / std
    
    # Flag outliers
    df["is_outlier"] = df["zscore"].abs() > zscore_threshold
    
    outlier_count = df["is_outlier"].sum()
    if outlier_count > 0:
        logger.warning(f"Detected {outlier_count} outliers in {price_col}")
    
    return df


def resample_bars(
    df: pd.DataFrame,
    rule: str,
    date_col: str = "ts"
) -> pd.DataFrame:
    """Resample bar data to different frequency.
    
    Args:
        df: DataFrame with OHLCV data
        rule: Resampling rule (W=weekly, M=monthly, etc.)
        date_col: Date column name
        
    Returns:
        Resampled DataFrame
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.set_index(date_col)
    
    resampled = df.resample(rule).agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum"
    })
    
    resampled = resampled.dropna()
    resampled = resampled.reset_index()
    
    return resampled


def calculate_returns(
    df: pd.DataFrame,
    price_col: str = "close",
    periods: int = 1,
    log: bool = False
) -> pd.Series:
    """Calculate returns from price series.
    
    Args:
        df: DataFrame with price data
        price_col: Price column name
        periods: Return periods
        log: Use log returns
        
    Returns:
        Returns series
    """
    prices = df[price_col]
    
    if log:
        returns = np.log(prices / prices.shift(periods))
    else:
        returns = prices.pct_change(periods)
    
    return returns


# ============================================================================
# Trading Regime Transforms (Curated Schema Extensions)
# ============================================================================

def add_prev_close(df: pd.DataFrame, price_col: str = "close") -> pd.DataFrame:
    """Add previous close price column.
    
    Calculates prev_close per symbol. Used for limit up/down calculation.
    
    Args:
        df: DataFrame with OHLCV data (must have 'symbol' column)
        price_col: Price column to shift
        
    Returns:
        DataFrame with added 'prev_close' column
    """
    if price_col not in df.columns:
        raise ValueError(f"Price column '{price_col}' not found")
    
    df = df.copy()
    
    if "symbol" in df.columns:
        # Calculate per symbol
        df["prev_close"] = df.groupby("symbol")[price_col].shift(1)
    else:
        # Single symbol case
        df["prev_close"] = df[price_col].shift(1)
    
    return df


def add_limit_flags(
    df: pd.DataFrame,
    limit_pct: float = 0.10,
    st_limit_pct: float = 0.05,
    tolerance: float = 0.001,
    is_st: Optional[str] = None
) -> pd.DataFrame:
    """Add limit up/down flags based on price change from prev_close.
    
    Simplified version for A-share markets. Supports ST stocks with different limits.
    
    Args:
        df: DataFrame with 'prev_close' and OHLC columns
        limit_pct: Normal limit percentage (default 10% for most stocks)
        st_limit_pct: ST stock limit percentage (default 5%)
        tolerance: Tolerance for considering at limit (default 0.1%)
        is_st: Column name indicating ST status, or None to use limit_pct for all
        
    Returns:
        DataFrame with added 'is_limit_up' and 'is_limit_down' columns
    """
    required = ["prev_close", "close", "high", "low"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for limit calculation: {missing}")
    
    df = df.copy()
    
    # Determine limit pct per row (ST vs normal)
    if is_st and is_st in df.columns:
        limit = df[is_st].map({True: st_limit_pct, False: limit_pct})
    else:
        limit = limit_pct
    
    # Calculate price change
    price_change = (df["close"] - df["prev_close"]) / df["prev_close"]
    
    # Check if at limit (within tolerance)
    df["is_limit_up"] = (price_change >= limit - tolerance) & (df["close"] == df["high"])
    df["is_limit_down"] = (price_change <= -limit + tolerance) & (df["close"] == df["low"])
    
    # Handle NaN prev_close (first bar)
    df["is_limit_up"] = df["is_limit_up"].fillna(False)
    df["is_limit_down"] = df["is_limit_down"].fillna(False)
    
    return df


def add_suspension_flags(
    df: pd.DataFrame,
    volume_col: str = "volume",
    price_cols: Optional[List[str]] = None
) -> pd.DataFrame:
    """Add trading suspension flags.
    
    Simplified detection: volume==0 or any price missing indicates suspension.
    
    Args:
        df: DataFrame with OHLCV data
        volume_col: Volume column name
        price_cols: Price columns to check (default: open, high, low, close)
        
    Returns:
        DataFrame with added 'is_suspended' column
    """
    if price_cols is None:
        price_cols = ["open", "high", "low", "close"]
    
    df = df.copy()
    
    # Suspended if volume is 0 or any price is NaN
    volume_zero = (df[volume_col] == 0) | (df[volume_col].isna())
    price_missing = df[price_cols].isna().any(axis=1)
    
    df["is_suspended"] = volume_zero | price_missing
    
    return df


def add_trade_constraints(df: pd.DataFrame) -> pd.DataFrame:
    """Add trade constraint flags (can_buy, can_sell).
    
    Combines suspension and limit flags to determine tradability.
    
    Rules:
        - can_buy = not suspended AND not at limit up
        - can_sell = not suspended AND not at limit down
        
    Args:
        df: DataFrame with is_suspended, is_limit_up, is_limit_down columns
        
    Returns:
        DataFrame with added 'can_buy' and 'can_sell' columns
    """
    required = ["is_suspended", "is_limit_up", "is_limit_down"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for trade constraints: {missing}")
    
    df = df.copy()
    
    df["can_buy"] = (~df["is_suspended"]) & (~df["is_limit_up"])
    df["can_sell"] = (~df["is_suspended"]) & (~df["is_limit_down"])
    
    return df


def add_adj_factor(df: pd.DataFrame, adj_factor_col: Optional[str] = None) -> pd.DataFrame:
    """Add adjustment factor column.
    
    If source data has adjustment factor, use it. Otherwise fill with NaN.
    
    Args:
        df: DataFrame with price data
        adj_factor_col: Existing adjustment factor column, or None
        
    Returns:
        DataFrame with 'adj_factor' column
    """
    df = df.copy()
    
    if adj_factor_col and adj_factor_col in df.columns:
        df["adj_factor"] = df[adj_factor_col]
    else:
        # No adjustment factor available
        df["adj_factor"] = float("nan")
    
    return df


def apply_curated_transforms(
    df: pd.DataFrame,
    limit_pct: float = 0.10,
    st_limit_pct: float = 0.05,
    is_st_col: Optional[str] = None
) -> pd.DataFrame:
    """Apply all curated transforms in sequence.
    
    Pipeline:
        1. add_prev_close
        2. add_limit_flags
        3. add_suspension_flags
        4. add_trade_constraints
        5. add_adj_factor
        
    Args:
        df: Raw OHLCV DataFrame with 'symbol' column
        limit_pct: Normal limit percentage
        st_limit_pct: ST stock limit percentage
        is_st_col: Column indicating ST status
        
    Returns:
        Curated DataFrame with all regime columns
    """
    from quantlab.data.schema import reorder_columns
    
    df = df.copy()
    
    # Apply transforms in order
    df = add_prev_close(df)
    df = add_limit_flags(df, limit_pct, st_limit_pct, is_st=is_st_col)
    df = add_suspension_flags(df)
    df = add_trade_constraints(df)
    df = add_adj_factor(df)
    
    # Reorder to standard column order
    df = reorder_columns(df)
    
    return df
