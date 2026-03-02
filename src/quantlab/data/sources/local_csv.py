"""Local CSV file data source.

Reads price data from local CSV files.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
from loguru import logger

from quantlab.core.registry import register_data_source
from quantlab.data.sources.base import AbstractDataSource


@register_data_source("local_csv")
class LocalCSVDataSource(AbstractDataSource):
    """Data source for local CSV files.
    
    Expected CSV format:
        date,open,high,low,close,volume,adj_close
        2020-01-02,100.0,101.0,99.0,100.5,1000000,100.5
        ...
    """
    
    def __init__(
        self, 
        data_dir: str | Path,
        date_column: str = "date",
        file_pattern: str = "{symbol}.csv"
    ) -> None:
        """Initialize local CSV data source.
        
        Args:
            data_dir: Directory containing CSV files
            date_column: Name of date column
            file_pattern: Pattern for CSV filenames
        """
        super().__init__("local_csv")
        self.data_dir = Path(data_dir)
        self.date_column = date_column
        self.file_pattern = file_pattern
        self._available_symbols: Optional[List[str]] = None
    
    def _get_file_path(self, symbol: str) -> Path:
        """Get path to CSV file for symbol."""
        filename = self.file_pattern.format(symbol=symbol)
        return self.data_dir / filename
    
    def _read_csv(self, symbol: str) -> pd.DataFrame:
        """Read CSV file for symbol."""
        path = self._get_file_path(symbol)
        if not path.exists():
            raise FileNotFoundError(f"Data file not found: {path}")
        
        df = pd.read_csv(path, parse_dates=[self.date_column])
        df = df.rename(columns={self.date_column: "ts"})
        df = df.sort_values("ts")
        df["symbol"] = symbol
        return df
    
    def get_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        frequency: str = "daily"
    ) -> pd.DataFrame:
        """Get OHLCV bars for symbol."""
        cache_key = f"{symbol}_{frequency}"
        
        if cache_key not in self._cache:
            self._cache[cache_key] = self._read_csv(symbol)
        
        df = self._cache[cache_key]
        mask = (df["ts"] >= start) & (df["ts"] <= end)
        return df[mask].copy()
    
    def get_symbols(self) -> List[str]:
        """Get available symbols from directory."""
        if self._available_symbols is None:
            pattern = self.file_pattern.format(symbol="*")
            files = list(self.data_dir.glob(pattern))
            # Extract symbol from filename
            prefix = self.file_pattern.split("{")[0]
            suffix = self.file_pattern.split("}")[1] if "}" in self.file_pattern else ""
            self._available_symbols = [
                f.name.replace(prefix, "").replace(suffix, "")
                for f in files
            ]
        return self._available_symbols


@register_data_source("mock")
class MockDataSource(AbstractDataSource):
    """Mock data source for testing.
    
    Generates synthetic price data.
    """
    
    def __init__(
        self,
        symbols: List[str] = None,
        start_price: float = 100.0,
        volatility: float = 0.02
    ) -> None:
        super().__init__("mock")
        self.symbols = symbols or ["AAPL", "MSFT", "GOOGL"]
        self.start_price = start_price
        self.volatility = volatility
    
    def get_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        frequency: str = "daily"
    ) -> pd.DataFrame:
        """Generate mock price data."""
        import numpy as np
        
        # Generate date range
        dates = pd.date_range(start=start, end=end, freq="D")
        dates = dates[dates.weekday < 5]  # Only weekdays
        
        n = len(dates)
        np.random.seed(hash(symbol) % 2**32)  # Reproducible per symbol
        
        # Generate random walk
        returns = np.random.normal(0.0001, self.volatility, n)
        prices = self.start_price * np.exp(np.cumsum(returns))
        
        # Generate OHLC from close
        df = pd.DataFrame({
            "ts": dates,
            "symbol": symbol,
            "close": prices,
        })
        
        # Derive OHLC
        df["open"] = df["close"].shift(1) * (1 + np.random.normal(0, 0.001, n))
        df["high"] = df[["open", "close"]].max(axis=1) * (1 + abs(np.random.normal(0, 0.005, n)))
        df["low"] = df[["open", "close"]].min(axis=1) * (1 - abs(np.random.normal(0, 0.005, n)))
        df["volume"] = np.random.randint(1000000, 10000000, n)
        
        df = df.dropna()
        return df
    
    def get_symbols(self) -> List[str]:
        """Get mock symbols."""
        return self.symbols
