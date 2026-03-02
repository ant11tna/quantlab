"""Base data source interface.

All data sources must implement this interface for consistent data access.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Protocol

import pandas as pd

from quantlab.core.types import Bar


class DataSource(Protocol):
    """Protocol for data sources."""
    
    def get_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        frequency: str = "daily"
    ) -> pd.DataFrame:
        """Get OHLCV bars for a symbol.
        
        Args:
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            frequency: Data frequency (daily, hourly, etc.)
            
        Returns:
            DataFrame with columns: ts, open, high, low, close, volume
        """
        ...
    
    def get_symbols(self) -> List[str]:
        """Get list of available symbols."""
        ...
    
    def get_last_price(self, symbol: str) -> float:
        """Get last available price for symbol."""
        ...


class AbstractDataSource(ABC):
    """Abstract base class for data sources."""
    
    def __init__(self, name: str) -> None:
        self.name = name
        self._cache: dict = {}
    
    @abstractmethod
    def get_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        frequency: str = "daily"
    ) -> pd.DataFrame:
        """Get OHLCV bars for a symbol."""
        raise NotImplementedError
    
    @abstractmethod
    def get_symbols(self) -> List[str]:
        """Get list of available symbols."""
        raise NotImplementedError
    
    def get_last_price(self, symbol: str) -> float:
        """Get last available price."""
        bars = self.get_bars(symbol, 
                            datetime(1900, 1, 1), 
                            datetime(2100, 12, 31))
        if bars.empty:
            raise ValueError(f"No data for {symbol}")
        return float(bars["close"].iloc[-1])
    
    def get_multiple_bars(
        self,
        symbols: List[str],
        start: datetime,
        end: datetime,
        frequency: str = "daily"
    ) -> dict[str, pd.DataFrame]:
        """Get bars for multiple symbols."""
        return {
            symbol: self.get_bars(symbol, start, end, frequency)
            for symbol in symbols
        }
