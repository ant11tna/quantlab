"""AKShare data source for Chinese market data.

Provides access to Chinese stocks, futures, and fund data through AKShare.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
from loguru import logger

try:
    import akshare as ak
except ImportError:
    raise ImportError(
        "akshare is required. Install with: pip install akshare"
    )

from quantlab.core.registry import register_data_source
from quantlab.data.sources.base import AbstractDataSource


@register_data_source("akshare")
class AKShareDataSource(AbstractDataSource):
    """AKShare data source for Chinese markets.
    
    Supports:
    - A-shares (stock_zh_a)
    - Indices (stock_zh_index)
    - ETFs (fund_etf_category)
    - Futures (futures_zh_)
    
    Example:
        source = AKShareDataSource()
        df = source.get_bars("000001", start, end)  # 平安银行
    """
    
    def __init__(self, market: str = "stock_zh_a") -> None:
        """Initialize AKShare data source.
        
        Args:
            market: Market type (stock_zh_a, stock_zh_index, futures_zh, etc.)
        """
        super().__init__("akshare")
        self.market = market
        
        # Symbol suffix mapping
        self._exchange_suffix = {
            "sh": ".SH",  # Shanghai
            "sz": ".SZ",  # Shenzhen
            "bj": ".BJ",  # Beijing
        }
    
    def _normalize_symbol(self, symbol: str) -> str:
        """Normalize symbol for AKShare.
        
        AKShare uses format like "000001" without exchange suffix.
        We handle both "000001.SZ" and "000001" formats.
        """
        # Remove suffix if present
        if "." in symbol:
            symbol = symbol.split(".")[0]
        return symbol
    
    def get_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        frequency: str = "daily"
    ) -> pd.DataFrame:
        """Get OHLCV bars for a symbol.
        
        Args:
            symbol: Stock code (e.g., "000001" or "000001.SZ")
            start: Start datetime
            end: End datetime
            frequency: Data frequency (daily, weekly, monthly)
            
        Returns:
            DataFrame with columns: ts, open, high, low, close, volume
        """
        symbol_clean = self._normalize_symbol(symbol)
        
        logger.info(f"Fetching {symbol_clean} from {start.date()} to {end.date()}")
        
        try:
            # Map frequency to AKShare parameter
            period_map = {
                "daily": "daily",
                "D": "daily",
                "weekly": "weekly",
                "W": "weekly",
                "monthly": "monthly",
                "M": "monthly",
            }
            period = period_map.get(frequency, "daily")
            
            # Fetch data from AKShare
            # stock_zh_a_hist uses symbol without exchange prefix for most stocks
            df = ak.stock_zh_a_hist(
                symbol=symbol_clean,
                period=period,
                start_date=start.strftime("%Y%m%d"),
                end_date=end.strftime("%Y%m%d"),
                adjust="qfq"  # 前复权
            )
            
            if df.empty:
                logger.warning(f"No data returned for {symbol_clean}")
                return pd.DataFrame()
            
            # Standardize column names
            df = df.rename(columns={
                "日期": "ts",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
            })
            
            # Convert timestamp
            df["ts"] = pd.to_datetime(df["ts"])
            df["symbol"] = symbol_clean
            
            # Select and order columns
            df = df[["ts", "symbol", "open", "high", "low", "close", "volume"]]
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to fetch {symbol_clean}: {e}")
            return pd.DataFrame()
    
    def get_symbols(self) -> List[str]:
        """Get list of available A-share symbols."""
        try:
            # Get stock list
            df = ak.stock_zh_a_spot_em()
            return df["代码"].tolist()
        except Exception as e:
            logger.error(f"Failed to get symbol list: {e}")
            return []
    
    def get_stock_info(self, symbol: str) -> dict:
        """Get stock basic information.
        
        Args:
            symbol: Stock code
            
        Returns:
            Dict with stock info
        """
        symbol_clean = self._normalize_symbol(symbol)
        
        try:
            # Get real-time quote which includes basic info
            df = ak.stock_zh_a_spot_em()
            info = df[df["代码"] == symbol_clean]
            
            if info.empty:
                return {}
            
            row = info.iloc[0]
            return {
                "symbol": symbol_clean,
                "name": row.get("名称", ""),
                "industry": row.get("行业", ""),
                "market_cap": row.get("总市值", 0),
            }
        except Exception as e:
            logger.error(f"Failed to get info for {symbol_clean}: {e}")
            return {}
    
    def get_index_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime
    ) -> pd.DataFrame:
        """Get index data (e.g., 上证指数 "000001")."""
        symbol_clean = self._normalize_symbol(symbol)
        
        try:
            df = ak.index_zh_a_hist(
                symbol=symbol_clean,
                period="daily",
                start_date=start.strftime("%Y%m%d"),
                end_date=end.strftime("%Y%m%d")
            )
            
            if df.empty:
                return pd.DataFrame()
            
            df = df.rename(columns={
                "日期": "ts",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
            })
            
            df["ts"] = pd.to_datetime(df["ts"])
            df["symbol"] = f"{symbol_clean}.INDEX"
            
            return df[["ts", "symbol", "open", "high", "low", "close", "volume"]]
            
        except Exception as e:
            logger.error(f"Failed to fetch index {symbol_clean}: {e}")
            return pd.DataFrame()


def fetch_multiple_stocks(
    symbols: List[str],
    start: datetime,
    end: datetime,
    progress: bool = True
) -> pd.DataFrame:
    """Fetch data for multiple stocks and combine.
    
    Args:
        symbols: List of stock codes
        start: Start date
        end: End date
        progress: Show progress bar
        
    Returns:
        Combined DataFrame
    """
    source = AKShareDataSource()
    all_data = []
    
    iterator = symbols
    if progress:
        from tqdm import tqdm
        iterator = tqdm(symbols, desc="Fetching stocks")
    
    for symbol in iterator:
        df = source.get_bars(symbol, start, end)
        if not df.empty:
            all_data.append(df)
    
    if not all_data:
        return pd.DataFrame()
    
    return pd.concat(all_data, ignore_index=True)


# Common index symbols
INDEX_SYMBOLS = {
    "上证指数": "000001",
    "深证成指": "399001",
    "创业板指": "399006",
    "科创50": "000688",
    "沪深300": "000300",
    "中证500": "000905",
    "中证1000": "000852",
}

# Example ETFs
ETF_SYMBOLS = {
    "沪深300ETF": "510300",
    "中证500ETF": "510500",
    "创业板ETF": "159915",
    "科创50ETF": "588000",
}
