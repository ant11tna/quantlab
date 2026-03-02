"""Time utilities for trading operations.

Handles timezones, trading calendars, and timestamp alignment.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Optional, Union
import zoneinfo

import pandas as pd
from exchange_calendars import get_calendar


class TradingCalendar:
    """Trading calendar wrapper."""
    
    def __init__(self, exchange: str = "XNYS") -> None:
        """Initialize trading calendar.
        
        Args:
            exchange: Exchange code (XNYS=NYSE, XSHG=Shanghai, etc.)
        """
        self.exchange = exchange
        self._calendar = get_calendar(exchange)
    
    def is_trading_day(self, date: Union[datetime, pd.Timestamp]) -> bool:
        """Check if date is a trading day."""
        ts = pd.Timestamp(date)
        return self._calendar.is_session(ts)
    
    def next_trading_day(
        self, 
        date: Union[datetime, pd.Timestamp],
        n: int = 1
    ) -> pd.Timestamp:
        """Get next trading day."""
        ts = pd.Timestamp(date)
        return self._calendar.session_offset(ts, n)
    
    def previous_trading_day(
        self, 
        date: Union[datetime, pd.Timestamp],
        n: int = 1
    ) -> pd.Timestamp:
        """Get previous trading day."""
        return self.next_trading_day(date, -n)
    
    def trading_days_between(
        self,
        start: Union[datetime, pd.Timestamp],
        end: Union[datetime, pd.Timestamp]
    ) -> pd.DatetimeIndex:
        """Get all trading days between start and end (inclusive)."""
        return self._calendar.sessions_in_range(
            pd.Timestamp(start), 
            pd.Timestamp(end)
        )
    
    def get_rebalance_dates(
        self,
        start: Union[datetime, pd.Timestamp],
        end: Union[datetime, pd.Timestamp],
        freq: str = "M"  # M=monthly, Q=quarterly, W=weekly
    ) -> pd.DatetimeIndex:
        """Get rebalance dates at given frequency.
        
        Args:
            start: Start date
            end: End date
            freq: Frequency - M (monthly), Q (quarterly), W (weekly)
        
        Returns:
            DatetimeIndex of rebalance dates
        """
        # Get all sessions first
        sessions = self.trading_days_between(start, end)
        
        # Group by period and take first/last trading day
        if freq == "M":
            grouper = sessions.to_period("M")
        elif freq == "Q":
            grouper = sessions.to_period("Q")
        elif freq == "W":
            grouper = sessions.to_period("W")
        else:
            raise ValueError(f"Unsupported frequency: {freq}")
        
        # Return last trading day of each period
        result = sessions.to_series().groupby(grouper).last()
        return pd.DatetimeIndex(result.values)


class TimezoneHandler:
    """Handle timezone conversions."""
    
    UTC = zoneinfo.ZoneInfo("UTC")
    NY = zoneinfo.ZoneInfo("America/New_York")
    SHANGHAI = zoneinfo.ZoneInfo("Asia/Shanghai")
    
    @classmethod
    def normalize(
        cls, 
        dt: datetime, 
        tz: Optional[Union[str, zoneinfo.ZoneInfo]] = None
    ) -> datetime:
        """Normalize datetime to timezone.
        
        If dt has no timezone, assumes it's in the target timezone.
        """
        if tz is None:
            tz = cls.UTC
        if isinstance(tz, str):
            tz = zoneinfo.ZoneInfo(tz)
        
        if dt.tzinfo is None:
            return dt.replace(tzinfo=tz)
        return dt.astimezone(tz)
    
    @classmethod
    def to_utc(cls, dt: datetime) -> datetime:
        """Convert datetime to UTC."""
        return cls.normalize(dt, cls.UTC)
    
    @classmethod
    def align_to_market_close(
        cls,
        dt: datetime,
        calendar: TradingCalendar,
        hour: int = 16,
        minute: int = 0
    ) -> datetime:
        """Align timestamp to market close time."""
        tz = dt.tzinfo or cls.NY
        return dt.replace(hour=hour, minute=minute, second=0, microsecond=0, tzinfo=tz)


def align_timestamps(
    timestamps: List[datetime],
    freq: str = "D",
    calendar: Optional[TradingCalendar] = None
) -> List[datetime]:
    """Align timestamps to regular intervals.
    
    Args:
        timestamps: List of timestamps
        freq: Frequency string (D=daily, H=hourly)
        calendar: Optional trading calendar for business day alignment
    
    Returns:
        Aligned timestamps
    """
    if calendar:
        # Use trading days
        start, end = min(timestamps), max(timestamps)
        aligned = calendar.trading_days_between(start, end)
        return aligned.tolist()
    else:
        # Use pandas date_range
        start, end = min(timestamps), max(timestamps)
        aligned = pd.date_range(start=start, end=end, freq=freq)
        return aligned.tolist()
