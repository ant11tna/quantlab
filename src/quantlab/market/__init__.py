"""Market data module - Slice 1: Local curated storage.

Usage:
    from quantlab.market import MarketStore
    
    store = MarketStore()
    
    # Write data
    store.write_curated(df_bars, freq="1d")
    
    # Query data
    df = store.get_bars(["AAPL", "MSFT"], start="2024-01-01", end="2024-12-31")
"""

from .store import MarketStore
from .types import ListingInfo, MetadataEntry
from .utils import now_utc_iso

__all__ = [
    "MarketStore",
    "ListingInfo",
    "MetadataEntry",
    "now_utc_iso",
]
