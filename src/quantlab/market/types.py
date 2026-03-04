"""Market data types and schemas.

Slice 1: Local curated parquet with metadata tracking.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd


# Curated bar schema (long table format)
CURATED_SCHEMA = {
    "ts": "datetime64[ns]",
    "listing_id": "string",
    "close": "float64",
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "volume": "float64",
    "amount": "float64",
    "currency": "string",
    "adj": "string",  # none/qfq/hfq/unknown
    "freq": "string",
}

# Metadata schema
METADATA_SCHEMA = {
    "listing_id": "string",
    "region": "string",
    "exchange": "string",
    "freq": "string",
    "min_ts": "datetime64[ns]",
    "max_ts": "datetime64[ns]",
    "last_updated_at": "string",  # ISO8601 UTC
    "status": "string",  # ok/missing
    "provider": "string",
    "note": "string",
}

# Required columns for curated bars
REQUIRED_CURATED_COLS = ["ts", "listing_id", "close"]

# Default values for optional columns
DEFAULT_CURATED_VALUES = {
    "currency": "unknown",
    "adj": "unknown",
    "freq": "1d",
}


@dataclass(frozen=True)
class ListingInfo:
    """Listing information from universe."""
    
    listing_id: str
    region: str
    exchange: str
    currency: str = "unknown"


@dataclass
class MetadataEntry:
    """Single metadata entry for a listing."""
    
    listing_id: str
    region: str
    exchange: str
    freq: str
    min_ts: pd.Timestamp
    max_ts: pd.Timestamp
    last_updated_at: str  # ISO8601
    status: str = "ok"
    provider: str = "local"
    note: str = ""
    
    def to_dict(self) -> dict:
        """Convert to dictionary for DataFrame construction."""
        return {
            "listing_id": self.listing_id,
            "region": self.region,
            "exchange": self.exchange,
            "freq": self.freq,
            "min_ts": self.min_ts,
            "max_ts": self.max_ts,
            "last_updated_at": self.last_updated_at,
            "status": self.status,
            "provider": self.provider,
            "note": self.note,
        }
