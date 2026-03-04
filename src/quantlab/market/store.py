"""MarketStore - Core class for market data storage and retrieval.

Slice 1: Local curated parquet with metadata tracking.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from loguru import logger

from .types import (
    CURATED_SCHEMA,
    METADATA_SCHEMA,
    DEFAULT_CURATED_VALUES,
    ListingInfo,
    MetadataEntry,
)
from .utils import (
    now_utc_iso,
    ensure_dirs,
    coerce_ts,
    schema_empty_df,
    validate_curated_df,
    generate_part_filename,
    dedupe_by_ts,
)


class MarketStore:
    """Market data storage with curated parquet and metadata tracking.
    
    Directory structure:
        data/market/
          curated/region=<REGION>/exchange=<EXCHANGE>/freq=<FREQ>/part-*.parquet
          metadata.parquet
    """
    
    def __init__(
        self,
        base_dir: str = "data/market",
        universe_dir: str = "data/universe"
    ):
        """Initialize MarketStore.
        
        Args:
            base_dir: Base directory for market data
            universe_dir: Directory containing universe listings
        """
        self.base_dir = Path(base_dir)
        self.universe_dir = Path(universe_dir)
        self.curated_dir = self.base_dir / "curated"
        self.metadata_path = self.base_dir / "metadata.parquet"
        
        # Ensure directories exist
        self.curated_dir.mkdir(parents=True, exist_ok=True)
        
        # Cache listings
        self._listings_cache: Optional[pd.DataFrame] = None
    
    def load_universe_listings(self) -> pd.DataFrame:
        """Load universe listings from parquet.
        
        Returns:
            DataFrame with columns: listing_id, region, exchange, currency
        """
        if self._listings_cache is not None:
            return self._listings_cache
        
        listings_path = self.universe_dir / "listings.parquet"
        
        if not listings_path.exists():
            logger.warning(f"Universe listings not found: {listings_path}")
            return schema_empty_df({
                "listing_id": "string",
                "region": "string",
                "exchange": "string",
                "currency": "string",
            })
        
        try:
            df = pd.read_parquet(listings_path)
            # Ensure required columns exist
            required = ["listing_id", "region", "exchange"]
            for col in required:
                if col not in df.columns:
                    logger.warning(f"Missing column in listings: {col}")
                    df[col] = "UNK"
            
            if "currency" not in df.columns:
                df["currency"] = "unknown"
            
            self._listings_cache = df
            logger.debug(f"Loaded {len(df)} listings from universe")
            return df
        
        except Exception as e:
            logger.error(f"Failed to load listings: {e}")
            return schema_empty_df({
                "listing_id": "string",
                "region": "string",
                "exchange": "string",
                "currency": "string",
            })
    
    def get_listing_info(self, listing_id: str) -> ListingInfo:
        """Get listing info for a specific listing_id.
        
        Args:
            listing_id: Listing identifier
            
        Returns:
            ListingInfo with region/exchange
        """
        listings = self.load_universe_listings()
        row = listings[listings["listing_id"] == listing_id]
        
        if row.empty:
            logger.warning(f"Listing not found in universe: {listing_id}")
            return ListingInfo(
                listing_id=listing_id,
                region="UNK",
                exchange="UNK",
                currency="unknown"
            )
        
        return ListingInfo(
            listing_id=listing_id,
            region=str(row.iloc[0]["region"]),
            exchange=str(row.iloc[0]["exchange"]),
            currency=str(row.iloc[0].get("currency", "unknown"))
        )
    
    def load_metadata(self) -> pd.DataFrame:
        """Load metadata from parquet.
        
        Returns:
            DataFrame with metadata, or empty if not exists
        """
        if not self.metadata_path.exists():
            logger.debug("Metadata not found, returning empty")
            return schema_empty_df(METADATA_SCHEMA)
        
        try:
            df = pd.read_parquet(self.metadata_path)
            # Ensure all columns exist
            for col, dtype in METADATA_SCHEMA.items():
                if col not in df.columns:
                    df[col] = pd.Series(dtype=dtype)
            return df
        except Exception as e:
            logger.error(f"Failed to load metadata: {e}")
            return schema_empty_df(METADATA_SCHEMA)
    
    def _save_metadata(self, df: pd.DataFrame) -> None:
        """Save metadata to parquet."""
        ensure_dirs(self.metadata_path)
        df.to_parquet(self.metadata_path, index=False)
    
    def _update_metadata(
        self,
        listing_id: str,
        new_min_ts: pd.Timestamp,
        new_max_ts: pd.Timestamp,
        freq: str,
        provider: str = "local"
    ) -> None:
        """Update metadata for a listing.
        
        Args:
            listing_id: Listing identifier
            new_min_ts: New minimum timestamp
            new_max_ts: New maximum timestamp
            freq: Frequency
            provider: Data provider
        """
        metadata = self.load_metadata()
        info = self.get_listing_info(listing_id)
        now = now_utc_iso()
        
        # Check if listing+freq already exists in metadata
        mask = (metadata["listing_id"].astype(str) == str(listing_id)) & (metadata["freq"].astype(str) == str(freq))
        
        if mask.any():
            # Update existing entry
            idx = metadata[mask].index[0]
            old_min = metadata.at[idx, "min_ts"]
            old_max = metadata.at[idx, "max_ts"]
            
            # Merge timestamps, preserving existing coverage if present
            old_min = pd.to_datetime(old_min, errors="coerce")
            old_max = pd.to_datetime(old_max, errors="coerce")
            metadata.at[idx, "min_ts"] = min(old_min, new_min_ts) if pd.notna(old_min) else new_min_ts
            metadata.at[idx, "max_ts"] = max(old_max, new_max_ts) if pd.notna(old_max) else new_max_ts
            metadata.at[idx, "last_updated_at"] = now
            metadata.at[idx, "status"] = "ok"
            metadata.at[idx, "freq"] = freq
            metadata.at[idx, "provider"] = provider
        else:
            # Create new entry
            entry = MetadataEntry(
                listing_id=listing_id,
                region=info.region,
                exchange=info.exchange,
                freq=freq,
                min_ts=new_min_ts,
                max_ts=new_max_ts,
                last_updated_at=now,
                status="ok",
                provider=provider
            )
            new_row = pd.DataFrame([entry.to_dict()])
            metadata = pd.concat([metadata, new_row], ignore_index=True)
        
        self._save_metadata(metadata)
        logger.debug(f"Updated metadata for {listing_id}: {new_min_ts} to {new_max_ts}")
    
    def write_curated(
        self,
        df_bars: pd.DataFrame,
        freq: str = "1d",
        adj: str = "unknown",
        provider: str = "local"
    ) -> None:
        """Write curated bars to storage.
        
        Args:
            df_bars: DataFrame with bars (must have ts, listing_id, close)
            freq: Frequency (e.g., "1d", "1m")
            adj: Adjustment type (none/qfq/hfq/unknown)
            provider: Data provider
        """
        # Validate input
        is_valid, msg = validate_curated_df(df_bars)
        if not is_valid:
            raise ValueError(f"Invalid curated data: {msg}")
        
        # Coerce timestamp
        df_bars = coerce_ts(df_bars, "ts")
        
        # Add default values for optional columns
        for col, default in DEFAULT_CURATED_VALUES.items():
            if col not in df_bars.columns:
                df_bars[col] = default
        
        # Set freq and adj
        df_bars["freq"] = freq
        df_bars["adj"] = adj
        
        # Process each listing_id
        for listing_id in df_bars["listing_id"].unique():
            df_listing = df_bars[df_bars["listing_id"] == listing_id].copy()
            
            # Deduplicate by timestamp
            df_listing = dedupe_by_ts(df_listing, "ts")
            
            if df_listing.empty:
                continue
            
            # Get listing info for partitioning
            info = self.get_listing_info(listing_id)
            
            # Build partition path: region=X/exchange=Y/freq=Z/
            partition_dir = (
                self.curated_dir /
                f"region={info.region}" /
                f"exchange={info.exchange}" /
                f"freq={freq}"
            )
            partition_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate filename
            max_ts = df_listing["ts"].max()
            filename = generate_part_filename(listing_id, max_ts)
            filepath = partition_dir / filename
            
            # Write parquet
            df_listing.to_parquet(filepath, index=False, compression="zstd")
            logger.info(f"Wrote {len(df_listing)} bars to {filepath}")
            
            # Update metadata
            min_ts = df_listing["ts"].min()
            self._update_metadata(
                listing_id=listing_id,
                new_min_ts=min_ts,
                new_max_ts=max_ts,
                freq=freq,
                provider=provider
            )
    
    def get_bars(
        self,
        listing_ids: list[str],
        start: str | datetime,
        end: str | datetime,
        freq: str = "1d",
        fields: Optional[list[str]] = None
    ) -> pd.DataFrame:
        """Query bars from curated storage.
        
        Args:
            listing_ids: List of listing IDs to query
            start: Start timestamp (inclusive)
            end: End timestamp (inclusive)
            freq: Frequency filter
            fields: Columns to return (None = all)
            
        Returns:
            DataFrame with bars, sorted by ts, listing_id
        """
        start_ts = pd.to_datetime(start)
        end_ts = pd.to_datetime(end)
        
        # Find all parquet files for the freq
        pattern = f"**/freq={freq}/*.parquet"
        parquet_files = list(self.curated_dir.glob(pattern))
        
        if not parquet_files:
            logger.warning(f"No curated data found for freq={freq}")
            return schema_empty_df(CURATED_SCHEMA)
        
        # Read and filter
        dfs = []
        for f in parquet_files:
            try:
                df = pd.read_parquet(f)
                
                # Filter by listing_id
                df = df[df["listing_id"].isin(listing_ids)]
                
                # Filter by timestamp
                df = df[(df["ts"] >= start_ts) & (df["ts"] <= end_ts)]
                
                if not df.empty:
                    dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to read {f}: {e}")
        
        if not dfs:
            return schema_empty_df(CURATED_SCHEMA)
        
        # Concat and dedupe across all parts
        result = pd.concat(dfs, ignore_index=True)
        result = result.drop_duplicates(subset=["listing_id", "ts"], keep="last")

        # Select fields if specified
        if fields is not None:
            available_cols = set(result.columns)
            select_cols = ["ts", "listing_id"] + [
                f for f in fields if f in available_cols
            ]
            result = result[[col for col in select_cols if col in result.columns]]

        result = result.sort_values(["ts", "listing_id"])

        return result.reset_index(drop=True)
