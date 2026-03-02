"""Data ingestion and validation.

Handles importing data from various sources and validating quality.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

from quantlab.core.registry import data_sources
from quantlab.core.types import DataSnapshot
from quantlab.data.sources.base import DataSource, AbstractDataSource


class DataIngestor:
    """Data ingestion pipeline."""
    
    def __init__(
        self,
        source: str | DataSource,
        output_dir: str | Path
    ) -> None:
        """Initialize ingestor.
        
        Args:
            source: Data source name (registered) or instance
            output_dir: Directory to store curated data
        """
        if isinstance(source, str):
            source_class = data_sources.get(source)
            self.source = source_class  # type: ignore
        else:
            self.source = source
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def ingest(
        self,
        symbols: List[str],
        start: datetime,
        end: datetime,
        frequency: str = "daily"
    ) -> pd.DataFrame:
        """Ingest data for symbols.
        
        Args:
            symbols: List of symbols to ingest
            start: Start date
            end: End date
            frequency: Data frequency
            
        Returns:
            Combined DataFrame with all symbols
        """
        logger.info(f"Ingesting {len(symbols)} symbols from {start} to {end}")
        
        all_data = []
        for symbol in symbols:
            try:
                df = self.source.get_bars(symbol, start, end, frequency)
                all_data.append(df)
                logger.debug(f"Ingested {len(df)} rows for {symbol}")
            except Exception as e:
                logger.error(f"Failed to ingest {symbol}: {e}")
        
        if not all_data:
            raise ValueError("No data ingested")
        
        combined = pd.concat(all_data, ignore_index=True)
        
        # Validate
        self._validate(combined)
        
        logger.info(f"Ingested {len(combined)} total rows")
        return combined
    
    def _validate(self, df: pd.DataFrame) -> None:
        """Validate ingested data."""
        required_cols = ["ts", "symbol", "open", "high", "low", "close", "volume"]
        missing = set(required_cols) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        # Check for negative prices
        for col in ["open", "high", "low", "close"]:
            if (df[col] < 0).any():
                raise ValueError(f"Negative values found in {col}")
        
        # Check OHLC relationships
        if not (df["high"] >= df["low"]).all():
            raise ValueError("high < low found")
        
        # Check for missing values
        null_count = df[required_cols].isnull().sum().sum()
        if null_count > 0:
            logger.warning(f"Found {null_count} null values")
    
    def save_curated(
        self,
        df: pd.DataFrame,
        name: str,
        format: str = "parquet"
    ) -> Path:
        """Save curated data.
        
        Args:
            df: DataFrame to save
            name: Dataset name
            format: File format (parquet, csv)
            
        Returns:
            Path to saved file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}"
        
        if format == "parquet":
            path = self.output_dir / f"{filename}.parquet"
            df.to_parquet(path, index=False)
        elif format == "csv":
            path = self.output_dir / f"{filename}.csv"
            df.to_csv(path, index=False)
        else:
            raise ValueError(f"Unknown format: {format}")
        
        logger.info(f"Saved curated data to {path}")
        return path
    
    def create_snapshot(
        self,
        df: pd.DataFrame,
        symbols: List[str],
        manifest_path: str
    ) -> DataSnapshot:
        """Create data snapshot for reproducibility.
        
        Args:
            df: DataFrame
            symbols: List of symbols
            manifest_path: Path to store manifest
            
        Returns:
            DataSnapshot object
        """
        # Calculate hash
        data_hash = hashlib.sha256(
            pd.util.hash_pandas_object(df).values.tobytes()
        ).hexdigest()[:16]
        
        # Get date range
        start = df["ts"].min()
        end = df["ts"].max()
        
        snapshot = DataSnapshot(
            snapshot_id=f"snap_{data_hash}",
            data_range=(start, end),
            symbols=sorted(symbols),
            hash=data_hash,
            manifest_path=manifest_path
        )
        
        # Save manifest
        manifest = {
            "snapshot_id": snapshot.snapshot_id,
            "hash": snapshot.hash,
            "data_range": [start.isoformat(), end.isoformat()],
            "symbols": symbols,
            "created_at": datetime.now().isoformat(),
            "row_count": len(df),
        }
        
        Path(manifest_path).parent.mkdir(parents=True, exist_ok=True)
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        
        return snapshot
