"""Data Update Script for AkShare

Fetches ETF and Index data from AkShare, normalizes to unified format,
and saves to curated directories with incremental updates.

Usage:
    python scripts/update_data.py --config config/data_sources.yaml
    python scripts/update_data.py --symbol ETF:510300 --force
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yaml
from loguru import logger

try:
    import akshare as ak
except ImportError:
    raise ImportError("akshare is required. Install with: pip install akshare")

# Setup logging
def setup_logging(level: str = "INFO", log_file: Optional[str] = None):
    """Setup logging configuration."""
    logger.remove()
    logger.add(sys.stderr, level=level)
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        logger.add(log_file, rotation="10 MB", retention=3, level=level)


# Field mapping from AkShare Chinese columns to unified format
AKSHARE_FIELD_MAP = {
    # ETF fields
    "日期": "ts",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
    "成交额": "amount",
    # Index fields (same mapping)
    # Add more mappings if AkShare uses different names for indices
}


class DataValidator:
    """Validate OHLCV data quality."""
    
    @staticmethod
    def validate(df: pd.DataFrame, symbol: str) -> Tuple[bool, List[str]]:
        """Validate DataFrame and return (is_valid, list_of_issues).
        
        Checks:
        1. ts is sorted and no duplicates
        2. high >= max(open, close)
        3. low <= min(open, close)
        4. volume >= 0
        5. No NaN in required columns
        """
        issues = []
        
        if df.empty:
            return False, ["Empty dataframe"]
        
        required_cols = ["ts", "open", "high", "low", "close", "volume"]
        for col in required_cols:
            if col not in df.columns:
                return False, [f"Missing required column: {col}"]
        
        # Check sorted and no duplicates
        if not df["ts"].is_monotonic_increasing:
            issues.append("Timestamps not sorted")
        if df["ts"].duplicated().any():
            dup_count = df["ts"].duplicated().sum()
            issues.append(f"Found {dup_count} duplicate timestamps")
        
        # Check OHLC logic
        ohlc_violations = df[
            (df["high"] < df[["open", "close"]].max(axis=1)) |
            (df["low"] > df[["open", "close"]].min(axis=1))
        ]
        if len(ohlc_violations) > 0:
            issues.append(f"OHLC logic violations: {len(ohlc_violations)} rows")
        
        # Check volume
        if (df["volume"] < 0).any():
            neg_volume = (df["volume"] < 0).sum()
            issues.append(f"Negative volume: {neg_volume} rows")
        
        # Check NaN
        for col in ["open", "high", "low", "close"]:
            if df[col].isna().any():
                issues.append(f"NaN in {col}")
        
        return len(issues) == 0, issues


class ManifestManager:
    """Manage update manifests with last timestamp tracking."""
    
    def __init__(self, manifest_dir: Path):
        self.manifest_dir = Path(manifest_dir)
        self.manifest_dir.mkdir(parents=True, exist_ok=True)
    
    def load(self, name: str) -> Dict[str, str]:
        """Load manifest file."""
        path = self.manifest_dir / f"{name}.json"
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
    
    def save(self, name: str, data: Dict[str, str]):
        """Save manifest file."""
        path = self.manifest_dir / f"{name}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    
    def get_last_ts(self, name: str, symbol: str) -> Optional[str]:
        """Get last timestamp for symbol."""
        manifest = self.load(name)
        return manifest.get(symbol)
    
    def update_last_ts(self, name: str, symbol: str, ts: str):
        """Update last timestamp for symbol."""
        manifest = self.load(name)
        manifest[symbol] = ts
        self.save(name, manifest)


class AkShareDataUpdater:
    """Update ETF and Index data from AkShare."""
    
    def __init__(self, config_path: str):
        with open(config_path, "r", encoding="utf-8-sig") as f:
            self.config = yaml.safe_load(f)
        
        self.validator = DataValidator()
        self.manifest = ManifestManager(
            self.config.get("manifest", {}).get("dir", "data/manifest")
        )
        
        setup_logging(
            self.config.get("logging", {}).get("level", "INFO"),
            self.config.get("logging", {}).get("file")
        )
    
    def _fetch_etf(self, symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> pd.DataFrame:
        """Fetch ETF historical data from AkShare."""
        logger.info(f"Fetching ETF {symbol} from {start_date} to {end_date}")
        
        try:
            # AkShare ETF interface
            df = ak.fund_etf_hist_em(
                symbol=symbol,
                period="daily",
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", ""),
                adjust=adjust
            )
            
            if df.empty:
                logger.warning(f"No data returned for ETF {symbol}")
                return pd.DataFrame()
            
            # Rename columns
            df = df.rename(columns=AKSHARE_FIELD_MAP)
            
            # Normalize timestamp
            df["ts"] = pd.to_datetime(df["ts"]).dt.strftime("%Y-%m-%d")
            
            # Add symbol with prefix
            df["symbol"] = f"ETF:{symbol}"
            
            # Ensure amount column exists
            if "amount" not in df.columns:
                df["amount"] = 0.0
            
            return df[["ts", "symbol", "open", "high", "low", "close", "volume", "amount"]]
            
        except Exception as e:
            logger.error(f"Failed to fetch ETF {symbol}: {e}")
            return pd.DataFrame()
    
    def _fetch_index(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch Index historical data from AkShare."""
        logger.info(f"Fetching Index {symbol} from {start_date} to {end_date}")
        
        try:
            # Determine market prefix for AkShare
            # Shanghai: sh + code, Shenzhen: sz + code
            if symbol.startswith("0") or symbol.startswith("000"):
                ak_symbol = f"sh{symbol}"
            elif symbol.startswith("3") or symbol.startswith("399"):
                ak_symbol = f"sz{symbol}"
            else:
                ak_symbol = symbol  # Assume already formatted
            
            df = ak.index_zh_a_hist(
                symbol=symbol,  # AkShare uses raw code for some index interfaces
                period="daily",
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", "")
            )
            
            if df.empty:
                logger.warning(f"No data returned for Index {symbol}")
                return pd.DataFrame()
            
            # Rename columns
            df = df.rename(columns=AKSHARE_FIELD_MAP)
            
            # Normalize timestamp
            df["ts"] = pd.to_datetime(df["ts"]).dt.strftime("%Y-%m-%d")
            
            # Add symbol with prefix
            df["symbol"] = f"IDX:{symbol}"
            
            # Ensure amount column exists
            if "amount" not in df.columns:
                df["amount"] = 0.0
            
            return df[["ts", "symbol", "open", "high", "low", "close", "volume", "amount"]]
            
        except Exception as e:
            logger.error(f"Failed to fetch Index {symbol}: {e}")
            return pd.DataFrame()
    
    def _load_existing(self, filepath: Path) -> pd.DataFrame:
        """Load existing data from CSV."""
        if not filepath.exists():
            return pd.DataFrame()
        
        try:
            df = pd.read_csv(filepath)
            if not df.empty:
                df["ts"] = pd.to_datetime(df["ts"]).dt.strftime("%Y-%m-%d")
            return df
        except Exception as e:
            logger.error(f"Failed to load {filepath}: {e}")
            return pd.DataFrame()
    
    def _save_data(self, df: pd.DataFrame, filepath: Path):
        """Save data to CSV."""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(filepath, index=False)
        logger.info(f"Saved {len(df)} rows to {filepath}")
    
    def update_etf(self, force: bool = False):
        """Update all configured ETFs."""
        if not self.config.get("etf", {}).get("enabled", False):
            logger.info("ETF update disabled")
            return
        
        config = self.config["etf"]
        symbols = config.get("symbols", [])
        out_dir = Path(config.get("out_dir", "data/raw/bars/etf"))
        adjust = config.get("adjust", "qfq")
        
        today = datetime.now().strftime("%Y-%m-%d")
        
        for symbol in symbols:
            logger.info(f"Processing ETF: {symbol}")
            
            internal_symbol = f"ETF:{symbol}"
            filepath = out_dir / f"{internal_symbol}.csv"
            
            # Determine date range
            if force or not filepath.exists():
                start_date = "20150101"  # Fetch full history
            else:
                last_ts = self.manifest.get_last_ts("etf", internal_symbol)
                if last_ts:
                    # Start from next day
                    start_date = (datetime.strptime(last_ts, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                else:
                    start_date = "20150101"
            
            # Fetch new data
            df_new = self._fetch_etf(symbol, start_date, today, adjust)
            
            if df_new.empty:
                logger.info(f"No new data for ETF:{symbol}")
                continue
            
            # Validate
            is_valid, issues = self.validator.validate(df_new, internal_symbol)
            if not is_valid:
                logger.error(f"Validation failed for ETF:{symbol}: {issues}")
                continue
            
            # Load existing and merge
            df_existing = self._load_existing(filepath)
            if not df_existing.empty:
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                df_combined = df_combined.drop_duplicates(subset=["ts"], keep="last")
                df_combined = df_combined.sort_values("ts")
            else:
                df_combined = df_new
            
            # Save
            self._save_data(df_combined, filepath)
            
            # Update manifest
            last_ts = df_combined["ts"].max()
            self.manifest.update_last_ts("etf", internal_symbol, last_ts)
    
    def update_index(self, force: bool = False):
        """Update all configured Indices."""
        if not self.config.get("index", {}).get("enabled", False):
            logger.info("Index update disabled")
            return
        
        config = self.config["index"]
        symbols = config.get("symbols", [])
        out_dir = Path(config.get("out_dir", "data/raw/bars/index"))
        
        today = datetime.now().strftime("%Y-%m-%d")
        
        for symbol in symbols:
            logger.info(f"Processing Index: {symbol}")
            
            internal_symbol = f"IDX:{symbol}"
            filepath = out_dir / f"{internal_symbol}.csv"
            
            # Determine date range
            if force or not filepath.exists():
                start_date = "20100101"
            else:
                last_ts = self.manifest.get_last_ts("index", internal_symbol)
                if last_ts:
                    start_date = (datetime.strptime(last_ts, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                else:
                    start_date = "20100101"
            
            # Fetch
            df_new = self._fetch_index(symbol, start_date, today)
            
            if df_new.empty:
                logger.info(f"No new data for IDX:{symbol}")
                continue
            
            # Validate
            is_valid, issues = self.validator.validate(df_new, internal_symbol)
            if not is_valid:
                logger.error(f"Validation failed for IDX:{symbol}: {issues}")
                continue
            
            # Load existing and merge
            df_existing = self._load_existing(filepath)
            if not df_existing.empty:
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                df_combined = df_combined.drop_duplicates(subset=["ts"], keep="last")
                df_combined = df_combined.sort_values("ts")
            else:
                df_combined = df_new
            
            # Save
            self._save_data(df_combined, filepath)
            
            # Update manifest
            last_ts = df_combined["ts"].max()
            self.manifest.update_last_ts("index", internal_symbol, last_ts)
    
    def update_all(self, force: bool = False):
        """Update all configured data."""
        logger.info("Starting data update...")
        
        self.update_etf(force=force)
        self.update_index(force=force)
        
        logger.info("Data update completed!")


def main():
    parser = argparse.ArgumentParser(description="Update market data from AkShare")
    parser.add_argument(
        "--config",
        default="config/data_sources.yaml",
        help="Path to data sources config"
    )
    parser.add_argument(
        "--symbol",
        help="Update specific symbol only (e.g., ETF:510300 or IDX:000300)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force full refresh (ignore existing data)"
    )
    parser.add_argument(
        "--type",
        choices=["etf", "index", "all"],
        default="all",
        help="Which type to update"
    )
    
    args = parser.parse_args()
    
    updater = AkShareDataUpdater(args.config)
    
    if args.symbol:
        # Single symbol update
        if args.symbol.startswith("ETF:"):
            updater.update_etf(force=args.force)
        elif args.symbol.startswith("IDX:"):
            updater.update_index(force=args.force)
        else:
            logger.error(f"Unknown symbol format: {args.symbol}")
            return 1
    else:
        # Batch update
        if args.type in ("etf", "all"):
            updater.update_etf(force=args.force)
        if args.type in ("index", "all"):
            updater.update_index(force=args.force)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
