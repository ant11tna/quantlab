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
def emit(obj: Dict):
    """Emit structured event as JSON line to stdout."""
    print(json.dumps(obj, ensure_ascii=False), flush=True)


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

        # ETF data source fallback order and timeout settings
        self.config.setdefault("etf_provider_order", ["em", "sina"])
        self.config.setdefault("timeout_sec", 15)
        
        self.validator = DataValidator()
        self.updated_files = 0
        self.manifest = ManifestManager(
            self.config.get("manifest", {}).get("dir", "data/manifest")
        )
        
        setup_logging(
            self.config.get("logging", {}).get("level", "INFO"),
            self.config.get("logging", {}).get("file")
        )

    def _to_sina_symbol(self, code: str) -> str:
        """Convert ETF code to Sina symbol format."""
        if code.startswith("5"):
            return f"sh{code}"
        if code.startswith("1"):
            return f"sz{code}"

        logger.warning(f"Unknown ETF code prefix for {code}, fallback to sh{code}")
        return f"sh{code}"

    def _normalize_bar_dataframe(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Normalize OHLCV schema for storage and downstream curation."""
        if df.empty:
            return df

        required_cols = ["ts", "symbol", "open", "high", "low", "close", "volume"]
        if any(col not in df.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df.columns]
            logger.error(f"Cannot normalize {symbol}: missing columns {missing}")
            return pd.DataFrame()

        out = df.copy()
        out["ts"] = pd.to_datetime(out["ts"])
        for col in ["open", "high", "low", "close"]:
            out[col] = pd.to_numeric(out[col], errors="coerce")
        out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0).astype("int64")

        if "amount" in out.columns:
            out["amount"] = pd.to_numeric(out["amount"], errors="coerce").fillna(0.0)

        out = out.dropna(subset=["ts", "open", "high", "low", "close"])
        out = out.sort_values("ts").drop_duplicates(["symbol", "ts"])
        out["ts"] = out["ts"].dt.strftime("%Y-%m-%d")

        columns = required_cols + (["amount"] if "amount" in out.columns else [])
        return out[columns]

    def _fetch_etf_em(self, symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> pd.DataFrame:
        """Fetch ETF history from Eastmoney provider."""
        df = ak.fund_etf_hist_em(
            symbol=symbol,
            period="daily",
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
            adjust=adjust,
        )

        if df.empty:
            return pd.DataFrame()

        df = df.rename(columns=AKSHARE_FIELD_MAP)
        df["symbol"] = f"ETF:{symbol}"
        return self._normalize_bar_dataframe(df, f"ETF:{symbol}")

    def _fetch_etf_sina(self, code: str, start: str, end: str) -> pd.DataFrame:
        """Fetch ETF history from Sina provider and normalize schema."""
        sina_symbol = self._to_sina_symbol(code)
        df = ak.fund_etf_hist_sina(symbol=sina_symbol)

        if df.empty:
            return pd.DataFrame()

        df = df.rename(columns={"date": "ts"})
        df["ts"] = pd.to_datetime(df["ts"])
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
        if "amount" in df.columns:
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)

        df["symbol"] = f"ETF:{code}"

        start_dt = pd.to_datetime(start)
        end_dt = pd.to_datetime(end)
        df = df[(df["ts"] >= start_dt) & (df["ts"] <= end_dt)]
        df = df.sort_values("ts").drop_duplicates(["symbol", "ts"])

        return self._normalize_bar_dataframe(df, f"ETF:{code}")
    
    def _fetch_etf(self, symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> pd.DataFrame:
        """Fetch ETF historical data with provider fallback."""
        logger.info(f"Fetching ETF {symbol} from {start_date} to {end_date}")

        providers = self.config.get("etf_provider_order", ["em", "sina"])
        for provider in providers:
            try:
                if provider == "em":
                    df = self._fetch_etf_em(symbol, start_date, end_date, adjust)
                elif provider == "sina":
                    df = self._fetch_etf_sina(symbol, start_date, end_date)
                else:
                    logger.warning(f"Unknown ETF provider '{provider}', skipping")
                    continue

                if not df.empty:
                    logger.info(f"Fetched ETF {symbol} via {provider}, rows={len(df)}")
                    return df

                logger.warning(f"ETF {symbol} returned empty via {provider}")
            except Exception as e:
                logger.warning(f"Failed ETF {symbol} via {provider}: {e}")

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
        df = self._normalize_bar_dataframe(df, filepath.stem)
        if df.empty:
            logger.warning(f"Skip saving empty normalized dataframe for {filepath}")
            return

        df.to_csv(filepath, index=False, encoding="utf-8")
        self.updated_files += 1
        logger.info(f"Saved {len(df)} rows to {filepath}")

    @staticmethod
    def _resolve_last_ts(last_ts_manifest: Optional[str], last_ts_file: Optional[str]) -> Optional[str]:
        """Resolve newest timestamp between manifest and existing file."""

        candidates = [ts for ts in [last_ts_manifest, last_ts_file] if ts]
        if not candidates:
            return None

        parsed = []
        for ts in candidates:
            try:
                parsed.append(datetime.strptime(ts, "%Y-%m-%d"))
            except ValueError:
                logger.warning(f"Invalid timestamp format '{ts}', ignored when resolving last_ts")

        if not parsed:
            return None

        return max(parsed).strftime("%Y-%m-%d")
    
    def _update_one_etf(self, symbol: str, force: bool = False) -> bool:
        """Update a single ETF symbol."""
        config = self.config["etf"]
        out_dir = Path(config.get("out_dir", "data/raw/bars/etf"))
        adjust = config.get("adjust", "qfq")
        today = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Processing ETF: {symbol}")

        internal_symbol = f"ETF:{symbol}"
        filepath = out_dir / f"{internal_symbol}.csv"

        if force or not filepath.exists():
            start_date = "20150101"
        else:
            last_ts_manifest = self.manifest.get_last_ts("etf", internal_symbol)
            df_existing = self._load_existing(filepath)
            last_ts_file = df_existing["ts"].max() if not df_existing.empty else None
            last_ts = self._resolve_last_ts(last_ts_manifest, last_ts_file)
            if last_ts:
                start_date = (datetime.strptime(last_ts, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = "20150101"

        df_new = self._fetch_etf(symbol, start_date, today, adjust)
        if df_new.empty:
            logger.info(f"No new data for ETF:{symbol}")
            return True

        is_valid, issues = self.validator.validate(df_new, internal_symbol)
        if not is_valid:
            raise ValueError(f"Validation failed for ETF:{symbol}: {issues}")

        df_existing = self._load_existing(filepath)
        if not df_existing.empty:
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=["ts"], keep="last")
            df_combined = df_combined.sort_values("ts")
        else:
            df_combined = df_new

        self._save_data(df_combined, filepath)
        last_ts = df_combined["ts"].max()
        self.manifest.update_last_ts("etf", internal_symbol, last_ts)
        return True

    def update_etf(self, force: bool = False):
        """Update all configured ETFs."""
        if not self.config.get("etf", {}).get("enabled", False):
            logger.info("ETF update disabled")
            return
        
        config = self.config["etf"]
        for symbol in config.get("symbols", []):
            try:
                self._update_one_etf(symbol, force=force)
            except Exception as e:
                logger.error(f"Failed ETF:{symbol}: {e}")
    
    def _update_one_index(self, symbol: str, force: bool = False) -> bool:
        """Update a single index symbol."""
        config = self.config["index"]
        out_dir = Path(config.get("out_dir", "data/raw/bars/index"))
        today = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Processing Index: {symbol}")

        internal_symbol = f"IDX:{symbol}"
        filepath = out_dir / f"{internal_symbol}.csv"

        if force or not filepath.exists():
            start_date = "20100101"
        else:
            last_ts_manifest = self.manifest.get_last_ts("index", internal_symbol)
            df_existing = self._load_existing(filepath)
            last_ts_file = df_existing["ts"].max() if not df_existing.empty else None
            last_ts = self._resolve_last_ts(last_ts_manifest, last_ts_file)
            if last_ts:
                start_date = (datetime.strptime(last_ts, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = "20100101"

        df_new = self._fetch_index(symbol, start_date, today)
        if df_new.empty:
            logger.info(f"No new data for IDX:{symbol}")
            return True

        is_valid, issues = self.validator.validate(df_new, internal_symbol)
        if not is_valid:
            raise ValueError(f"Validation failed for IDX:{symbol}: {issues}")

        df_existing = self._load_existing(filepath)
        if not df_existing.empty:
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=["ts"], keep="last")
            df_combined = df_combined.sort_values("ts")
        else:
            df_combined = df_new

        self._save_data(df_combined, filepath)
        last_ts = df_combined["ts"].max()
        self.manifest.update_last_ts("index", internal_symbol, last_ts)
        return True

    def update_index(self, force: bool = False):
        """Update all configured Indices."""
        if not self.config.get("index", {}).get("enabled", False):
            logger.info("Index update disabled")
            return
        
        config = self.config["index"]
        for symbol in config.get("symbols", []):
            try:
                self._update_one_index(symbol, force=force)
            except Exception as e:
                logger.error(f"Failed IDX:{symbol}: {e}")
    
    def update_all(self, force: bool = False):
        """Update all configured data."""
        logger.info("Starting data update...")

        self.update_etf(force=force)
        self.update_index(force=force)

        logger.info("Data update completed!")

    def _has_any_raw_data(self, data_type: str = "all") -> bool:
        """Check if local raw CSV data already exists."""
        sources = self.config.get("sources", {})

        if data_type in ("etf", "all"):
            etf_dir = Path(sources.get("etf", {}).get("out_dir", "data/raw/bars/etf"))
            if list(etf_dir.glob("*.csv")):
                return True

        if data_type in ("index", "all"):
            index_dir = Path(sources.get("index", {}).get("out_dir", "data/raw/bars/index"))
            if list(index_dir.glob("*.csv")):
                return True

        return False


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
    
    etf_symbols = updater.config.get("etf", {}).get("symbols", []) if args.type in ("etf", "all") else []
    index_symbols = updater.config.get("index", {}).get("symbols", []) if args.type in ("index", "all") else []

    targets: List[Tuple[str, str]] = []
    if args.symbol:
        if args.symbol.startswith("ETF:"):
            targets = [("etf", args.symbol.split(":", 1)[1])]
        elif args.symbol.startswith("IDX:"):
            targets = [("index", args.symbol.split(":", 1)[1])]
        else:
            logger.error(f"Unknown symbol format: {args.symbol}")
            return 1
    else:
        targets.extend(("etf", sym) for sym in etf_symbols)
        targets.extend(("index", sym) for sym in index_symbols)

    total_symbols = len(targets)
    done = 0
    raw_ok = True

    for data_type, symbol in targets:
        ok = True
        try:
            if data_type == "etf":
                updater._update_one_etf(symbol, force=args.force)
            else:
                updater._update_one_index(symbol, force=args.force)
        except Exception as e:
            ok = False
            raw_ok = False
            emit({"type": "error", "stage": "raw", "symbol": symbol, "message": str(e)})
            logger.error(f"Failed to process {data_type}:{symbol}: {e}")
        finally:
            done += 1
            emit({
                "type": "progress",
                "stage": "raw",
                "done": done,
                "total": total_symbols,
                "symbol": symbol,
                "ok": ok,
            })

    emit({"type": "done", "stage": "raw", "ok": raw_ok, "done": done, "total": total_symbols})

    # Fail only when nothing was updated and no local data exists at all.
    if updater.updated_files == 0 and not updater._has_any_raw_data(args.type):
        logger.error("No data updated and no local raw CSV found. Check network/proxy or data source config.")
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
