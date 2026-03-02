"""Curated data builder - Transform raw CSV to curated Parquet with regime fields.

Builds the curated_v1 data contract:
    1. Reads CSV files from data/raw/bars (scripts/update_data.py output)
    2. Applies apply_curated_transforms() to add regime fields
    3. Writes Parquet files (zstd compressed) to data/curated/bars
    4. Generates curated_index.json for fast lookups

Usage:
    from quantlab.data.curate import CuratedDataBuilder
    
    builder = CuratedDataBuilder()
    builder.build_all()  # Build all CSV files in data/raw/bars/
    
    # Or build specific file
    builder.build_symbol("data/raw/bars/etf/ETF:510300.csv")
"""

from __future__ import annotations

import json
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

from quantlab.data.transforms import apply_curated_transforms
from quantlab.data.schema import validate_bars_df, SCHEMA_CURATED_V1




@dataclass
class CuratedDataBuilderConfig:
    """Configuration for curated data builder paths."""

    raw_dir: Path = Path("data/raw/bars")
    curated_root: Path = Path("data/curated/bars")
    index_file: str = "curated_index.json"


class CuratedDataBuilder:
    """Build curated parquet files from raw CSV data."""
    
    # Default paths (CSV raw -> Parquet curated)
    DEFAULT_RAW_DIR = Path("data/raw/bars")      # Input: raw CSV from update_data.py
    DEFAULT_CURATED_ROOT = Path("data/curated/bars")  # Output: curated Parquet
    DEFAULT_INDEX_FILE = "curated_index.json"
    
    def __init__(
        self,
        raw_dir: Optional[Path] = None,
        curated_root: Optional[Path] = None,
        out_dir: Optional[Path] = None,
        index_file: Optional[str] = None,
        config: Optional[CuratedDataBuilderConfig] = None,
    ):
        """Initialize builder.
        
        Args:
            raw_dir: Directory containing raw CSV files (default: data/raw/bars)
            curated_root: Directory for output parquet files (default: data/curated/bars)
            out_dir: Backward-compatible alias of curated_root
            index_file: Name of the index JSON file
            config: Optional typed configuration
        """
        if config is not None:
            self.raw_dir = Path(config.raw_dir)
            self.out_dir = Path(config.curated_root)
            self.index_file = config.index_file
        else:
            self.raw_dir = Path(raw_dir) if raw_dir else self.DEFAULT_RAW_DIR
            resolved_out_dir = curated_root or out_dir
            self.out_dir = Path(resolved_out_dir) if resolved_out_dir else self.DEFAULT_CURATED_ROOT
            self.index_file = index_file or self.DEFAULT_INDEX_FILE

        self.out_dir.mkdir(parents=True, exist_ok=True)
        
        # Track built symbols for index
        self.built_symbols: Dict[str, Dict] = {}
    
    def _find_csv_files(self) -> List[Path]:
        """Find all CSV files in raw directory."""
        csv_files = []
        
        if not self.raw_dir.exists():
            logger.warning(f"Raw directory not found: {self.raw_dir}")
            return csv_files
        
        # Search in subdirectories (etf/, index/, stock/)
        for subdir in self.raw_dir.iterdir():
            if subdir.is_dir():
                csv_files.extend(subdir.glob("*.csv"))
        
        # Also check root of raw_dir
        csv_files.extend(self.raw_dir.glob("*.csv"))
        
        return sorted(csv_files)
    
    def _extract_symbol_from_path(self, csv_path: Path) -> str:
        """Extract symbol from CSV filename.
        
        Examples:
            ETF:510300.csv -> ETF:510300
            IDX:000300.csv -> IDX:000300
            sh510300.csv -> ETF:510300 (legacy format)
        """
        stem = csv_path.stem
        
        # Already has prefix
        if stem.startswith(("ETF:", "IDX:", "STK:")):
            return stem
        
        # Legacy formats
        if stem.startswith("sh") or stem.startswith("sz"):
            code = stem[2:]
            if code.startswith("5") or code.startswith("1"):
                return f"ETF:{code}"
            elif code.startswith(("0", "3", "6")):
                return f"STK:{code}"
        
        # Default: use stem as-is
        return stem
    
    def _extract_source_type(self, csv_path: Path) -> str:
        """Extract source type from path."""
        parts = csv_path.parts
        
        # Check parent directory name
        parent = csv_path.parent.name.lower()
        if parent in ("etf", "index", "stock"):
            return parent
        
        # Infer from filename
        stem = csv_path.stem.upper()
        if stem.startswith("ETF:"):
            return "etf"
        elif stem.startswith("IDX:"):
            return "index"
        elif stem.startswith("STK:"):
            return "stock"
        
        return "unknown"
    
    def build_symbol(
        self,
        csv_path: Path,
        output_symbol: Optional[str] = None,
        validate: bool = True
    ) -> Optional[Path]:
        """Build curated parquet from a single CSV file.
        
        Args:
            csv_path: Path to input CSV file
            output_symbol: Optional output symbol name (default: extracted from filename)
            validate: Whether to validate output against schema
            
        Returns:
            Path to output parquet file, or None if failed
        """
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            return None
        
        symbol = output_symbol or self._extract_symbol_from_path(csv_path)
        source_type = self._extract_source_type(csv_path)
        
        logger.info(f"Building curated data for {symbol} from {csv_path}")
        
        try:
            # Read CSV
            df = pd.read_csv(csv_path)
            
            if df.empty:
                logger.warning(f"Empty CSV file: {csv_path}")
                return None
            
            # Normalize columns
            df = self._normalize_columns(df)
            
            # Ensure symbol column exists
            if "symbol" not in df.columns:
                df["symbol"] = symbol
            
            # Apply curated transforms (adds regime fields)
            df = apply_curated_transforms(df)
            
            # Validate if requested
            if validate:
                is_valid, issues = validate_bars_df(df, required_schema="curated_v1")
                if not is_valid:
                    logger.error(f"Validation failed for {symbol}: {issues}")
                    return None
            
            # Determine output path
            out_subdir = self.out_dir / source_type
            out_subdir.mkdir(parents=True, exist_ok=True)
            
            # Sanitize filename
            safe_name = symbol.replace(":", "_")
            parquet_path = out_subdir / f"{safe_name}.parquet"
            
            # Write parquet with zstd compression
            df.to_parquet(
                parquet_path,
                compression="zstd",
                index=False
            )
            
            logger.info(f"Saved {len(df)} rows to {parquet_path}")
            
            # Track for index
            self.built_symbols[symbol] = {
                "file": str(parquet_path.relative_to(self.out_dir.parent)),
                "source_type": source_type,
                "rows": len(df),
                "start_ts": df["ts"].min(),
                "end_ts": df["ts"].max(),
                "columns": list(df.columns),
            }
            
            return parquet_path
            
        except Exception as e:
            logger.error(f"Failed to build {symbol}: {e}")
            return None
    
    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names and types."""
        df = df.copy()
        
        # Rename common variations
        rename_map = {
            "timestamp": "ts",
            "date": "ts",
            "datetime": "ts",
            "Date": "ts",
            "open": "open",
            "Open": "open",
            "high": "high",
            "High": "high",
            "low": "low",
            "Low": "low",
            "close": "close",
            "Close": "close",
            "volume": "volume",
            "Volume": "volume",
            "vol": "volume",
            "amount": "amount",
            "Amount": "amount",
        }
        
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        
        # Ensure ts is datetime
        if "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"])
        
        return df
    
    def build_all(self, validate: bool = True) -> Dict[str, Path]:
        """Build curated data for all CSV files.
        
        Args:
            validate: Whether to validate outputs
            
        Returns:
            Dict mapping symbol -> parquet path
        """
        if not self.raw_dir.exists():
            raise FileNotFoundError(f"Input directory not found: {self.raw_dir}")

        csv_files = self._find_csv_files()
        
        if not csv_files:
            logger.warning(f"No CSV files found in {self.raw_dir}")
            return {}
        
        logger.info(f"Found {len(csv_files)} CSV files to process")
        
        results = {}
        for csv_path in csv_files:
            parquet_path = self.build_symbol(csv_path, validate=validate)
            if parquet_path:
                symbol = self._extract_symbol_from_path(csv_path)
                results[symbol] = parquet_path
        
        # Write index file
        if self.built_symbols:
            self._write_index()
        
        logger.info(f"Built {len(results)} curated files")
        return results
    
    def _write_index(self):
        """Write curated_index.json."""
        index_path = self.out_dir / self.index_file
        
        index_data = {
            "version": "curated_v1",
            "symbols": self.built_symbols,
            "count": len(self.built_symbols),
        }
        
        with open(index_path, 'w', encoding="utf-8") as f:
            json.dump(index_data, f, indent=2, default=str)
        
        logger.info(f"Wrote index to {index_path}")
    
    def load_index(self) -> Dict:
        """Load curated_index.json."""
        index_path = self.out_dir / self.index_file
        
        if not index_path.exists():
            logger.warning(f"Index file not found: {index_path}")
            return {}
        
        with open(index_path, 'r', encoding="utf-8") as f:
            return json.load(f)
    
    def get_symbol_path(self, symbol: str) -> Optional[Path]:
        """Get parquet path for a symbol from index."""
        index = self.load_index()
        symbol_info = index.get("symbols", {}).get(symbol)
        
        if not symbol_info:
            return None
        
        file_path = symbol_info.get("file")
        if file_path:
            return self.out_dir.parent / file_path
        
        return None


def build_curated_bars_from_csv_dir(
    in_dir: Path,
    out_dir: Path,
    index_path: Optional[Path] = None
) -> Dict[str, Path]:
    """High-level function to build curated bars from CSV directory.
    
    This is the main entry point for the data pipeline:
        scripts/update_data.py -> CSV files
        build_curated_bars_from_csv_dir() -> Parquet files + curated_index.json
    
    Args:
        in_dir: Input directory with CSV files
        out_dir: Output directory for parquet files
        index_path: Optional path for index file (default: out_dir/curated_index.json)
        
    Returns:
        Dict mapping symbol -> parquet path
        
    Example:
        >>> build_curated_bars_from_csv_dir(
        ...     Path("data/curated/bars"),
        ...     Path("data/curated/bars")
        ... )
    """
    builder = CuratedDataBuilder(
        raw_dir=in_dir,
        curated_root=out_dir,
        index_file=index_path.name if index_path else "curated_index.json"
    )
    
    return builder.build_all(validate=True)


if __name__ == "__main__":
    """CLI for building curated data."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Build curated parquet from CSV")
    parser.add_argument("--in-dir", default="data/raw/bars", help="Input CSV directory")
    parser.add_argument("--out-dir", default="data/curated/bars", help="Output parquet directory")
    parser.add_argument("--symbol", help="Build specific symbol only")
    parser.add_argument("--all", action="store_true", help="Build all symbols (explicit flag, default behavior)")
    parser.add_argument("--validate", action="store_true", default=True, help="Validate outputs")
    
    args = parser.parse_args()
    
    builder = CuratedDataBuilder(
        raw_dir=Path(args.in_dir),
        curated_root=Path(args.out_dir)
    )
    
    if args.symbol and not args.all:
        # Build single symbol
        csv_path = Path(args.in_dir) / f"{args.symbol}.csv"
        if not csv_path.exists():
            # Try with subdirectory
            for subdir in ["etf", "index", "stock"]:
                csv_path = Path(args.in_dir) / subdir / f"{args.symbol}.csv"
                if csv_path.exists():
                    break
        
        if csv_path.exists():
            result = builder.build_symbol(csv_path, validate=args.validate)
            print(f"Built: {result}")
        else:
            print(f"CSV not found for symbol: {args.symbol}")
    else:
        # Build all
        results = builder.build_all(validate=args.validate)
        print(f"Built {len(results)} files")
