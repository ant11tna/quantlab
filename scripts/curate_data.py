"""Data curation CLI - Transform raw CSV to curated Parquet with regime fields.

Usage:
    python scripts/curate_data.py --in-dir data/raw/bars --out-dir data/curated/bars
    python scripts/curate_data.py --symbol ETF:510300  # Curate single symbol
    python scripts/curate_data.py --all  # Curate all symbols

This script reads raw OHLCV CSV files, applies curated transforms (adds regime fields
like is_suspended, can_buy, can_sell), and writes optimized Parquet files.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from loguru import logger

from quantlab.data.curate import (
    CuratedDataBuilder,
    build_curated_bars_from_csv_dir,
)


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    logger.remove()
    level = "DEBUG" if verbose else "INFO"
    logger.add(sys.stderr, level=level)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Curate raw OHLCV data to parquet with regime fields",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Curate all raw data
  python scripts/curate_data.py --all
  
  # Curate specific symbol
  python scripts/curate_data.py --symbol ETF:510300
  
  # Curate with custom paths
  python scripts/curate_data.py --in-dir data/raw/bars --out-dir data/curated/bars
  
  # Dry run (validate without writing)
  python scripts/curate_data.py --all --dry-run
        """
    )
    
    parser.add_argument(
        "--in-dir",
        type=Path,
        default=Path("data/raw/bars"),
        help="Input directory with raw CSV files (default: data/raw/bars)"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/curated/bars"),
        help="Output directory for curated parquet files (default: data/curated/bars)"
    )
    parser.add_argument(
        "--symbol",
        type=str,
        help="Curate specific symbol only (e.g., ETF:510300, IDX:000300)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Curate all symbols found in input directory"
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        default=True,
        help="Validate output against curated_v1 schema (default: True)"
    )
    parser.add_argument(
        "--no-validate",
        dest="validate",
        action="store_false",
        help="Skip validation"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate without writing files"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    setup_logging(args.verbose)
    
    if not args.symbol and not args.all:
        parser.error("Must specify --symbol or --all")
    
    if args.dry_run:
        logger.info("DRY RUN: Validating without writing files")
    
    # Ensure directories exist
    if not args.in_dir.exists():
        logger.error(f"Input directory not found: {args.in_dir}")
        sys.exit(1)
    
    if not args.dry_run:
        args.out_dir.mkdir(parents=True, exist_ok=True)
    
    success = True

    # Build curated data
    if args.symbol:
        # Single symbol mode
        logger.info(f"Curating single symbol: {args.symbol}")
        
        # Find CSV file
        csv_path = None
        for subdir in ["etf", "index", "stock"]:
            candidate = args.in_dir / subdir / f"{args.symbol}.csv"
            if candidate.exists():
                csv_path = candidate
                break
            # Try with underscore separator
            candidate2 = args.in_dir / subdir / f"{args.symbol.replace(':', '_')}.csv"
            if candidate2.exists():
                csv_path = candidate2
                break
        
        if not csv_path:
            # Try root of in_dir
            csv_path = args.in_dir / f"{args.symbol}.csv"
            if not csv_path.exists():
                csv_path = args.in_dir / f"{args.symbol.replace(':', '_')}.csv"
        
        if not csv_path or not csv_path.exists():
            logger.error(f"CSV file not found for symbol: {args.symbol}")
            sys.exit(1)
        
        if args.dry_run:
            logger.info(f"Would curate: {csv_path}")
        else:
            builder = CuratedDataBuilder(
                raw_dir=args.in_dir,
                out_dir=args.out_dir
            )
            result = builder.build_symbol(csv_path, validate=args.validate)
            if result:
                logger.info(f"Successfully curated: {result}")
            else:
                logger.error(f"Failed to curate: {args.symbol}")
                success = False
    
    else:
        # Batch mode
        logger.info(f"Curating all symbols from {args.in_dir}")
        
        if args.dry_run:
            # Find all CSV files
            csv_files = []
            for subdir in args.in_dir.iterdir():
                if subdir.is_dir():
                    csv_files.extend(subdir.glob("*.csv"))
            csv_files.extend(args.in_dir.glob("*.csv"))
            
            logger.info(f"Would process {len(csv_files)} files:")
            for f in csv_files[:10]:  # Show first 10
                logger.info(f"  - {f.relative_to(args.in_dir)}")
            if len(csv_files) > 10:
                logger.info(f"  ... and {len(csv_files) - 10} more")
        else:
            results = build_curated_bars_from_csv_dir(
                in_dir=args.in_dir,
                out_dir=args.out_dir,
                index_path=args.out_dir / "curated_index.json"
            )
            
            if results:
                logger.info(f"Successfully curated {len(results)} symbols")
                for symbol, path in sorted(results.items())[:5]:
                    logger.info(f"  - {symbol}: {path}")
                if len(results) > 5:
                    logger.info(f"  ... and {len(results) - 5} more")
            else:
                logger.warning("No symbols were curated")

    if not args.dry_run and success:
        build_script = Path(__file__).resolve().parent / "build_data_version.py"
        try:
            subprocess.run([sys.executable, str(build_script)], check=True)
            logger.info("Updated data/data_version.json")
        except subprocess.CalledProcessError as exc:
            logger.error(f"Failed to update data version metadata: {exc}")
            return 1

    if not success:
        return 1
    
    logger.info("Curation complete!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
