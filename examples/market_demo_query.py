"""Market Data Demo - Query sample data.

This script:
1. Reads Universe listings
2. Queries bars for the first 2 listings
3. Prints statistics and metadata
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from loguru import logger

from quantlab.market import MarketStore


def main():
    """Query and display market data."""
    print("=" * 70)
    print("Market Data Demo - Query Sample Data")
    print("=" * 70)
    
    # Step 1: Initialize MarketStore
    print("\n[Step 1] Initializing MarketStore...")
    store = MarketStore(base_dir="data/market", universe_dir="data/universe")
    
    # Step 2: Load listings
    print("\n[Step 2] Loading Universe listings...")
    listings = store.load_universe_listings()
    
    if listings.empty:
        logger.error("No listings found. Run market_demo_seed.py first.")
        return 1
    
    print(f"Total listings: {len(listings)}")
    print("\nFirst 3 listings:")
    print(listings.head(3).to_string(index=False))
    
    # Step 3: Select target listings
    target_listings = listings.head(2)["listing_id"].tolist()
    print(f"\n[Step 3] Querying bars for: {target_listings}")
    
    # Step 4: Query bars
    # Query last 30 days from the sample data period
    end_date = datetime(2024, 2, 15)
    start_date = end_date - timedelta(days=30)
    
    print(f"\n[Step 4] Querying bars from {start_date.date()} to {end_date.date()}...")
    
    df = store.get_bars(
        listing_ids=target_listings,
        start=start_date,
        end=end_date,
        freq="1d",
        fields=["open", "high", "low", "close", "volume"]
    )
    
    if df.empty:
        logger.warning("No bars found. Run market_demo_seed.py first.")
        return 1
    
    # Step 5: Display results
    print("\n[Step 5] Query Results:")
    print("-" * 70)
    
    # Summary by listing
    print("\nRow count by listing:")
    counts = df.groupby("listing_id").size()
    for listing_id, count in counts.items():
        print(f"  {listing_id}: {count} bars")
    
    # Date range
    print(f"\nDate range: {df['ts'].min().date()} to {df['ts'].max().date()}")
    
    # Duplicate check by listing_id on ts
    print("\nDuplicate count by listing (same ts):")
    dup_counts = (
        df.groupby(["listing_id", "ts"]).size().sub(1).clip(lower=0)
        .groupby("listing_id").sum()
    )
    dup_counts = dup_counts.reindex(target_listings, fill_value=0).astype(int)
    for listing_id, dup_count in dup_counts.items():
        print(f"  {listing_id}: {dup_count}")

    # Sample data
    print("\nFirst 5 rows:")
    print(df.head().to_string(index=False))
    
    print("\nLast 5 rows:")
    print(df.tail().to_string(index=False))
    
    # Step 6: Display metadata
    print("\n[Step 6] Metadata:")
    print("-" * 70)
    metadata = store.load_metadata()
    
    if metadata.empty:
        print("  No metadata found.")
    else:
        for _, row in metadata.iterrows():
            print(f"\n  Listing: {row['listing_id']}")
            print(f"    Region: {row['region']}, Exchange: {row['exchange']}")
            print(f"    Coverage: {row['min_ts'].date()} to {row['max_ts'].date()}")
            print(f"    Last updated: {row['last_updated_at']}")
            print(f"    Status: {row['status']}")
    
    # Step 7: Directory structure
    print("\n[Step 7] Curated Storage Structure:")
    print("-" * 70)
    curated_dir = Path("data/market/curated")
    if curated_dir.exists():
        for path in sorted(curated_dir.rglob("*")):
            rel_path = path.relative_to(curated_dir)
            if path.is_file():
                size_kb = path.stat().st_size / 1024
                print(f"  {rel_path} ({size_kb:.1f} KB)")
            else:
                print(f"  {rel_path}/")
    
    print("说明: last_updated_at 会在每次 seed 写入时刷新；query 仅查询不会刷新。")

    print("\n" + "=" * 70)
    print("Query complete!")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
