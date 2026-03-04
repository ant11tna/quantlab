"""Market Data Demo - Seed sample data.

This script:
1. Checks for Universe listings
2. Generates sample OHLCV data for 2-3 listings
3. Writes to curated storage via MarketStore
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
import numpy as np
from loguru import logger

from quantlab.market import MarketStore


def create_sample_universe() -> None:
    """Create sample universe listings if not exists."""
    universe_dir = Path("data/universe")
    listings_path = universe_dir / "listings.parquet"
    
    if listings_path.exists():
        return
    
    logger.info("Creating sample universe listings...")
    universe_dir.mkdir(parents=True, exist_ok=True)
    
    # Sample listings: 2 US stocks, 1 ETF
    listings = pd.DataFrame({
        "listing_id": ["AAPL", "MSFT", "SPY"],
        "region": ["US", "US", "US"],
        "exchange": ["NASDAQ", "NASDAQ", "NYSE"],
        "currency": ["USD", "USD", "USD"],
        "name": ["Apple Inc", "Microsoft Corp", "SPDR S&P 500 ETF"],
    })
    
    listings.to_parquet(listings_path, index=False)
    logger.info(f"Created {listings_path} with {len(listings)} listings")


def generate_sample_bars(
    listing_id: str,
    start_date: datetime,
    days: int = 30,
    seed: int = 42
) -> pd.DataFrame:
    """Generate sample OHLCV bars for a listing.
    
    Args:
        listing_id: Asset identifier
        start_date: Start date
        days: Number of trading days
        seed: Random seed
        
    Returns:
        DataFrame with OHLCV bars
    """
    np.random.seed(seed + hash(listing_id) % 1000)
    
    # Generate trading days (skip weekends)
    dates = []
    current = start_date
    while len(dates) < days:
        if current.weekday() < 5:  # Mon-Fri
            dates.append(current)
        current += timedelta(days=1)
    
    # Generate price series with random walk
    base_price = {"AAPL": 150.0, "MSFT": 300.0, "SPY": 400.0}.get(listing_id, 100.0)
    volatility = 0.02  # 2% daily volatility
    
    prices = [base_price]
    for _ in range(1, days):
        change = np.random.normal(0, volatility)
        new_price = prices[-1] * (1 + change)
        prices.append(new_price)
    
    # Generate OHLC from close prices
    data = []
    for i, (date, close) in enumerate(zip(dates, prices)):
        # Intraday range
        daily_range = close * volatility * np.random.uniform(0.5, 1.5)
        high = close + daily_range * np.random.uniform(0.3, 0.7)
        low = close - daily_range * np.random.uniform(0.3, 0.7)
        open_price = low + (high - low) * np.random.uniform(0.2, 0.8)
        
        # Volume (random)
        volume = int(np.random.uniform(1e6, 10e6))
        
        data.append({
            "ts": date,
            "listing_id": listing_id,
            "open": round(open_price, 2),
            "high": round(high, 2),
            "low": round(low, 2),
            "close": round(close, 2),
            "volume": volume,
        })
    
    return pd.DataFrame(data)


def main():
    """Seed sample market data."""
    print("=" * 70)
    print("Market Data Demo - Seed Sample Data")
    print("=" * 70)
    
    # Step 1: Ensure universe exists
    print("\n[Step 1] Checking Universe...")
    create_sample_universe()
    
    # Step 2: Initialize MarketStore
    print("\n[Step 2] Initializing MarketStore...")
    store = MarketStore(base_dir="data/market", universe_dir="data/universe")
    
    # Load listings
    listings = store.load_universe_listings()
    if listings.empty:
        logger.error("No listings found in universe. Please create data/universe/listings.parquet")
        return 1
    
    print(f"Found {len(listings)} listings in universe")
    print(f"  - {listings['listing_id'].tolist()}")
    
    # Step 3: Generate and write sample data
    print("\n[Step 3] Generating sample bars...")
    
    # Use first 2 listings
    target_listings = listings.head(2)
    start_date = datetime(2024, 1, 1)
    
    for _, row in target_listings.iterrows():
        listing_id = row["listing_id"]
        print(f"\n  Generating data for {listing_id}...")
        
        df = generate_sample_bars(listing_id, start_date, days=30)
        print(f"    Generated {len(df)} bars ({df['ts'].min().date()} to {df['ts'].max().date()})")
        
        # Write to curated
        store.write_curated(df, freq="1d", adj="none")
        print(f"    [OK] Written to curated storage")
    
    # Step 4: Verify metadata
    print("\n[Step 4] Verifying metadata...")
    metadata = store.load_metadata()
    print(f"  Metadata entries: {len(metadata)}")
    for _, row in metadata.iterrows():
        print(f"    - {row['listing_id']}: {row['min_ts'].date()} to {row['max_ts'].date()}")
        print(f"      last_updated_at: {row['last_updated_at']}")
    
    # Step 5: Show directory structure
    print("\n[Step 5] Curated data structure:")
    curated_dir = Path("data/market/curated")
    for path in sorted(curated_dir.rglob("*.parquet")):
        rel_path = path.relative_to(curated_dir)
        print(f"  {rel_path}")
    
    print("\n" + "=" * 70)
    print("Seed complete! Run market_demo_query.py to query the data.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
