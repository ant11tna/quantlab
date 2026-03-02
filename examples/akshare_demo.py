"""AKShare Data Source Demo

Demonstrates how to use AKShare for Chinese market data.
"""
from __future__ import annotations
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
from loguru import logger

# Remove default handler and add simple one
logger.remove()
logger.add(sys.stdout, level="INFO")


def demo_single_stock():
    """Demo: Fetch single stock data."""
    print("\n" + "=" * 70)
    print("Demo 1: Single Stock Data (000001 平安银行)")
    print("=" * 70)
    
    from quantlab.data.sources.akshare_source import AKShareDataSource
    
    source = AKShareDataSource()
    
    # Fetch last 30 days
    end = datetime.now()
    start = end - timedelta(days=30)
    
    df = source.get_bars("000001", start, end)
    
    if df.empty:
        print("No data returned. Check network connection.")
        return
    
    print(f"\nFetched {len(df)} days of data:")
    print(df.head(10).to_string())
    print("\nLatest price:", df["close"].iloc[-1])


def demo_index():
    """Demo: Fetch index data."""
    print("\n" + "=" * 70)
    print("Demo 2: Index Data (000001 上证指数)")
    print("=" * 70)
    
    from quantlab.data.sources.akshare_source import AKShareDataSource, INDEX_SYMBOLS
    
    source = AKShareDataSource()
    
    end = datetime.now()
    start = end - timedelta(days=30)
    
    print(f"\nAvailable indices: {list(INDEX_SYMBOLS.keys())}")
    
    # Fetch Shanghai Composite Index
    df = source.get_index_bars("000001", start, end)
    
    if df.empty:
        print("No index data returned.")
        return
    
    print(f"\nFetched {len(df)} days of index data:")
    print(df.head(5).to_string())


def demo_stock_info():
    """Demo: Get stock basic info."""
    print("\n" + "=" * 70)
    print("Demo 3: Stock Information")
    print("=" * 70)
    
    from quantlab.data.sources.akshare_source import AKShareDataSource
    
    source = AKShareDataSource()
    
    # Get info for Ping An Bank
    info = source.get_stock_info("000001")
    
    print(f"\nStock Info (000001):")
    for key, value in info.items():
        print(f"  {key}: {value}")


def demo_multiple_stocks():
    """Demo: Fetch multiple stocks."""
    print("\n" + "=" * 70)
    print("Demo 4: Multiple Stocks")
    print("=" * 70)
    
    from quantlab.data.sources.akshare_source import fetch_multiple_stocks
    
    # Some popular A-shares
    symbols = ["000001", "000002", "600519"]  # 平安银行, 万科A, 贵州茅台
    
    end = datetime.now()
    start = end - timedelta(days=10)
    
    print(f"\nFetching {len(symbols)} stocks...")
    
    df = fetch_multiple_stocks(symbols, start, end, progress=True)
    
    if df.empty:
        print("No data returned.")
        return
    
    print(f"\nCombined data shape: {df.shape}")
    print("\nData preview:")
    print(df.groupby("symbol")[["open", "close", "volume"]].last())


def demo_save_to_parquet():
    """Demo: Save fetched data to parquet."""
    print("\n" + "=" * 70)
    print("Demo 5: Save to Parquet")
    print("=" * 70)
    
    from quantlab.data.sources.akshare_source import AKShareDataSource
    
    source = AKShareDataSource()
    
    end = datetime.now()
    start = end - timedelta(days=60)
    
    # Fetch data
    df = source.get_bars("000001", start, end)
    
    if df.empty:
        print("No data to save.")
        return
    
    # Save to parquet
    output_path = Path("data/raw/akshare_000001.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_parquet(output_path, index=False)
    print(f"\nSaved {len(df)} rows to {output_path}")
    
    # Verify by reading back
    df_read = pd.read_parquet(output_path)
    print(f"Verified: read back {len(df_read)} rows")


def demo_usage_in_backtest():
    """Demo: How to use in backtest."""
    print("\n" + "=" * 70)
    print("Demo 6: Usage in Backtest")
    print("=" * 70)
    
    from quantlab.data.sources.akshare_source import AKShareDataSource, INDEX_SYMBOLS
    from quantlab.data.ingest import DataIngestor
    
    print("\nExample code for backtest:")
    print("""
    # 1. Create data source
    source = AKShareDataSource()
    
    # 2. Define universe (Chinese stocks)
    symbols = ["000001", "000002", "600519"]  # 平安银行, 万科A, 茅台
    
    # 3. Ingest data
    ingestor = DataIngestor(source, "data/curated")
    data = ingestor.ingest(symbols, start, end)
    
    # 4. Run backtest
    engine = BacktestEngine(strategy=strategy, ...)
    results = engine.run(data=data, ...)
    """)
    
    print("\nChinese stock universe examples:")
    print("  Blue chips: 600519 (茅台), 000858 (五粮液), 000333 (美的)")
    print("  Banks: 000001 (平安), 600036 (招行), 601398 (工行)")
    print("  Tech: 002415 (海康), 000725 (京东方), 300750 (宁德时代)")
    print("\nIndices for benchmarking:")
    for name, code in INDEX_SYMBOLS.items():
        print(f"  {code}: {name}")


def main():
    """Run all demos."""
    print("\n" + "=" * 70)
    print("AKShare Data Source Demo")
    print("=" * 70)
    print("\nAKShare provides free Chinese market data including:")
    print("  - A-shares (沪深A股)")
    print("  - Indices (上证指数, 深证成指, 创业板指, etc.)")
    print("  - ETFs and funds")
    print("  - Futures and options")
    print("\nNote: Requires internet connection to AKShare servers")
    
    demos = [
        ("Single Stock", demo_single_stock),
        ("Index Data", demo_index),
        ("Stock Info", demo_stock_info),
        ("Multiple Stocks", demo_multiple_stocks),
        ("Save to Parquet", demo_save_to_parquet),
        ("Backtest Usage", demo_usage_in_backtest),
    ]
    
    for name, func in demos:
        try:
            func()
        except Exception as e:
            print(f"\n[ERROR] {name} failed: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 70)
    print("Demo completed!")
    print("=" * 70)


if __name__ == "__main__":
    main()
