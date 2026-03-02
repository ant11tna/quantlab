"""AKShare Data Workflow Example

Demonstrates the complete workflow:
1. Update data from AkShare
2. Load data for backtest
3. Run backtest with ETF/Index data
"""
from __future__ import annotations
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

print("=" * 70)
print("AKShare Data Workflow Demo")
print("=" * 70)
print("""
This demo shows the complete workflow for using AkShare data:

Step 1: Update Data
-------------------
python scripts/update_data.py --config config/data_sources.yaml

This will:
- Fetch ETF data (510300, 510500, etc.)
- Fetch Index data (000300, 000905, etc.)
- Save to data/curated/bars/etf/ and data/curated/bars/index/
- Update manifest files

Step 2: Verify Data
-------------------
Check data/curated/bars/:
- ETF:ETF:510300.csv
- IDX:IDX:000300.csv

Step 3: Use in Backtest
-----------------------
""")

# Example: How to load curated data
def demo_load_curated_data():
    """Demo loading data from curated directory."""
    print("Demo: Loading Curated Data")
    print("-" * 70)
    
    import pandas as pd
    
    # Example paths
    etf_path = Path("data/curated/bars/etf/ETF:510300.csv")
    index_path = Path("data/curated/bars/index/IDX:000300.csv")
    
    print(f"ETF path: {etf_path}")
    print(f"Index path: {index_path}")
    
    # Check if files exist
    if etf_path.exists():
        df_etf = pd.read_csv(etf_path)
        print(f"\nETF:510300 loaded: {len(df_etf)} rows")
        print(f"Date range: {df_etf['ts'].min()} to {df_etf['ts'].max()}")
        print(f"\nSample data:")
        print(df_etf.tail(3).to_string())
    else:
        print(f"\n[Not found] Run: python scripts/update_data.py")
    
    if index_path.exists():
        df_idx = pd.read_csv(index_path)
        print(f"\n\nIDX:000300 loaded: {len(df_idx)} rows")
        print(f"Date range: {df_idx['ts'].min()} to {df_idx['ts'].max()}")
    else:
        print(f"\n[Not found] Run: python scripts/update_data.py")


def demo_data_format():
    """Demo unified data format."""
    print("\n" + "=" * 70)
    print("Unified Data Format")
    print("=" * 70)
    
    print("""
All data is normalized to the same format:

Columns:
- ts: YYYY-MM-DD (string)
- symbol: ETF:510300 or IDX:000300
- open: float
- high: float
- low: float
- close: float
- volume: float
- amount: float (optional)

Example:
""")
    
    import pandas as pd
    
    example = pd.DataFrame({
        "ts": ["2024-01-15", "2024-01-16", "2024-01-17"],
        "symbol": ["ETF:510300", "ETF:510300", "ETF:510300"],
        "open": [3.85, 3.87, 3.90],
        "high": [3.88, 3.91, 3.95],
        "low": [3.84, 3.86, 3.89],
        "close": [3.87, 3.90, 3.93],
        "volume": [1500000, 1800000, 2100000],
        "amount": [5805000.0, 7020000.0, 8253000.0]
    })
    
    print(example.to_string())
    print("\n✓ This format is directly usable by BacktestEngine")


def demo_usage_in_backtest():
    """Demo how to use in backtest."""
    print("\n" + "=" * 70)
    print("Usage in Backtest")
    print("=" * 70)
    
    print("""
Code example:

```python
from pathlib import Path
import pandas as pd
from quantlab.backtest.engine import BacktestEngine
from quantlab.research.strategies.base import EqualWeightStrategy

# 1. Load curated data
def load_curated_universe(symbols: list[str]) -> pd.DataFrame:
    all_data = []
    for symbol in symbols:
        # Determine path based on prefix
        if symbol.startswith("ETF:"):
            path = Path(f"data/curated/bars/etf/{symbol}.csv")
        elif symbol.startswith("IDX:"):
            path = Path(f"data/curated/bars/index/{symbol}.csv")
        else:
            continue
        
        if path.exists():
            df = pd.read_csv(path)
            all_data.append(df)
    
    return pd.concat(all_data, ignore_index=True)

# 2. Define universe
etf_universe = [
    "ETF:510300",   # 沪深300ETF
    "ETF:510500",   # 中证500ETF
    "ETF:518880",   # 黄金ETF
    "ETF:511010",   # 国债ETF
]

# 3. Load data
data = load_curated_universe(etf_universe)

# 4. Create strategy
strategy = EqualWeightStrategy(
    symbols=[s.split(":")[1] for s in etf_universe]
)

# 5. Run backtest
engine = BacktestEngine(strategy=strategy, initial_cash=1_000_000)
results = engine.run(data=data, rebalance_freq="M")
```
""")


def demo_update_workflow():
    """Demo update workflow."""
    print("\n" + "=" * 70)
    print("Update Workflow Commands")
    print("=" * 70)
    
    print("""
# Full update (all symbols)
python scripts/update_data.py --config config/data_sources.yaml

# Update only ETFs
python scripts/update_data.py --type etf

# Update only Indices
python scripts/update_data.py --type index

# Force full refresh (ignore existing)
python scripts/update_data.py --force

# Update specific symbol
python scripts/update_data.py --symbol ETF:510300
""")


def demo_manifest():
    """Demo manifest files."""
    print("\n" + "=" * 70)
    print("Manifest Files (Data Watermark)")
    print("=" * 70)
    
    print("""
After update, manifest files track last timestamp per symbol:

data/manifest/etf.json:
{
  "ETF:510300": "2024-02-28",
  "ETF:510500": "2024-02-28",
  ...
}

data/manifest/index.json:
{
  "IDX:000300": "2024-02-28",
  "IDX:000905": "2024-02-28",
  ...
}

Purpose:
- Incremental updates (only fetch new data)
- Audit trail
- Avoid redundant API calls
""")


def main():
    print("\nRunning workflow demos...\n")
    
    demo_data_format()
    demo_load_curated_data()
    demo_usage_in_backtest()
    demo_update_workflow()
    demo_manifest()
    
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print("""
1. Configure symbols in: config/data_sources.yaml
2. Update data: python scripts/update_data.py
3. Data location: data/curated/bars/{etf,index}/
4. Use in backtest: Load CSV -> pd.DataFrame -> BacktestEngine

Next steps:
- Edit config/data_sources.yaml to add your target ETFs
- Run: python scripts/update_data.py
- Check: data/curated/bars/etf/
""")


if __name__ == "__main__":
    main()
