# QuantLab Examples

This directory contains example scripts demonstrating various features of QuantLab.

## Available Examples

### 1. Simple Backtest (`run_simple_backtest.py`)

Basic equal-weight portfolio backtest with monthly rebalancing.

```bash
cd quantlab
python examples/run_simple_backtest.py
```

**What it demonstrates:**
- Data ingestion from mock source
- Strategy creation
- Backtest engine usage
- Results analysis and reporting

### 2. Compare Rebalance Frequencies (`compare_rebalance_freq.py`)

Compare monthly vs quarterly rebalancing strategies.

```bash
cd quantlab
python examples/compare_rebalance_freq.py
```

**What it demonstrates:**
- Multiple backtest runs
- Results comparison
- Transaction cost impact analysis

## Creating Your Own Example

Template for new examples:

```python
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from quantlab.backtest.engine import BacktestEngine
from quantlab.data.sources.local_csv import MockDataSource
from quantlab.research.strategies.base import EqualWeightStrategy

# 1. Setup data
# 2. Create strategy
# 3. Run backtest
# 4. Analyze results
```

## Data Sources

Examples use `MockDataSource` for reproducibility. For real data:

1. Add data to `data/raw/`
2. Use `LocalCSVDataSource` instead
3. Or implement a custom data source

```python
from quantlab.data.sources.local_csv import LocalCSVDataSource

data_source = LocalCSVDataSource("data/raw")
```
