"""Example: Compare different rebalancing frequencies.

Demonstrates how to compare multiple backtest runs.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from loguru import logger

from quantlab.backtest.engine import BacktestEngine
from quantlab.data.ingest import DataIngestor
from quantlab.data.sources.local_csv import MockDataSource
from quantlab.research.strategies.base import EqualWeightStrategy


def run_with_frequency(freq: str, data, symbols):
    """Run backtest with given frequency."""
    logger.info(f"Running with frequency: {freq}")
    
    strategy = EqualWeightStrategy(symbols=symbols)
    engine = BacktestEngine(
        strategy=strategy,
        initial_cash=1_000_000.0,
        fee_model="us_etfs"
    )
    
    results = engine.run(
        data=data,
        rebalance_freq=freq,
        progress=False
    )
    
    return results, engine


def main():
    """Compare rebalancing frequencies."""
    logger.info("=" * 60)
    logger.info("Rebalancing Frequency Comparison")
    logger.info("=" * 60)
    
    # Setup
    symbols = ["SPY", "TLT", "GLD"]
    start = datetime(2020, 1, 1)
    end = datetime(2024, 12, 31)
    
    # Ingest data once
    data_source = MockDataSource(symbols=symbols)
    ingestor = DataIngestor(data_source, "data/curated")
    data = ingestor.ingest(symbols, start, end)
    
    # Test different frequencies
    frequencies = ["M", "Q"]  # Monthly, Quarterly
    results_dict = {}
    
    for freq in frequencies:
        results, engine = run_with_frequency(freq, data, symbols)
        results_dict[freq] = results
        engine.save_run(results, run_id=f"compare_{freq}")
    
    # Compare results
    print("\n" + "=" * 80)
    print("COMPARISON RESULTS")
    print("=" * 80)
    print(f"{'Freq':<10} {'Return':>10} {'Sharpe':>10} {'Max DD':>10} {'Trades':>10}")
    print("-" * 80)
    
    for freq, results in results_dict.items():
        summary = results["metrics"].get("summary", {})
        print(
            f"{freq:<10} "
            f"{summary.get('total_return', 0):>9.2%} "
            f"{summary.get('sharpe_ratio', 0):>10.2f} "
            f"{summary.get('max_drawdown', 0):>9.2%} "
            f"{results['metrics'].get('trading', {}).get('total_trades', 0):>10}"
        )
    
    print("=" * 80)
    print("\nNote: Higher frequency = more trades = higher transaction costs")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
