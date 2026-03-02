"""Capacity Test Module

Test strategy capacity at different capital levels.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import pandas as pd
from loguru import logger


@dataclass
class CapacityTestResult:
    """Result of capacity test at specific capital level."""
    initial_capital: float
    total_return: float
    total_fees: float
    total_impact_cost: float
    impact_cost_ratio: float
    turnover: float
    max_drawdown: float
    sharpe_ratio: float
    num_trades: int


def run_capacity_test(
    config_path: str,
    capital_levels: List[float] = None
) -> Dict[float, CapacityTestResult]:
    """Run backtest at different capital levels.
    
    Args:
        config_path: Path to config YAML
        capital_levels: List of initial capital levels to test
        
    Returns:
        Dict mapping capital level to test result
    """
    if capital_levels is None:
        capital_levels = [1_000_000, 10_000_000, 100_000_000]  # 100万, 1000万, 1亿
    
    results = {}
    
    for capital in capital_levels:
        logger.info(f"\n{'='*60}")
        logger.info(f"Testing capacity: ${capital:,.0f}")
        logger.info(f"{'='*60}")
        
        # Run backtest with this capital
        result = _run_single_capital(config_path, capital)
        results[capital] = result
    
    # Generate capacity curve
    _plot_capacity_curve(results)
    
    return results


def _run_single_capital(config_path: str, capital: float) -> CapacityTestResult:
    """Run backtest with specific capital level."""
    from quantlab.backtest.engine import BacktestEngine
    from quantlab.data.sources.local_csv import MockDataSource
    from quantlab.research.strategies.base import EqualWeightStrategy
    
    # Setup
    symbols = ["SPY", "TLT", "GLD"]
    data_source = MockDataSource(symbols=symbols)
    
    from quantlab.data.ingest import DataIngestor
    from datetime import datetime
    
    ingestor = DataIngestor(data_source, "data/curated")
    data = ingestor.ingest(symbols, datetime(2020, 1, 1), datetime(2024, 12, 31))
    
    strategy = EqualWeightStrategy(symbols=symbols)
    
    # Run with specific capital
    engine = BacktestEngine(
        strategy=strategy,
        initial_cash=capital,
        fee_model="us_etfs"
    )
    
    results = engine.run(data=data, rebalance_freq="M", progress=False)
    
    # Extract metrics
    metrics = results["metrics"]
    summary = metrics.get("summary", {})
    trading = metrics.get("trading", {})
    
    return CapacityTestResult(
        initial_capital=capital,
        total_return=summary.get("total_return", 0),
        total_fees=trading.get("total_fees", 0),
        total_impact_cost=trading.get("total_impact_cost", 0),
        impact_cost_ratio=trading.get("impact_cost_ratio", 0),
        turnover=trading.get("turnover", 0),
        max_drawdown=summary.get("max_drawdown", 0),
        sharpe_ratio=summary.get("sharpe_ratio", 0),
        num_trades=trading.get("total_trades", 0)
    )


def _plot_capacity_curve(results: Dict[float, CapacityTestResult]):
    """Generate capacity curve report."""
    print("\n" + "="*80)
    print("CAPACITY TEST RESULTS")
    print("="*80)
    
    # Create comparison table
    data = []
    for capital, result in sorted(results.items()):
        data.append({
            "Capital ($)": f"{capital:,.0f}",
            "Return (%)": f"{result.total_return:.2%}",
            "Sharpe": f"{result.sharpe_ratio:.2f}",
            "Max DD (%)": f"{result.max_drawdown:.2%}",
            "Fees ($)": f"{result.total_fees:,.0f}",
            "Impact Cost (%)": f"{result.impact_cost_ratio:.4%}",
            "Turnover": f"{result.turnover:.2f}",
            "Trades": result.num_trades
        })
    
    df = pd.DataFrame(data)
    print("\n", df.to_string(index=False))
    
    # Calculate degradation
    if len(results) >= 2:
        capitals = sorted(results.keys())
        small = results[capitals[0]]
        large = results[capitals[-1]]
        
        print("\n" + "-"*80)
        print("CAPACITY DEGRADATION ANALYSIS")
        print("-"*80)
        
        return_degradation = small.total_return - large.total_return
        print(f"Return degradation (small → large): {return_degradation:.2%}")
        
        impact_increase = large.impact_cost_ratio - small.impact_cost_ratio
        print(f"Impact cost increase: {impact_increase:.4%}")
        
        if return_degradation > 0.05:  # More than 5% degradation
            print("\n⚠️  WARNING: Significant capacity constraints detected!")
            print("   Strategy may not scale well to large capital.")
        else:
            print("\n✓ Strategy scales reasonably well.")


def main():
    """Run capacity test example."""
    print("Capacity Test Example")
    print("Testing strategy at: 100万, 1000万, 1亿")
    
    results = run_capacity_test(
        config_path="config/backtest.yaml",
        capital_levels=[1_000_000, 10_000_000, 100_000_000]
    )
    
    # Save results
    output = {
        str(k): {
            "initial_capital": v.initial_capital,
            "total_return": v.total_return,
            "impact_cost_ratio": v.impact_cost_ratio,
            "max_drawdown": v.max_drawdown
        }
        for k, v in results.items()
    }
    
    output_path = Path("runs/capacity_test_results.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"\nResults saved to {output_path}")


if __name__ == "__main__":
    main()
