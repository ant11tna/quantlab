"""Example: Simple equal-weight backtest.

Demonstrates the basic usage of the backtest engine.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
from loguru import logger

from quantlab.backtest.engine import BacktestEngine
from quantlab.data.ingest import DataIngestor
from quantlab.data.sources.local_csv import MockDataSource
from quantlab.research.strategies.base import EqualWeightStrategy
from quantlab.research.reports import ReportGenerator
from quantlab.research.risk import RiskAnalyzer


def main() -> None:
    """Run example backtest."""
    logger.info("=" * 60)
    logger.info("QuantLab Simple Backtest Example")
    logger.info("=" * 60)
    
    # Configuration
    symbols = ["SPY", "TLT", "GLD", "VEA", "VWO"]
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 12, 31)
    initial_cash = 1_000_000.0
    
    logger.info(f"Symbols: {symbols}")
    logger.info(f"Period: {start_date.date()} to {end_date.date()}")
    logger.info(f"Initial Capital: ${initial_cash:,.2f}")
    
    # Step 1: Create data source and ingest data
    logger.info("\n[1/4] Ingesting data...")
    data_source = MockDataSource(symbols=symbols)
    ingestor = DataIngestor(data_source, "data/curated")
    
    data = ingestor.ingest(symbols, start_date, end_date)
    logger.info(f"Ingested {len(data)} rows")
    
    # Step 2: Create strategy
    logger.info("\n[2/4] Creating strategy...")
    strategy = EqualWeightStrategy(symbols=symbols)
    logger.info(f"Strategy: {strategy.name}")
    
    # Step 3: Run backtest
    logger.info("\n[3/4] Running backtest...")
    engine = BacktestEngine(
        strategy=strategy,
        initial_cash=initial_cash,
        fee_model="us_etfs"
    )
    
    results = engine.run(
        data=data,
        rebalance_freq="M",  # Monthly rebalancing
        progress=True
    )
    
    # Step 4: Analyze results
    logger.info("\n[4/4] Analyzing results...")
    
    equity_df = results["equity_curve"]
    trades_df = results["trades"]
    metrics = results["metrics"]
    
    # Calculate risk metrics
    risk_analyzer = RiskAnalyzer()
    returns = equity_df["nav"].pct_change().dropna()
    risk_metrics = risk_analyzer.calculate_metrics(returns)
    
    # Print summary
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY")
    print("=" * 60)
    print(f"Final NAV:        ${equity_df['nav'].iloc[-1]:,.2f}")
    print(f"Total Return:     {risk_metrics.total_return:.2%}")
    print(f"Annualized Return: {risk_metrics.annualized_return:.2%}")
    print(f"Volatility:       {risk_metrics.annualized_volatility:.2%}")
    print(f"Sharpe Ratio:     {risk_metrics.sharpe_ratio:.2f}")
    print(f"Max Drawdown:     {risk_metrics.max_drawdown:.2%}")
    print(f"Total Trades:     {len(trades_df)}")
    print("=" * 60)
    
    # Save results
    run_dir = engine.save_run(results)
    logger.info(f"\nResults saved to: {run_dir}")
    
    # Generate report
    report_gen = ReportGenerator(run_dir)
    from quantlab.core.types import RunConfig
    config = RunConfig(
        run_id=run_dir.name,
        universe="example",
        start_date=start_date,
        end_date=end_date,
        rebalance_freq="M",
        fee_model="us_etfs"
    )
    
    report_path = report_gen.generate_report(
        run_id=run_dir.name,
        config=config,
        metrics=risk_metrics,
        trades_df=trades_df,
        equity_df=equity_df
    )
    logger.info(f"Report saved to: {report_path}")
    
    logger.info("\nDone!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
