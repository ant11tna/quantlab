"""Risk Constraints Integration Demo

Demonstrates how to use RiskConstraintChecker with BacktestEngine,
including configuration from YAML and saving run metadata.
"""

import sys
sys.path.insert(0, "src")

import pandas as pd
from loguru import logger

from quantlab.backtest import create_engine_from_config
from quantlab.research.strategies.base import Strategy
from quantlab.core.types import TargetWeight


class DemoStrategy(Strategy):
    """Simple demo strategy that produces aggressive weights to test constraints."""
    
    def __init__(self):
        super().__init__("demo_aggressive")
    
    def generate_targets(self, data, current_time, current_weights=None):
        """Generate targets - intentionally aggressive to trigger constraints."""
        from datetime import datetime
        
        # Get latest available symbols
        latest = data[data["ts"] == data["ts"].max()]
        symbols = latest["symbol"].unique().tolist()
        
        if not symbols:
            return []
        
        # Try to allocate 60% to first asset (will trigger max_position_weight=0.50)
        targets = []
        if len(symbols) >= 1:
            targets.append(TargetWeight(
                ts=current_time,
                symbol=symbols[0], 
                target_weight=0.60,  # Violates 50% limit
                source=self.name
            ))
        if len(symbols) >= 2:
            targets.append(TargetWeight(
                ts=current_time,
                symbol=symbols[1], 
                target_weight=0.40,
                source=self.name
            ))
        
        return targets


def main():
    logger.info("=" * 60)
    logger.info("Risk Constraints Integration Demo")
    logger.info("=" * 60)
    
    # 1. Load engine with risk constraints from YAML
    strategy = DemoStrategy()
    engine = create_engine_from_config(
        config_path="config/backtest.yaml",
        strategy=strategy,
        config_name="default"
    )
    
    logger.info(f"Engine created with constraint_mode: {engine.constraint_mode}")
    logger.info(f"Risk config: {engine.risk_config}")
    
    # 2. Create synthetic test data
    dates = pd.date_range("2024-01-01", "2024-03-01", freq="B")
    data = []
    for ts in dates:
        for symbol, price in [("AAPL", 150.0), ("MSFT", 300.0)]:
            data.append({
                "ts": ts,
                "symbol": symbol,
                "open": price,
                "high": price * 1.01,
                "low": price * 0.99,
                "close": price,
                "volume": 1000000
            })
    
    df = pd.DataFrame(data)
    logger.info(f"Test data: {len(df)} bars, {df['symbol'].nunique()} symbols")
    
    # 3. Run backtest
    logger.info("\nRunning backtest with risk constraints...")
    results = engine.run(df, rebalance_freq="M", progress=False)
    
    logger.info(f"Final NAV: ${results['final_nav']:,.2f}")
    logger.info(f"Metrics: {results['metrics']}")
    
    # 4. Save results with full config + bars
    run_dir = engine.save_run(
        results,
        run_id="risk_constraints_demo",
        backtest_config=getattr(engine, '_full_config', None),
        data=df  # Save OHLCV bars for UI
    )
    
    logger.info(f"\nResults saved to: {run_dir}")
    logger.info(f"Check {run_dir / 'config.yaml'} for persisted configuration")
    
    # 5. Show what was saved
    import yaml
    with open(run_dir / "config.yaml", 'r') as f:
        saved_config = yaml.safe_load(f)
    
    logger.info("\n" + "=" * 60)
    logger.info("Saved Configuration:")
    logger.info("=" * 60)
    logger.info(f"Risk Constraints: {saved_config.get('risk_constraints', {})}")
    logger.info(f"Constraint Mode: {saved_config.get('constraint_enforcement', {})}")
    
    return results


if __name__ == "__main__":
    main()
