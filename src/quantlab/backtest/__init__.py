"""Backtest module for quantlab.

Event-driven backtesting with realistic execution simulation.
"""

from pathlib import Path
from typing import Dict, Optional

import yaml

from quantlab.backtest.engine import BacktestEngine
from quantlab.backtest.broker_sim import SimulatedBroker, FeeConfig, load_fee_model
from quantlab.backtest.metrics import MetricsCalculator
from quantlab.research.risk_constraints import RiskConstraintConfig
from quantlab.research.strategies.base import Strategy

__all__ = [
    "BacktestEngine",
    "SimulatedBroker",
    "FeeConfig",
    "load_fee_model",
    "MetricsCalculator",
    "create_engine_from_config",
]


def create_engine_from_config(
    config_path: str,
    strategy: Strategy,
    config_name: str = "default"
) -> BacktestEngine:
    """Create BacktestEngine from YAML configuration file.
    
    This is the recommended way to initialize the engine with full
    risk constraint and execution configuration.
    
    Args:
        config_path: Path to backtest.yaml file
        strategy: Trading strategy instance
        config_name: Config section to use (default, monthly_rebalance, etc.)
        
    Returns:
        Configured BacktestEngine
        
    Example:
        >>> from quantlab.backtest import create_engine_from_config
        >>> from quantlab.research.strategies import MyStrategy
        >>> 
        >>> strategy = MyStrategy()
        >>> engine = create_engine_from_config(
        ...     "config/backtest.yaml",
        ...     strategy,
        ...     config_name="default"
        ... )
        >>> results = engine.run(data)
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        full_config = yaml.safe_load(f)
    
    # Get base config
    if config_name == "default":
        config = full_config.get("default", {})
    else:
        base = full_config.get("default", {})
        override = full_config.get("configs", {}).get(config_name, {})
        # Simple merge (override takes precedence)
        config = {**base, **override}
    
    # Extract parameters
    capital = config.get("capital", {})
    initial_cash = capital.get("initial", 1_000_000.0)
    
    execution = config.get("execution", {})
    
    # Build risk config from YAML
    risk_config = None
    risk_yaml = config.get("risk", {})
    if risk_yaml:
        risk_config = RiskConstraintConfig.from_dict(risk_yaml)
    
    # Get constraint mode
    constraint_mode = config.get("constraint_mode", "clip")
    
    # Create engine
    engine = BacktestEngine(
        strategy=strategy,
        initial_cash=initial_cash,
        fee_model=execution.get("slippage_model", "us_etfs"),
        exec_config=execution,
        calendar="XNYS",  # TODO: Make configurable
        risk_config=risk_config,
        constraint_mode=constraint_mode
    )
    
    # Store full config for save_run
    engine._full_config = config
    
    return engine
