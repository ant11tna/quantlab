"""QuantLab - Quantitative research and backtesting system.

A modular system for quantitative trading research with clear separation
between research (signal generation) and execution (order management).

Key modules:
- core: Fundamental data structures and utilities
- data: Data sources, ingestion, and transformation
- research: Strategies, portfolio construction, risk analysis
- backtest: Event-driven backtesting engine
- execution: Broker adapters for live trading (future)
- ui: Streamlit visualization interface
"""

__version__ = "0.1.0"
__author__ = "Quant Researcher"

from quantlab.core.types import (
    Bar,
    DataSnapshot,
    Fill,
    Order,
    OrderIntent,
    OrderStatus,
    OrderType,
    PortfolioState,
    Position,
    RunConfig,
    Side,
    Signal,
    TargetWeight,
)

__all__ = [
    "Bar",
    "DataSnapshot",
    "Fill",
    "Order",
    "OrderIntent",
    "OrderStatus",
    "OrderType",
    "PortfolioState",
    "Position",
    "RunConfig",
    "Side",
    "Signal",
    "TargetWeight",
]
