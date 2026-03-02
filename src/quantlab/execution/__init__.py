"""Execution module for quantlab.

Broker adapters and order management for live trading.
For backtesting, use backtest.broker_sim instead.
"""

from quantlab.execution.broker_base import (
    BrokerAdapter,
    OrderRouter,
    RiskGate,
)
from quantlab.execution.broker_mock import MockBrokerAdapter
from quantlab.execution.router import (
    ExecutionRouter,
    RiskAwareRouter,
    create_router_for_backtest,
    create_router_for_live,
)

__all__ = [
    "BrokerAdapter",
    "OrderRouter",
    "RiskGate",
    "MockBrokerAdapter",
    "ExecutionRouter",
    "RiskAwareRouter",
    "create_router_for_backtest",
    "create_router_for_live",
]
