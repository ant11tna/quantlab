"""Core module for quantlab.

Contains fundamental data structures and utilities.
"""

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
    PricePoint,
    RunConfig,
    Side,
    Signal,
    TargetWeight,
    Urgency,
)
from quantlab.core.time import TradingCalendar, TimezoneHandler, align_timestamps
from quantlab.core.registry import (
    Registry,
    broker_adapters,
    data_sources,
    fee_models,
    register_broker_adapter,
    register_data_source,
    register_fee_model,
    register_strategy,
    strategies,
)
from quantlab.core.runlog import (
    create_run_dir,
    write_run_metadata,
    finalize_run,
    list_runs,
    load_run_metrics,
    compare_runs,
)

__all__ = [
    # Types
    "Bar",
    "DataSnapshot", 
    "Fill",
    "Order",
    "OrderIntent",
    "OrderStatus",
    "OrderType",
    "PortfolioState",
    "Position",
    "PricePoint",
    "RunConfig",
    "Side",
    "Signal",
    "TargetWeight",
    "Urgency",
    # Time
    "TradingCalendar",
    "TimezoneHandler",
    "align_timestamps",
    # Registry
    "Registry",
    "strategies",
    "data_sources",
    "broker_adapters",
    "fee_models",
    "register_strategy",
    "register_data_source",
    "register_broker_adapter",
    "register_fee_model",
]
