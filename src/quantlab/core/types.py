"""Core data structures for the quantitative research system.

This module defines the fundamental types used throughout the system:
- Market data (Bar, PricePoint)
- Research outputs (Signal, TargetWeight, OrderIntent)
- Execution records (Order, Fill, Position)
- Portfolio state (PortfolioState)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Dict, List, Literal, Optional
from decimal import Decimal


class Side(Enum):
    """Trade side."""
    BUY = auto()
    SELL = auto()


class OrderType(Enum):
    """Order type."""
    MARKET = auto()
    LIMIT = auto()


class OrderStatus(str, Enum):
    """Order lifecycle status.
    
    Using str Enum for JSON serialization compatibility.
    """
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    PARTIAL_FILLED = "PARTIAL_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class Urgency(Enum):
    """Execution urgency level."""
    LOW = auto()      # Can work over multiple days
    MEDIUM = auto()   # Execute within day
    HIGH = auto()     # Immediate execution


@dataclass(frozen=True, slots=True)
class Bar:
    """OHLCV bar data.
    
    Args:
        ts: Timestamp
        symbol: Trading symbol/ticker
        open_: Opening price
        high: High price
        low: Low price
        close: Closing price
        volume: Trading volume
        adj_factor: Optional adjustment factor for splits/dividends
    """
    ts: datetime
    symbol: str
    open_: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    adj_factor: Optional[Decimal] = None
    
    def __post_init__(self) -> None:
        # Validate price ordering
        if self.high < self.low:
            raise ValueError(f"high ({self.high}) must be >= low ({self.low})")
        if self.high < self.open_ or self.high < self.close:
            raise ValueError("high must be >= open and close")
        if self.low > self.open_ or self.low > self.close:
            raise ValueError("low must be <= open and close")


@dataclass(frozen=True, slots=True)
class PricePoint:
    """Single price point.
    
    Args:
        ts: Timestamp
        price: Price value
    """
    ts: datetime
    price: Decimal


# =============================================================================
# Research Layer Types
# =============================================================================

@dataclass(frozen=True, slots=True)
class Signal:
    """Research signal output.
    
    This is the raw output from research/strategy layer.
    
    Args:
        ts: Signal timestamp
        symbol: Trading symbol
        side: Buy or sell signal
        strength: Signal strength (0.0 to 1.0)
        reason: Human-readable signal rationale
        meta: Additional metadata
    """
    ts: datetime
    symbol: str
    side: Side
    strength: float  # 0.0 to 1.0
    reason: str
    meta: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self) -> None:
        if not 0.0 <= self.strength <= 1.0:
            raise ValueError(f"strength must be in [0, 1], got {self.strength}")


@dataclass(frozen=True, slots=True)
class TargetWeight:
    """Target portfolio weight.
    
    This is the bridge between research and execution.
    Research layer outputs target weights, execution converts to orders.
    
    Args:
        ts: Timestamp when target is set
        symbol: Trading symbol
        target_weight: Target weight in portfolio (-1.0 to 1.0)
                       Negative for short positions
        source: Source of target (strategy name, etc.)
    """
    ts: datetime
    symbol: str
    target_weight: Decimal
    source: str = "default"
    
    def __post_init__(self) -> None:
        if not -1.0 <= float(self.target_weight) <= 1.0:
            raise ValueError(
                f"target_weight must be in [-1, 1], got {self.target_weight}"
            )


@dataclass(frozen=True, slots=True)
class OrderIntent:
    """Trading intent from research layer.
    
    This is a more concrete form than Signal, specifying exactly
    what the research wants to achieve.
    
    Args:
        ts: Intent timestamp
        symbol: Trading symbol
        target_qty: Target quantity (None if using target_weight)
        target_weight: Target weight (None if using target_qty)
        urgency: Execution urgency
        reason: Intent rationale
        constraints: Execution constraints (e.g., max slippage)
    """
    ts: datetime
    symbol: str
    target_qty: Optional[Decimal] = None
    target_weight: Optional[Decimal] = None
    urgency: Urgency = Urgency.MEDIUM
    reason: str = ""
    constraints: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self) -> None:
        if self.target_qty is None and self.target_weight is None:
            raise ValueError("Either target_qty or target_weight must be specified")


# =============================================================================
# Execution Layer Types
# =============================================================================

@dataclass(slots=True)
class Order:
    """Trading order.
    
    Note: Not frozen to allow status updates and partial fills.
    
    Args:
        id: Unique order ID
        ts: Order creation timestamp
        symbol: Trading symbol
        side: Buy or sell
        qty: Order quantity (target)
        order_type: Market or limit
        limit_price: Limit price (for limit orders)
        status: Current order status
        strategy_id: Strategy/experiment that generated this order
        filled_qty: Already filled quantity (for partial fills)
        reject_reason: Reason if order was rejected (empty if not rejected)
        signal_price: Signal/reference price when order was created
    """
    id: str
    ts: datetime
    symbol: str
    side: Side
    qty: Decimal
    order_type: OrderType = OrderType.MARKET
    limit_price: Optional[Decimal] = None
    status: OrderStatus = OrderStatus.PENDING
    strategy_id: str = ""
    filled_qty: Decimal = field(default_factory=lambda: Decimal("0"))
    reject_reason: str = ""  # NEW: Structured rejection reason
    signal_price: Decimal = Decimal("0")  # NEW: Signal price for slippage calc
    
    def __post_init__(self) -> None:
        if self.qty <= 0:
            raise ValueError(f"qty must be positive, got {self.qty}")
        if self.filled_qty < 0:
            raise ValueError(f"filled_qty must be non-negative, got {self.filled_qty}")
        if self.filled_qty > self.qty:
            raise ValueError(f"filled_qty ({self.filled_qty}) cannot exceed qty ({self.qty})")
        if self.order_type == OrderType.LIMIT and self.limit_price is None:
            raise ValueError("limit_price required for limit orders")
    
    @property
    def remaining_qty(self) -> Decimal:
        """Get remaining quantity to fill."""
        return self.qty - self.filled_qty
    
    @property
    def is_filled(self) -> bool:
        """Check if order is completely filled."""
        return self.filled_qty >= self.qty
    
    @property
    def is_partially_filled(self) -> bool:
        """Check if order has partial fills."""
        return self.filled_qty > 0 and self.filled_qty < self.qty


@dataclass(frozen=True, slots=True)
class Fill:
    """Order fill record.
    
    Args:
        order_id: Reference to parent order
        ts: Fill timestamp
        qty: Filled quantity
        price: Fill price
        fee: Trading fee
        slippage: Slippage cost (difference from expected price)
        impact_cost: Market impact cost (capacity sensitivity)
        impact_bps: Impact in basis points
        venue: Exchange/venue where filled
        filled_ratio: Ratio of order qty filled (0-1)
        signal_price: Signal/reference price when order was created
    """
    order_id: str
    ts: datetime
    qty: Decimal
    price: Decimal
    fee: Decimal = Decimal("0")
    slippage: Decimal = Decimal("0")
    impact_cost: Decimal = Decimal("0")
    impact_bps: Decimal = Decimal("0")
    venue: str = ""
    filled_ratio: Decimal = Decimal("0")  # NEW: Fill ratio for reporting
    signal_price: Decimal = Decimal("0")  # NEW: Signal price for slippage analysis
    
    @property
    def value(self) -> Decimal:
        """Fill value (qty * price)."""
        return self.qty * self.price
    
    @property
    def cost(self) -> Decimal:
        """Total cost including fees and slippage."""
        return self.fee + self.slippage


@dataclass(slots=True)
class Position:
    """Portfolio position.
    
    Args:
        symbol: Trading symbol
        qty: Current quantity (negative for short)
        avg_price: Average entry price
        unrealized_pnl: Unrealized P&L
        last_buy_ts: Timestamp of last buy (for T+1 settlement tracking)
    """
    symbol: str
    qty: Decimal = Decimal("0")
    avg_price: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    last_buy_ts: Optional[datetime] = None  # T+1: track last buy timestamp
    
    @property
    def market_value(self, price: Decimal) -> Decimal:
        """Calculate market value at given price."""
        return self.qty * price
    
    @property
    def is_long(self) -> bool:
        return self.qty > 0
    
    @property
    def is_short(self) -> bool:
        return self.qty < 0
    
    @property
    def is_flat(self) -> bool:
        return self.qty == 0


@dataclass(slots=True)
class PortfolioState:
    """Complete portfolio state snapshot.
    
    Args:
        ts: Snapshot timestamp
        cash: Available cash
        positions: Dict of symbol -> Position
        nav: Net asset value (cash + positions value)
        exposures: Dict of symbol -> exposure (qty * price)
    """
    ts: datetime
    cash: Decimal = Decimal("0")
    positions: Dict[str, Position] = field(default_factory=dict)
    nav: Decimal = Decimal("0")
    exposures: Dict[str, Decimal] = field(default_factory=dict)
    
    def get_position(self, symbol: str) -> Position:
        """Get position for symbol, creating empty if not exists."""
        if symbol not in self.positions:
            self.positions[symbol] = Position(symbol=symbol)
        return self.positions[symbol]
    
    def total_exposure(self) -> Decimal:
        """Sum of absolute exposures."""
        return sum((abs(e) for e in self.exposures.values()), Decimal("0"))
    
    def gross_leverage(self) -> Decimal:
        """Gross leverage (total exposure / NAV)."""
        if self.nav == 0:
            return Decimal("0")
        return self.total_exposure() / self.nav
    
    def net_exposure(self) -> Decimal:
        """Net exposure (long - short)."""
        return sum(self.exposures.values(), Decimal("0"))


# =============================================================================
# Utility Types
# =============================================================================

@dataclass(frozen=True, slots=True)
class RunConfig:
    """Experiment run configuration.
    
    Args:
        run_id: Unique run identifier
        universe: Asset universe name
        start_date: Backtest start date
        end_date: Backtest end date
        rebalance_freq: Rebalancing frequency (e.g., 'M', 'Q')
        threshold: Rebalance threshold (e.g., 0.05 for 5%)
        fee_model: Fee model name
        created_at: Config creation timestamp
    """
    run_id: str
    universe: str
    start_date: datetime
    end_date: datetime
    rebalance_freq: Optional[str] = None
    threshold: Optional[Decimal] = None
    fee_model: str = "default"
    created_at: datetime = field(default_factory=datetime.now)
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DataSnapshot:
    """Data version snapshot for reproducibility.
    
    Args:
        snapshot_id: Unique snapshot ID
        data_range: (start, end) tuple
        symbols: List of symbols included
        hash: Content hash for verification
        manifest_path: Path to manifest file
    """
    snapshot_id: str
    data_range: tuple[datetime, datetime]
    symbols: List[str]
    hash: str
    manifest_path: str
    created_at: datetime = field(default_factory=datetime.now)
