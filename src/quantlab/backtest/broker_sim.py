"""Simulated broker for backtesting.

Handles order execution with slippage, fees, volume constraints, and partial fills.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

from quantlab.core.registry import fee_models
from quantlab.core.types import (
    Bar,
    Fill,
    Order,
    OrderStatus,
    OrderType,
    PortfolioState,
    Position,
    Side,
)
from quantlab.execution.constraints import (
    check_all_constraints,
    ConstraintCheckResult,
)


@dataclass
class FeeConfig:
    """Fee configuration."""
    
    commission_rate: Decimal = Decimal("0")
    commission_min: Decimal = Decimal("0")
    spread_bps: Decimal = Decimal("5")  # 5 bps spread
    slippage_bps: Decimal = Decimal("2")  # 2 bps slippage
    
    def calculate_commission(
        self,
        qty: Decimal,
        price: Decimal,
    ) -> Decimal:
        """Calculate commission only (spread/slippage handled in price)."""
        notional = qty * price
        
        # Commission only
        commission = max(
            notional * self.commission_rate,
            self.commission_min
        )
        
        return commission
    
    def apply_price_impact(
        self,
        base_price: Decimal,
        side: Side,
        is_market_order: bool = True
    ) -> tuple[Decimal, Decimal]:
        """Apply spread and slippage to price.
        
        Returns:
            (adjusted_price, slippage_per_share)
        """
        # Total impact = spread + slippage (both in bps)
        total_bps = self.spread_bps
        if is_market_order:
            total_bps += self.slippage_bps
        
        # Price impact (unfavorable direction)
        impact = base_price * total_bps / Decimal("10000")
        
        if side == Side.BUY:
            adjusted_price = base_price + impact
        else:
            adjusted_price = base_price - impact
        
        return adjusted_price, impact


@dataclass
class ExecutionConfig:
    """Execution constraints configuration.
    
    Fields:
        participation_rate: Max % of bar volume we can fill (default 20%)
        lot_size: Lot size for rounding (default 1, A-shares=100)
        min_trade_qty: Minimum trade quantity (optional, default=lot_size)
        impact_k_bps: Impact cost coefficient (bps)
        impact_alpha: Impact curve exponent (0.5 = sqrt)
        enforce_t1: Enforce T+1 settlement (A-shares=True, US=False)
    """
    
    participation_rate: Decimal = Decimal("0.2")  # 20% of bar volume
    lot_size: Decimal = Decimal("1")  # Lot size for rounding (A-shares=100)
    min_trade_qty: Decimal = Decimal("1")  # Minimum trade quantity
    impact_k_bps: Decimal = Decimal("20")  # Impact cost coefficient (bps)
    impact_alpha: Decimal = Decimal("0.5")  # Impact curve exponent (0.5 = sqrt)
    enforce_t1: bool = False  # T+1 settlement enforcement (A-shares)
    
    @classmethod
    def from_dict(cls, config: Optional[Dict] = None) -> "ExecutionConfig":
        """Create from config dict.
        
        Config format:
            execution:
                participation_rate: 0.2
                lot_size: 100           # A-shares=100, US stocks/ETF=1
                min_trade_qty: 100      # Optional, defaults to lot_size
                impact_k_bps: 20
                impact_alpha: 0.5
                enforce_t1: true        # A-shares=True, US=False
        """
        if config is None:
            return cls()
        
        # Support legacy 'min_lot' as alias for 'lot_size'
        lot_size = config.get("lot_size", config.get("min_lot", 1))
        
        return cls(
            participation_rate=Decimal(str(config.get("participation_rate", 0.2))),
            lot_size=Decimal(str(lot_size)),
            min_trade_qty=Decimal(str(config.get("min_trade_qty", lot_size))),
            impact_k_bps=Decimal(str(config.get("impact_k_bps", 20))),
            impact_alpha=Decimal(str(config.get("impact_alpha", 0.5))),
            enforce_t1=bool(config.get("enforce_t1", False))
        )


class SimulatedBroker:
    """Simulated broker for backtesting with partial fill support."""
    
    def __init__(
        self,
        initial_cash: Decimal,
        fee_config: Optional[FeeConfig] = None,
        exec_config: Optional[ExecutionConfig] = None,
        fill_at: str = "close"
    ) -> None:
        """Initialize simulated broker.
        
        Args:
            initial_cash: Starting cash
            fee_config: Fee configuration
            exec_config: Execution constraints (participation_rate, lot_size, min_trade_qty)
            fill_at: Fill price source (close, open, vwap)
        """
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.fee_config = fee_config or FeeConfig()
        self.exec_config = exec_config or ExecutionConfig()
        self.fill_at = fill_at
        
        self.positions: Dict[str, Position] = {}
        self.orders: List[Order] = []
        self.fills: List[Fill] = []
        self.last_prices: Dict[str, Decimal] = {}  # MTM: last known prices
        
        self.order_id_counter = 0
    
    def get_portfolio_state(self, ts: datetime) -> PortfolioState:
        """Get current portfolio state with MTM valuation."""
        # Calculate positions value using last prices (MTM)
        positions_value = Decimal("0")
        exposures: Dict[str, Decimal] = {}
        
        for symbol, pos in self.positions.items():
            # Use last price if available, otherwise avg_price
            price = self.last_prices.get(symbol, pos.avg_price)
            value = pos.qty * price
            positions_value += value
            exposures[symbol] = value
        
        return PortfolioState(
            ts=ts,
            cash=self.cash,
            positions=self.positions.copy(),
            nav=self.cash + positions_value,
            exposures=exposures
        )
    
    def update_prices(self, bars: Dict[str, Bar]) -> None:
        """Update last prices from market data (MTM)."""
        for symbol, bar in bars.items():
            self.last_prices[symbol] = bar.close
    
    def place_order(self, order: Order) -> str:
        """Submit an order.
        
        Args:
            order: Order to submit
            
        Returns:
            Order ID
        """
        self.order_id_counter += 1
        order_id = f"ORD_{self.order_id_counter:06d}"
        
        # Create new order with ID
        submitted_order = Order(
            id=order_id,
            ts=order.ts,
            symbol=order.symbol,
            side=order.side,
            qty=order.qty,
            order_type=order.order_type,
            limit_price=order.limit_price,
            status=OrderStatus.SUBMITTED,
            strategy_id=order.strategy_id
        )
        
        self.orders.append(submitted_order)
        logger.info(f"Order submitted: {order_id} {order.symbol} {order.side.name} qty={order.qty}")
        
        return order_id
    
    def process_orders(
        self,
        ts: datetime,
        bars: Dict[str, tuple[Bar, pd.Series]]
    ) -> List[Fill]:
        """Process pending orders against market data.
        
        Supports partial fills across multiple bars.
        
        Args:
            ts: Current timestamp
            bars: Dict of symbol -> (Bar, full_row_series)
                  The full_row_series contains curated regime fields
            
        Returns:
            List of fills from this bar
        """
        # Extract just Bar objects for price updates
        bar_only = {s: bar for s, (bar, _) in bars.items()}
        
        # Update prices for MTM
        self.update_prices(bar_only)
        
        fills = []
        
        for order in self.orders:
            # Only process orders that are still active
            if order.status not in (OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILLED):
                continue
            
            if order.symbol not in bars:
                continue
            
            bar, bar_row = bars[order.symbol]
            fill = self._try_fill(order, ts, bar, bar_row)
            
            if fill and fill.qty > 0:
                self._execute_fill(order, fill)
                fills.append(fill)
        
        return fills
    
    def _try_fill(
        self,
        order: Order,
        ts: datetime,
        bar: Bar,
        bar_row: Optional[pd.Series] = None
    ) -> Optional[Fill]:
        """Attempt to fill an order with unified constraint checking.
        
        Processing order:
        1. Trading regime constraints (suspension, limits, T+1) - uses curated data if available
        2. Volume constraint with liquidity cap
        3. Min lot normalization
        4. Cash constraint (for BUY)
        5. Impact cost calculation
        
        All rejections are now structured with reasons.
        
        Args:
            order: Order to fill
            ts: Current timestamp
            bar: Bar data (OHLCV)
            bar_row: Full row series with curated regime fields (is_suspended, can_buy, etc.)
        """
        # Determine base fill price
        if self.fill_at == "close":
            base_price = bar.close
        elif self.fill_at == "open":
            base_price = bar.open_
        elif self.fill_at == "vwap":
            base_price = (bar.high + bar.low + bar.close) / Decimal("3")
        else:
            base_price = bar.close
        
        # Store signal price for slippage analysis
        if order.signal_price == Decimal("0"):
            order.signal_price = base_price
        
        # Apply spread + slippage to get adjusted price
        fill_price, slippage_per_share = self.fee_config.apply_price_impact(
            base_price, order.side, order.order_type == OrderType.MARKET
        )
        
        # For limit orders, check if price is hit
        if order.order_type == OrderType.LIMIT:
            if order.limit_price is None:
                order.reject_reason = "LIMIT_PRICE_NOT_SET"
                return None
            
            if order.side == Side.BUY and bar.low > order.limit_price:
                order.reject_reason = "LIMIT_NOT_REACHED"
                return None
            if order.side == Side.SELL and bar.high < order.limit_price:
                order.reject_reason = "LIMIT_NOT_REACHED"
                return None
            
            fill_price = min(fill_price, order.limit_price) if order.side == Side.BUY else max(fill_price, order.limit_price)
        
        # ========== 1. Unified Constraint Checking ==========
        # Build bar_series with all necessary fields for constraint checking
        # Priority: use curated regime fields if available (data_contract=curated_v1)
        
        # Start with base OHLCV fields
        bar_series_data = {
            "open": float(bar.open_),
            "high": float(bar.high),
            "low": float(bar.low),
            "close": float(bar.close),
            "volume": float(bar.volume),
        }
        
        # Add curated regime fields if available
        use_curated = False
        if bar_row is not None:
            # Check for curated regime fields
            regime_fields = [
                "prev_close", "is_suspended", "is_limit_up", 
                "is_limit_down", "can_buy", "can_sell"
            ]
            has_regime = any(field in bar_row.index for field in regime_fields)
            
            if has_regime:
                use_curated = True
                # Add all available regime fields to bar_series
                for field in regime_fields:
                    if field in bar_row.index:
                        bar_series_data[field] = bar_row[field]
        
        bar_series = pd.Series(bar_series_data)
        
        portfolio_state = self.get_portfolio_state(ts)
        
        result = check_all_constraints(
            bar_row=bar_series,
            portfolio_state=portfolio_state,
            symbol=order.symbol,
            side=order.side,
            qty=order.remaining_qty,
            ts=ts,
            lot_size=int(self.exec_config.lot_size),
            min_trade_qty=int(self.exec_config.min_trade_qty),
            participation_rate=float(self.exec_config.participation_rate),
            enforce_t1=self.exec_config.enforce_t1,  # T+1 settlement (A-shares)
            use_curated=use_curated  # Use curated regime fields if available
        )
        
        if not result.ok:
            order.reject_reason = result.reason
            logger.debug(f"Order {order.id} rejected: {result.reason}")
            return None
        
        fill_qty = result.adjusted_qty
        
        # ========== 2. Cash Constraint (for BUY orders) ==========
        if order.side == Side.BUY:
            tentative_cost = fill_qty * fill_price
            tentative_commission = self.fee_config.calculate_commission(fill_qty, fill_price)
            total_cost = tentative_cost + tentative_commission
            
            if total_cost > self.cash:
                if fill_price > 0:
                    commission_rate = self.fee_config.commission_rate
                    effective_price = fill_price * (Decimal("1") + commission_rate)
                    max_affordable = self.cash / effective_price
                    lot_size = self.exec_config.lot_size
                    
                    if lot_size > 1:
                        # Round down to nearest lot for A-shares
                        max_affordable = (max_affordable // lot_size) * lot_size
                    # For lot_size=1 (US stocks), no rounding needed
                    
                    if max_affordable <= 0:
                        order.reject_reason = "INSUFFICIENT_CASH"
                        logger.warning(
                            f"Order {order.id}: Insufficient cash for {order.symbol}. "
                            f"Cash: {self.cash:.2f}, Required: {total_cost:.2f}"
                        )
                        return None
                    
                    fill_qty = min(fill_qty, max_affordable)
                    logger.info(
                        f"Order {order.id}: Cash-constrained fill. "
                        f"Requested: {order.remaining_qty}, Affordable: {fill_qty}"
                    )
                else:
                    order.reject_reason = "ZERO_FILL_PRICE"
                    return None
        
        # Final validation
        if fill_qty <= 0:
            if not order.reject_reason:
                order.reject_reason = "ZERO_FILL_QTY"
            return None
        
        # ========== 3. Impact Cost Calculation ==========
        participation = fill_qty / bar.volume
        k_bps = self.exec_config.impact_k_bps
        alpha = self.exec_config.impact_alpha
        
        participation_float = float(participation)
        alpha_float = float(alpha)
        impact_bps = float(k_bps) * (participation_float ** alpha_float)
        impact_bps_decimal = Decimal(str(impact_bps))
        
        if order.side == Side.BUY:
            fill_price = fill_price * (Decimal("1") + impact_bps_decimal / Decimal("10000"))
        else:
            fill_price = fill_price * (Decimal("1") - impact_bps_decimal / Decimal("10000"))
        
        impact_cost = fill_qty * base_price * impact_bps_decimal / Decimal("10000")
        fee = self.fee_config.calculate_commission(fill_qty, fill_price)
        
        # Calculate filled ratio
        filled_ratio = fill_qty / order.qty
        
        logger.debug(
            f"Order {order.id}: participation={participation:.4f}, "
            f"impact_bps={impact_bps:.2f}, impact_cost=${impact_cost:.2f}"
        )
        
        return Fill(
            order_id=order.id,
            ts=ts,
            qty=fill_qty,
            price=fill_price,
            fee=fee,
            slippage=slippage_per_share,
            impact_cost=impact_cost,
            impact_bps=impact_bps_decimal,
            filled_ratio=filled_ratio,
            signal_price=order.signal_price
        )
    
    def _execute_fill(self, order: Order, fill: Fill) -> None:
        """Execute a fill and update state."""
        # Update order filled quantity
        order.filled_qty += fill.qty
        
        # Update order status
        if order.filled_qty >= order.qty:
            order.status = OrderStatus.FILLED
            logger.info(f"Order {order.id} FILLED ({order.qty}/{order.qty})")
        else:
            order.status = OrderStatus.PARTIAL_FILLED
            logger.info(
                f"Order {order.id} PARTIAL_FILLED "
                f"(this_bar: {fill.qty}, total: {order.filled_qty}/{order.qty})"
            )
        
        # Record fill
        self.fills.append(fill)
        
        # Update position
        symbol = order.symbol
        if symbol not in self.positions:
            self.positions[symbol] = Position(symbol=symbol)
        
        position = self.positions[symbol]
        
        if order.side == Side.BUY:
            # Update average price (weighted average)
            total_cost = (position.qty * position.avg_price) + (fill.qty * fill.price)
            position.qty += fill.qty
            if position.qty > 0:
                position.avg_price = total_cost / position.qty
            
            # T+1: Record last buy timestamp for settlement tracking
            position.last_buy_ts = fill.ts
            
            # Deduct cash (price + commission)
            self.cash -= fill.qty * fill.price + fill.fee
        
        else:  # SELL
            position.qty -= fill.qty
            # Add cash (price - commission)
            self.cash += fill.qty * fill.price - fill.fee
            
            # Clear avg_price if flat
            if position.qty == 0:
                position.avg_price = Decimal("0")
        
        logger.debug(
            f"Fill recorded: {order.symbol} {order.side.name} {fill.qty} @ {fill.price:.4f}"
        )
    
    def get_active_orders(self) -> List[Order]:
        """Get orders that are still pending or partially filled.
        
        Returns:
            List of active orders
        """
        return [
            o for o in self.orders
            if o.status in (OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILLED)
        ]
    
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        for order in self.orders:
            if order.id == order_id:
                if order.status in (OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILLED):
                    order.status = OrderStatus.CANCELLED
                    logger.info(f"Order {order_id} CANCELLED")
                    return True
        return False
    
    def get_trades_df(self) -> pd.DataFrame:
        """Get fills as DataFrame with structured rejection info."""
        if not self.fills:
            return pd.DataFrame()
        
        records = []
        for fill in self.fills:
            order = next(o for o in self.orders if o.id == fill.order_id)
            records.append({
                "ts": fill.ts,
                "order_id": fill.order_id,
                "symbol": order.symbol,
                "side": order.side.name,
                "order_qty": float(order.qty),           # NEW: Original order qty
                "filled_qty": float(fill.qty),           # NEW: Filled qty
                "filled_ratio": float(fill.filled_ratio), # NEW: Fill ratio
                "price": float(fill.price),
                "signal_price": float(fill.signal_price), # NEW: Signal price
                "fee": float(fill.fee),
                "slippage": float(fill.slippage),
                "impact_cost": float(fill.impact_cost),
                "impact_bps": float(fill.impact_bps),
                "reject_reason": order.reject_reason,     # NEW: Rejection reason
            })
        
        return pd.DataFrame(records)
    
    def get_rejected_orders_df(self) -> pd.DataFrame:
        """Get rejected orders as DataFrame."""
        rejected = [o for o in self.orders if o.reject_reason]
        if not rejected:
            return pd.DataFrame()
        
        records = []
        for order in rejected:
            records.append({
                "ts": order.ts,
                "order_id": order.id,
                "symbol": order.symbol,
                "side": order.side.name,
                "qty": float(order.qty),
                "signal_price": float(order.signal_price),
                "reject_reason": order.reject_reason,
            })
        
        return pd.DataFrame(records)


def load_fee_model(name: str) -> FeeConfig:
    """Load fee configuration by name.
    
    Supported models:
        - zero: No fees
        - us_etfs: US ETF trading (commission-free, low spread)
        - china_ashares: China A-shares (commission + stamp tax on sell)
    """
    if name == "zero":
        return FeeConfig(
            commission_rate=Decimal("0"),
            spread_bps=Decimal("0"),
            slippage_bps=Decimal("0")
        )
    elif name == "us_etfs":
        return FeeConfig(
            commission_rate=Decimal("0"),
            spread_bps=Decimal("5"),
            slippage_bps=Decimal("2")
        )
    elif name == "china_ashares":
        # A-share: 0.025% commission (min 5元), 0.05% stamp tax on sell
        return FeeConfig(
            commission_rate=Decimal("0.00025"),  # 0.025%
            spread_bps=Decimal("10"),            # Wider spread than US
            slippage_bps=Decimal("5")
        )
    else:
        return FeeConfig()
