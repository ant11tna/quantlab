"""Trading regime constraints for order validation.

Unified constraint checking for execution layer.
Called by router and broker_sim before order submission/fill.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Optional, Tuple
from datetime import datetime

import pandas as pd
from loguru import logger

from quantlab.core.types import PortfolioState, Side


@dataclass
class ConstraintCheckResult:
    """Result of constraint check."""
    ok: bool                          # Whether order can proceed
    reason: str                       # Human-readable reason if rejected
    fillable_qty: Optional[Decimal]   # Max fillable quantity (for partial fills)
    adjusted_qty: Optional[Decimal]   # Lot-size adjusted quantity


def check_bar_tradable(
    bar_row: pd.Series,
    side: Side,
    use_curated: bool = True
) -> Tuple[bool, str]:
    """Check if a bar allows trading on given side.
    
    Args:
        bar_row: Single bar data (must have is_suspended/can_buy/can_sell or OHLCV)
        side: BUY or SELL
        use_curated: If True, use curated regime fields. If False, infer from OHLCV.
        
    Returns:
        Tuple of (is_tradable: bool, reason: str)
    """
    # Try curated fields first
    if use_curated and "is_suspended" in bar_row.index:
        if bar_row["is_suspended"]:
            return False, "SUSPENDED"
        
        if side == Side.BUY:
            if "can_buy" in bar_row.index and not bar_row["can_buy"]:
                if "is_limit_up" in bar_row.index and bar_row["is_limit_up"]:
                    return False, "LIMIT_UP"
                return False, "CANNOT_BUY"
        else:  # SELL
            if "can_sell" in bar_row.index and not bar_row["can_sell"]:
                if "is_limit_down" in bar_row.index and bar_row["is_limit_down"]:
                    return False, "LIMIT_DOWN"
                return False, "CANNOT_SELL"
        
        return True, "OK"
    
    # Fallback: infer from OHLCV
    # Suspended if volume is 0 or NaN
    volume = bar_row.get("volume", 0)
    if pd.isna(volume) or volume == 0:
        return False, "SUSPENDED_NO_VOLUME"
    
    # Price check for limits (simplified)
    close = bar_row.get("close", 0)
    high = bar_row.get("high", 0)
    low = bar_row.get("low", 0)
    prev_close = bar_row.get("prev_close")
    
    if prev_close and not pd.isna(prev_close):
        change_pct = (close - prev_close) / prev_close
        
        if side == Side.BUY:
            # Can't buy at limit up
            if abs(close - high) < 0.0001 and change_pct > 0.09:  # ~10% up
                return False, "LIMIT_UP_ESTIMATED"
        else:
            # Can't sell at limit down
            if abs(close - low) < 0.0001 and change_pct < -0.09:  # ~10% down
                return False, "LIMIT_DOWN_ESTIMATED"
    
    return True, "OK"


def cap_fill_by_liquidity(
    bar_row: pd.Series,
    qty: Decimal,
    participation_rate: float = 0.2,
    min_volume: int = 1
) -> Decimal:
    """Cap fill quantity by available liquidity.
    
    Args:
        bar_row: Bar data with volume
        qty: Requested quantity
        participation_rate: Max % of bar volume we can fill (default 20%)
        min_volume: Minimum volume to consider bar tradable
        
    Returns:
        Fillable quantity (capped at participation_rate * volume)
    """
    volume = bar_row.get("volume", 0)
    
    if pd.isna(volume) or volume < min_volume:
        return Decimal("0")
    
    max_fill = Decimal(str(volume)) * Decimal(str(participation_rate))
    
    return min(qty, max_fill)


def normalize_lot_size(
    qty: Decimal,
    lot_size: int = 100,
    allow_partial: bool = False
) -> Tuple[Decimal, Decimal]:
    """Normalize quantity to lot size.
    
    Args:
        qty: Raw quantity
        lot_size: Lot size (default 100 for A-shares)
        allow_partial: If True, allow partial lots (for crypto/fractional)
        
    Returns:
        Tuple of (adjusted_qty, remainder)
    """
    if allow_partial or lot_size == 1:
        return qty, Decimal("0")
    
    # Round down to nearest lot
    lots = int(qty) // lot_size
    adjusted = Decimal(str(lots * lot_size))
    remainder = qty - adjusted
    
    return adjusted, remainder


def check_t1_constraint(
    portfolio_state: PortfolioState,
    symbol: str,
    side: Side,
    ts: datetime,
    check_same_day: bool = True
) -> Tuple[bool, str]:
    """Check T+1 settlement constraint for selling.
    
    Simplified version: checks if we have sellable position (not bought today).
    
    Args:
        portfolio_state: Current portfolio state
        symbol: Symbol to check
        side: Order side
        ts: Current timestamp
        check_same_day: If True, enforce T+1 (can't sell same-day buys)
        
    Returns:
        Tuple of (ok, reason)
    """
    if side == Side.BUY:
        return True, "OK"  # No T+1 constraint on buys
    
    position = portfolio_state.positions.get(symbol)
    if not position:
        return False, "NO_POSITION"
    
    if position.qty <= 0:
        return False, "NO_LONG_POSITION"
    
    # Check if we need T+1 enforcement
    if check_same_day and hasattr(position, 'last_buy_ts'):
        # Position has last buy timestamp - check if today
        if position.last_buy_ts and position.last_buy_ts.date() == ts.date():
            return False, "T1_LOCK_SAME_DAY_BUY"
    
    return True, "OK"


def check_all_constraints(
    bar_row: pd.Series,
    portfolio_state: PortfolioState,
    symbol: str,
    side: Side,
    qty: Decimal,
    ts: datetime,
    lot_size: int = 100,
    min_trade_qty: Optional[int] = None,
    participation_rate: float = 0.2,
    enforce_t1: bool = False,
    use_curated: bool = True
) -> ConstraintCheckResult:
    """Check all trading constraints in sequence.
    
    Pipeline:
        1. Check bar tradable (suspension/limits)
        2. Check T+1 constraint (if selling)
        3. Cap by liquidity
        4. Normalize lot size
        5. Check minimum trade quantity (optional)
        
    Args:
        bar_row: Current bar data
        portfolio_state: Portfolio state
        symbol: Trading symbol
        side: BUY or SELL
        qty: Requested quantity
        ts: Current timestamp
        lot_size: Lot size for normalization (A-shares=100, US=1)
        min_trade_qty: Minimum trade quantity (default=lot_size)
        participation_rate: Max participation of bar volume
        enforce_t1: Whether to enforce T+1
        use_curated: Use curated regime fields if available
        
    Returns:
        ConstraintCheckResult with ok/reason/fillable_qty/adjusted_qty
    """
    # Default min_trade_qty to lot_size if not specified
    if min_trade_qty is None:
        min_trade_qty = lot_size
    # 1. Check bar tradable
    tradable, reason = check_bar_tradable(bar_row, side, use_curated)
    if not tradable:
        return ConstraintCheckResult(
            ok=False,
            reason=reason,
            fillable_qty=Decimal("0"),
            adjusted_qty=Decimal("0")
        )
    
    # 2. Check T+1 (if selling and enabled)
    if enforce_t1 and side == Side.SELL:
        t1_ok, t1_reason = check_t1_constraint(portfolio_state, symbol, side, ts)
        if not t1_ok:
            return ConstraintCheckResult(
                ok=False,
                reason=t1_reason,
                fillable_qty=Decimal("0"),
                adjusted_qty=Decimal("0")
            )
    
    # 3. Cap by liquidity
    fillable_qty = cap_fill_by_liquidity(bar_row, qty, participation_rate)
    if fillable_qty <= 0:
        return ConstraintCheckResult(
            ok=False,
            reason="NO_LIQUIDITY",
            fillable_qty=Decimal("0"),
            adjusted_qty=Decimal("0")
        )
    
    # 4. Normalize lot size
    adjusted_qty, remainder = normalize_lot_size(fillable_qty, lot_size)
    if adjusted_qty <= 0:
        return ConstraintCheckResult(
            ok=False,
            reason=f"LOT_SIZE_TOO_SMALL_{lot_size}",
            fillable_qty=fillable_qty,
            adjusted_qty=Decimal("0")
        )
    
    # 5. Check minimum trade quantity
    if adjusted_qty < min_trade_qty:
        return ConstraintCheckResult(
            ok=False,
            reason=f"BELOW_MIN_TRADE_QTY_{min_trade_qty}",
            fillable_qty=fillable_qty,
            adjusted_qty=Decimal("0")
        )
    
    return ConstraintCheckResult(
        ok=True,
        reason="OK",
        fillable_qty=fillable_qty,
        adjusted_qty=adjusted_qty
    )


# Rejection reason categories for reporting
REJECTION_CATEGORIES = {
    # Market regime
    "SUSPENDED": "market_regime",
    "SUSPENDED_NO_VOLUME": "market_regime",
    "LIMIT_UP": "market_regime",
    "LIMIT_UP_ESTIMATED": "market_regime",
    "LIMIT_DOWN": "market_regime",
    "LIMIT_DOWN_ESTIMATED": "market_regime",
    "CANNOT_BUY": "market_regime",
    "CANNOT_SELL": "market_regime",
    
    # Position/Settlement
    "NO_POSITION": "position",
    "NO_LONG_POSITION": "position",
    "T1_LOCK_SAME_DAY_BUY": "settlement",
    
    # Liquidity/Size
    "NO_LIQUIDITY": "liquidity",
    "LOT_SIZE_TOO_SMALL": "order_size",
    "BELOW_MIN_TRADE_QTY": "order_size",
}


def categorize_rejection(reason: str) -> str:
    """Categorize rejection reason for reporting."""
    for prefix, category in REJECTION_CATEGORIES.items():
        if reason.startswith(prefix):
            return category
    return "other"
