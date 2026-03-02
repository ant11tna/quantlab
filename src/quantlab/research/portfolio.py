"""Portfolio construction and weight management.

Handles weight synthesis and constraints.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import numpy as np
from loguru import logger

from quantlab.core.types import TargetWeight, PortfolioState, Position


@dataclass
class WeightConstraints:
    """Portfolio weight constraints."""
    
    min_weight: float = 0.0
    max_weight: float = 1.0
    min_total: float = 0.95
    max_total: float = 1.05
    max_turnover: Optional[float] = None


class WeightOptimizer:
    """Optimize portfolio weights subject to constraints."""
    
    def __init__(self, constraints: Optional[WeightConstraints] = None) -> None:
        """Initialize optimizer.
        
        Args:
            constraints: Weight constraints
        """
        self.constraints = constraints or WeightConstraints()
    
    def normalize(
        self,
        weights: Dict[str, float],
        method: str = "sum_to_one"
    ) -> Dict[str, float]:
        """Normalize weights to sum to target.
        
        Args:
            weights: Raw weights
            method: Normalization method (sum_to_one, softmax)
            
        Returns:
            Normalized weights
        """
        if not weights:
            return {}
        
        if method == "sum_to_one":
            total = sum(abs(w) for w in weights.values())
            if total == 0:
                return {k: 0.0 for k in weights}
            return {k: v / total for k, v in weights.items()}
        
        elif method == "softmax":
            exp_weights = {k: np.exp(v) for k, v in weights.items()}
            total = sum(exp_weights.values())
            return {k: v / total for k, v in exp_weights.items()}
        
        else:
            raise ValueError(f"Unknown normalization method: {method}")
    
    def apply_bounds(
        self,
        weights: Dict[str, float],
        min_weight: Optional[float] = None,
        max_weight: Optional[float] = None
    ) -> Dict[str, float]:
        """Apply min/max weight bounds.
        
        Args:
            weights: Input weights
            min_weight: Minimum weight (default from constraints)
            max_weight: Maximum weight (default from constraints)
            
        Returns:
            Bounded weights
        """
        min_w = min_weight if min_weight is not None else self.constraints.min_weight
        max_w = max_weight if max_weight is not None else self.constraints.max_weight
        
        bounded = {}
        for symbol, weight in weights.items():
            bounded[symbol] = max(min_w, min(max_w, weight))
        
        return bounded
    
    def optimize(
        self,
        raw_weights: Dict[str, float],
        current_weights: Optional[Dict[str, float]] = None
    ) -> Dict[str, float]:
        """Full optimization pipeline.
        
        Args:
            raw_weights: Target weights from strategy
            current_weights: Current portfolio weights (for turnover)
            
        Returns:
            Optimized weights
        """
        # Apply bounds
        weights = self.apply_bounds(raw_weights)
        
        # Normalize
        weights = self.normalize(weights)
        
        # Check turnover constraint
        if current_weights and self.constraints.max_turnover:
            weights = self._limit_turnover(
                weights, current_weights, self.constraints.max_turnover
            )
        
        return weights
    
    def _limit_turnover(
        self,
        target: Dict[str, float],
        current: Dict[str, float],
        max_turnover: float
    ) -> Dict[str, float]:
        """Limit turnover from current to target.
        
        If target turnover exceeds max, interpolate between current and target.
        """
        # Calculate turnover
        all_symbols = set(target.keys()) | set(current.keys())
        turnover = sum(
            abs(target.get(s, 0) - current.get(s, 0))
            for s in all_symbols
        ) / 2  # Divide by 2 because turnover is double-counted
        
        if turnover <= max_turnover:
            return target
        
        # Scale down changes
        scale = max_turnover / turnover if turnover > 0 else 0
        
        limited = {}
        for symbol in all_symbols:
            current_w = current.get(symbol, 0)
            target_w = target.get(symbol, 0)
            limited[symbol] = current_w + scale * (target_w - current_w)
        
        # Renormalize
        return self.normalize(limited)


class LotSizeRounder:
    """Round positions to lot sizes."""
    
    def __init__(
        self,
        lot_sizes: Optional[Dict[str, int]] = None,
        default_lot: int = 1
    ) -> None:
        """Initialize lot size rounder.
        
        Args:
            lot_sizes: Dict of symbol -> lot size
            default_lot: Default lot size
        """
        self.lot_sizes = lot_sizes or {}
        self.default_lot = default_lot
    
    def round_quantity(
        self,
        symbol: str,
        target_qty: float,
        price: float
    ) -> int:
        """Round quantity to lot size.
        
        Args:
            symbol: Trading symbol
            target_qty: Target quantity
            price: Current price
            
        Returns:
            Rounded quantity
        """
        lot_size = self.lot_sizes.get(symbol, self.default_lot)
        
        # Round to nearest lot
        rounded = round(target_qty / lot_size) * lot_size
        
        return int(rounded)
    
    def round_weights_to_shares(
        self,
        weights: Dict[str, float],
        prices: Dict[str, float],
        nav: float
    ) -> Dict[str, int]:
        """Convert weights to share quantities.
        
        Args:
            weights: Target weights
            prices: Current prices
            nav: Portfolio NAV
            
        Returns:
            Dict of symbol -> quantity
        """
        quantities = {}
        
        for symbol, weight in weights.items():
            if symbol not in prices or prices[symbol] == 0:
                quantities[symbol] = 0
                continue
            
            target_value = nav * weight
            target_qty = target_value / prices[symbol]
            
            quantities[symbol] = self.round_quantity(symbol, target_qty, prices[symbol])
        
        return quantities


class PortfolioBuilder:
    """Build portfolio from target weights.
    
    Integrates Risk Constraints into portfolio construction.
    """
    
    def __init__(
        self,
        optimizer: Optional[WeightOptimizer] = None,
        rounder: Optional[LotSizeRounder] = None,
        risk_checker: Optional["RiskConstraintChecker"] = None
    ) -> None:
        """Initialize portfolio builder.
        
        Args:
            optimizer: Weight optimizer
            rounder: Lot size rounder
            risk_checker: Risk constraint checker (NEW)
        """
        self.optimizer = optimizer or WeightOptimizer()
        self.rounder = rounder or LotSizeRounder()
        self.risk_checker = risk_checker
    
    def build_from_targets(
        self,
        targets: List[TargetWeight],
        prices: Dict[str, float],
        nav: float,
        current_state: Optional[PortfolioState] = None
    ) -> Dict[str, int]:
        """Build portfolio from target weights with risk constraints.
        
        Flow:
        1. Extract raw weights from targets
        2. Apply optimization (normalization, bounds)
        3. Apply RISK CONSTRAINTS (NEW)
        4. Convert to quantities
        
        Args:
            targets: List of target weights
            prices: Current prices
            nav: Portfolio NAV
            current_state: Current portfolio state
            
        Returns:
            Dict of symbol -> target quantity
        """
        # Extract weights
        raw_weights = {t.symbol: float(t.target_weight) for t in targets}
        
        # Get current weights for turnover calculation
        current_weights = None
        if current_state:
            current_weights = {
                symbol: float(pos.qty) * prices.get(symbol, 0) / float(nav)
                for symbol, pos in current_state.positions.items()
                if symbol in prices
            }
        
        # Step 1: Optimize weights (normalization, bounds)
        optimized = self.optimizer.optimize(raw_weights, current_weights)
        
        # Step 2: Apply Risk Constraints (NEW)
        if self.risk_checker:
            optimized = self.risk_checker.apply(
                optimized, 
                current_state=current_state,
                current_weights=current_weights
            )
        
        # Step 3: Convert to quantities
        quantities = self.rounder.round_weights_to_shares(optimized, prices, nav)
        
        return quantities
    
    def calculate_orders(
        self,
        target_quantities: Dict[str, int],
        current_state: PortfolioState
    ) -> List[Tuple[str, int]]:
        """Calculate orders needed to reach target quantities.
        
        Args:
            target_quantities: Target quantities
            current_state: Current portfolio state
            
        Returns:
            List of (symbol, order_qty) tuples
        """
        orders = []
        
        for symbol, target_qty in target_quantities.items():
            current_qty = current_state.positions.get(symbol, Position(symbol)).qty
            order_qty = target_qty - int(current_qty)
            
            if order_qty != 0:
                orders.append((symbol, order_qty))
        
        return orders
