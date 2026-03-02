"""Risk Constraints Module

Modular risk control for portfolio construction.
All constraints are configurable, toggleable, and log violations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from decimal import Decimal

import numpy as np
from loguru import logger

from quantlab.core.types import TargetWeight, PortfolioState


@dataclass
class RiskConstraintConfig:
    """Configuration for risk constraints.
    
    All constraints can be disabled by setting to None.
    """
    # Position limits
    max_weight_per_asset: Optional[float] = 0.30  # Max 30% in single asset
    min_weight_per_asset: Optional[float] = None  # No minimum by default
    
    # Portfolio limits
    max_leverage: Optional[float] = 1.5  # Max 1.5x gross exposure
    min_cash_ratio: Optional[float] = 0.05  # Min 5% cash
    max_cash_ratio: Optional[float] = None  # No max cash limit
    
    # Turnover limits
    max_turnover: Optional[float] = 0.50  # Max 50% single rebalance
    max_annual_turnover: Optional[float] = None  # Optional annual limit
    
    # Sector/Group limits
    max_sector_weight: Optional[float] = None  # Sector concentration limit
    
    # Risk metrics
    max_volatility: Optional[float] = None  # Max portfolio volatility
    max_drawdown_trigger: Optional[float] = None  # Stop if DD exceeds
    
    # Toggle switches
    enabled: bool = True
    strict_mode: bool = False  # If True, reject on violation; if False, clip
    
    @classmethod
    def from_dict(cls, config: Dict) -> "RiskConstraintConfig":
        """Create RiskConstraintConfig from config dictionary.
        
        Args:
            config: Dict with keys matching field names (e.g., from backtest.yaml)
            
        Returns:
            RiskConstraintConfig instance
            
        Example:
            >>> cfg = RiskConstraintConfig.from_dict({
            ...     "max_position_weight": 0.25,
            ...     "max_leverage": 1.2,
            ...     "max_turnover": 0.3,
            ...     "strict_mode": True
            ... })
        """
        # Map YAML keys to dataclass fields
        key_mapping = {
            "max_position_weight": "max_weight_per_asset",
            "min_position_weight": "min_weight_per_asset",
            "max_sector_weight": "max_sector_weight",
            "max_leverage": "max_leverage",
            "min_cash_ratio": "min_cash_ratio",
            "max_cash_ratio": "max_cash_ratio",
            "max_turnover": "max_turnover",
            "max_annual_turnover": "max_annual_turnover",
            "max_drawdown_stop": "max_drawdown_trigger",
            "strict_mode": "strict_mode",
        }
        
        kwargs = {}
        for yaml_key, field_name in key_mapping.items():
            if yaml_key in config:
                kwargs[field_name] = config[yaml_key]
        
        return cls(**kwargs)


@dataclass
class ConstraintViolation:
    """Record of a constraint violation."""
    constraint_name: str
    severity: str  # "warning" or "error"
    message: str
    current_value: float
    limit_value: float
    symbol: Optional[str] = None


class RiskConstraintChecker:
    """Check and enforce risk constraints.
    
    Usage:
        checker = RiskConstraintChecker(config)
        is_valid, violations = checker.check_weights(weights, current_state)
        adjusted_weights = checker.apply(weights, current_state)
    """
    
    def __init__(self, config: Optional[RiskConstraintConfig] = None):
        self.config = config or RiskConstraintConfig()
        self.violations: List[ConstraintViolation] = []
    
    def check_weights(
        self,
        weights: Dict[str, float],
        current_state: Optional[PortfolioState] = None,
        current_weights: Optional[Dict[str, float]] = None
    ) -> Tuple[bool, List[ConstraintViolation]]:
        """Check if weights violate any constraints.
        
        Returns:
            (is_valid, list_of_violations)
        """
        if not self.config.enabled:
            return True, []
        
        self.violations = []
        
        # 1. Check per-asset limits
        self._check_per_asset_limits(weights)
        
        # 2. Check portfolio-level limits
        self._check_portfolio_limits(weights, current_state)
        
        # 3. Check turnover limits
        if current_weights:
            self._check_turnover_limits(weights, current_weights)
        
        # Determine validity
        errors = [v for v in self.violations if v.severity == "error"]
        is_valid = len(errors) == 0
        
        return is_valid, self.violations
    
    def _check_per_asset_limits(self, weights: Dict[str, float]):
        """Check individual asset weight limits."""
        cfg = self.config
        
        for symbol, weight in weights.items():
            # Max weight per asset
            if cfg.max_weight_per_asset is not None:
                if abs(weight) > cfg.max_weight_per_asset:
                    self.violations.append(ConstraintViolation(
                        constraint_name="max_weight_per_asset",
                        severity="error" if cfg.strict_mode else "warning",
                        message=f"Weight {weight:.2%} exceeds max {cfg.max_weight_per_asset:.2%}",
                        current_value=abs(weight),
                        limit_value=cfg.max_weight_per_asset,
                        symbol=symbol
                    ))
            
            # Min weight per asset
            if cfg.min_weight_per_asset is not None:
                if 0 < abs(weight) < cfg.min_weight_per_asset:
                    self.violations.append(ConstraintViolation(
                        constraint_name="min_weight_per_asset",
                        severity="warning",
                        message=f"Weight {weight:.2%} below min {cfg.min_weight_per_asset:.2%}",
                        current_value=abs(weight),
                        limit_value=cfg.min_weight_per_asset,
                        symbol=symbol
                    ))
    
    def _check_portfolio_limits(
        self,
        weights: Dict[str, float],
        current_state: Optional[PortfolioState]
    ):
        """Check portfolio-level limits."""
        cfg = self.config
        
        total_long = sum(w for w in weights.values() if w > 0)
        total_short = sum(abs(w) for w in weights.values() if w < 0)
        gross_exposure = total_long + total_short
        net_exposure = total_long - total_short
        
        # Leverage check
        if cfg.max_leverage is not None:
            if gross_exposure > cfg.max_leverage:
                self.violations.append(ConstraintViolation(
                    constraint_name="max_leverage",
                    severity="error",
                    message=f"Gross exposure {gross_exposure:.2f}x exceeds max {cfg.max_leverage:.2f}x",
                    current_value=gross_exposure,
                    limit_value=cfg.max_leverage
                ))
        
        # Cash ratio check
        cash_weight = 1.0 - net_exposure
        
        if cfg.min_cash_ratio is not None:
            if cash_weight < cfg.min_cash_ratio:
                self.violations.append(ConstraintViolation(
                    constraint_name="min_cash_ratio",
                    severity="warning",
                    message=f"Cash ratio {cash_weight:.2%} below min {cfg.min_cash_ratio:.2%}",
                    current_value=cash_weight,
                    limit_value=cfg.min_cash_ratio
                ))
        
        if cfg.max_cash_ratio is not None:
            if cash_weight > cfg.max_cash_ratio:
                self.violations.append(ConstraintViolation(
                    constraint_name="max_cash_ratio",
                    severity="warning",
                    message=f"Cash ratio {cash_weight:.2%} above max {cfg.max_cash_ratio:.2%}",
                    current_value=cash_weight,
                    limit_value=cfg.max_cash_ratio
                ))
    
    def _check_turnover_limits(
        self,
        target_weights: Dict[str, float],
        current_weights: Dict[str, float]
    ):
        """Check turnover limits."""
        cfg = self.config
        
        if cfg.max_turnover is None:
            return
        
        # Calculate turnover
        all_symbols = set(target_weights.keys()) | set(current_weights.keys())
        turnover = sum(
            abs(target_weights.get(s, 0) - current_weights.get(s, 0))
            for s in all_symbols
        ) / 2  # Divide by 2 because turnover is double-counted
        
        if turnover > cfg.max_turnover:
            self.violations.append(ConstraintViolation(
                constraint_name="max_turnover",
                severity="warning",
                message=f"Turnover {turnover:.2%} exceeds max {cfg.max_turnover:.2%}",
                current_value=turnover,
                limit_value=cfg.max_turnover
            ))
    
    def apply(
        self,
        weights: Dict[str, float],
        current_state: Optional[PortfolioState] = None,
        current_weights: Optional[Dict[str, float]] = None
    ) -> Dict[str, float]:
        """Apply constraints to weights (clip to limits).
        
        Returns adjusted weights that satisfy all constraints.
        """
        if not self.config.enabled:
            return weights
        
        adjusted = dict(weights)
        cfg = self.config
        
        # 1. Clip per-asset weights
        if cfg.max_weight_per_asset is not None:
            for symbol in adjusted:
                w = adjusted[symbol]
                if abs(w) > cfg.max_weight_per_asset:
                    adjusted[symbol] = np.sign(w) * cfg.max_weight_per_asset
                    logger.debug(f"Clipped {symbol} weight from {w:.2%} to {cfg.max_weight_per_asset:.2%}")
        
        # 2. Normalize to ensure sum = 1 (if long-only)
        total = sum(adjusted.values())
        if total != 0 and abs(total - 1.0) > 0.001:
            adjusted = {k: v / total for k, v in adjusted.items()}
        
        # 3. Check and log violations
        is_valid, violations = self.check_weights(adjusted, current_state, current_weights)
        
        for v in violations:
            if v.severity == "error":
                logger.warning(f"[RISK] {v.constraint_name}: {v.message}")
            else:
                logger.info(f"[RISK] {v.constraint_name}: {v.message}")
        
        return adjusted
    
    def get_summary(self) -> Dict[str, any]:
        """Get constraint checker summary."""
        return {
            "enabled": self.config.enabled,
            "strict_mode": self.config.strict_mode,
            "max_weight_per_asset": self.config.max_weight_per_asset,
            "max_leverage": self.config.max_leverage,
            "max_turnover": self.config.max_turnover,
            "last_violations_count": len(self.violations),
        }


class RiskConstraintSet:
    """Multiple constraint checkers combined."""
    
    def __init__(self, checkers: Optional[List[RiskConstraintChecker]] = None):
        self.checkers = checkers or []
    
    def add(self, checker: RiskConstraintChecker):
        """Add a constraint checker."""
        self.checkers.append(checker)
    
    def check(
        self,
        weights: Dict[str, float],
        current_state: Optional[PortfolioState] = None,
        current_weights: Optional[Dict[str, float]] = None
    ) -> Tuple[bool, List[ConstraintViolation]]:
        """Check all constraints."""
        all_violations = []
        
        for checker in self.checkers:
            is_valid, violations = checker.check_weights(weights, current_state, current_weights)
            all_violations.extend(violations)
        
        errors = [v for v in all_violations if v.severity == "error"]
        return len(errors) == 0, all_violations
    
    def apply_all(
        self,
        weights: Dict[str, float],
        current_state: Optional[PortfolioState] = None,
        current_weights: Optional[Dict[str, float]] = None
    ) -> Dict[str, float]:
        """Apply all constraints sequentially."""
        result = dict(weights)
        
        for checker in self.checkers:
            result = checker.apply(result, current_state, current_weights)
        
        return result
