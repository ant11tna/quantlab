"""Base strategy interface.

Strategies generate target weights from market data.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

from quantlab.core.types import TargetWeight


class Strategy(ABC):
    """Abstract base class for portfolio strategies."""
    
    def __init__(self, name: str, params: Optional[dict] = None) -> None:
        """Initialize strategy.
        
        Args:
            name: Strategy name
            params: Strategy parameters
        """
        self.name = name
        self.params = params or {}
        self.is_fitted = False
    
    @abstractmethod
    def generate_targets(
        self,
        data: pd.DataFrame,
        current_time: datetime,
        current_weights: Optional[Dict[str, float]] = None
    ) -> List[TargetWeight]:
        """Generate target weights.
        
        Args:
            data: Market data up to current_time
            current_time: Current timestamp
            current_weights: Current portfolio weights (if rebalancing)
            
        Returns:
            List of target weights
        """
        raise NotImplementedError
    
    def fit(self, data: pd.DataFrame) -> Strategy:
        """Fit strategy to historical data (optional).
        
        Args:
            data: Historical data for fitting
            
        Returns:
            Self (for chaining)
        """
        self.is_fitted = True
        return self
    
    def on_rebalance(
        self,
        data: pd.DataFrame,
        rebalance_time: datetime
    ) -> List[TargetWeight]:
        """Called on rebalance event.
        
        Default implementation calls generate_targets.
        Override for custom rebalance logic.
        
        Args:
            data: Market data
            rebalance_time: Rebalance timestamp
            
        Returns:
            List of target weights
        """
        return self.generate_targets(data, rebalance_time)


class EqualWeightStrategy(Strategy):
    """Equal weight allocation strategy."""
    
    def __init__(
        self,
        symbols: List[str],
        exclude_symbols: Optional[List[str]] = None
    ) -> None:
        """Initialize equal weight strategy.
        
        Args:
            symbols: Universe of symbols
            exclude_symbols: Symbols to exclude
        """
        super().__init__("equal_weight")
        self.symbols = symbols
        self.exclude_symbols = set(exclude_symbols or [])
        self.active_symbols = [s for s in symbols if s not in self.exclude_symbols]
    
    def generate_targets(
        self,
        data: pd.DataFrame,
        current_time: datetime,
        current_weights: Optional[Dict[str, float]] = None
    ) -> List[TargetWeight]:
        """Generate equal weights for all symbols."""
        n = len(self.active_symbols)
        if n == 0:
            logger.warning("No active symbols for equal weight strategy")
            return []
        
        weight = 1.0 / n
        targets = []
        
        for symbol in self.active_symbols:
            targets.append(TargetWeight(
                ts=current_time,
                symbol=symbol,
                target_weight=weight,  # type: ignore
                source=self.name
            ))
        
        return targets


class BuyAndHoldStrategy(Strategy):
    """Buy and hold strategy with initial weights."""
    
    def __init__(self, initial_weights: Dict[str, float]) -> None:
        """Initialize buy and hold strategy.
        
        Args:
            initial_weights: Initial target weights
        """
        super().__init__("buy_and_hold")
        self.initial_weights = initial_weights
        self._first_call = True
    
    def generate_targets(
        self,
        data: pd.DataFrame,
        current_time: datetime,
        current_weights: Optional[Dict[str, float]] = None
    ) -> List[TargetWeight]:
        """Return initial weights on first call, empty afterwards."""
        if not self._first_call:
            # No rebalancing needed
            return []
        
        self._first_call = False
        
        targets = []
        for symbol, weight in self.initial_weights.items():
            targets.append(TargetWeight(
                ts=current_time,
                symbol=symbol,
                target_weight=weight,  # type: ignore
                source=self.name
            ))
        
        return targets


class ThresholdRebalanceStrategy(Strategy):
    """Rebalance when weights deviate beyond threshold."""
    
    def __init__(
        self,
        base_strategy: Strategy,
        threshold: float = 0.05,
        min_days: int = 5
    ) -> None:
        """Initialize threshold rebalance wrapper.
        
        Args:
            base_strategy: Underlying strategy
            threshold: Deviation threshold (e.g., 0.05 for 5%)
            min_days: Minimum days between rebalances
        """
        super().__init__(f"threshold_{base_strategy.name}")
        self.base_strategy = base_strategy
        self.threshold = threshold
        self.min_days = min_days
        self.last_rebalance: Optional[datetime] = None
        self.last_targets: Dict[str, float] = {}
    
    def generate_targets(
        self,
        data: pd.DataFrame,
        current_time: datetime,
        current_weights: Optional[Dict[str, float]] = None
    ) -> List[TargetWeight]:
        """Generate targets if threshold exceeded."""
        # Check minimum days
        if self.last_rebalance is not None:
            days_since = (current_time - self.last_rebalance).days
            if days_since < self.min_days:
                return []
        
        # Get base strategy targets
        targets = self.base_strategy.generate_targets(
            data, current_time, current_weights
        )
        target_dict = {t.symbol: float(t.target_weight) for t in targets}
        
        # Check if rebalance needed
        if current_weights is None:
            needs_rebalance = True
        else:
            needs_rebalance = False
            all_symbols = set(target_dict.keys()) | set(current_weights.keys())
            
            for symbol in all_symbols:
                current = current_weights.get(symbol, 0.0)
                target = target_dict.get(symbol, 0.0)
                
                if abs(current - target) > self.threshold:
                    needs_rebalance = True
                    break
        
        if needs_rebalance:
            self.last_rebalance = current_time
            self.last_targets = target_dict
            return targets
        
        return []
