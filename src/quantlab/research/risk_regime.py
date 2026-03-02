"""Risk Regime Module

First layer of decision: Risk ON / Risk OFF
Determines overall portfolio risk posture based on market conditions.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger


class RiskState(Enum):
    """Overall risk regime state."""
    RISK_ON = auto()   # Full risk allocation
    RISK_OFF = auto()  # Defensive posture


@dataclass
class RiskRegimeConfig:
    """Configuration for risk regime detection."""
    
    # Moving average regime
    ma_window: int = 200  # 200-day moving average
    
    # Momentum regime
    momentum_lookback: int = 126  # 6 months (~126 trading days)
    momentum_short: int = 21      # 1 month for short-term
    
    # Risk-off triggers (ANY of these triggers risk-off)
    price_below_ma: bool = True      # Price below 200MA
    negative_momentum: bool = True   # 6-1 momentum negative
    
    # Defensive asset allocation in risk-off
    risk_off_equity_pct: float = 0.30  # Reduce equity to 30%
    risk_off_bond_pct: float = 0.50    # Increase bonds to 50%
    risk_off_gold_pct: float = 0.20    # Increase gold to 20%
    
    # Logging
    verbose: bool = True


@dataclass
class RegimeSignal:
    """Risk regime signal output."""
    state: RiskState
    timestamp: datetime
    price_vs_ma: float  # Current price / MA ratio
    momentum_6m: float  # 6-month momentum
    triggers: List[str]  # Which conditions triggered


class RiskRegimeDetector:
    """Detect market risk regime (Risk ON/OFF).
    
    First layer of two-layer decision structure:
    1. Risk Regime: Determine overall risk posture
    2. Rotation Strategy: Select specific assets within regime
    """
    
    def __init__(self, config: Optional[RiskRegimeConfig] = None):
        self.config = config or RiskRegimeConfig()
        self.history: List[RegimeSignal] = []
    
    def detect(
        self,
        data: pd.DataFrame,
        equity_symbols: List[str],
        timestamp: Optional[datetime] = None
    ) -> RegimeSignal:
        """Detect current risk regime.
        
        Args:
            data: Market data DataFrame
            equity_symbols: List of equity symbols to check
            timestamp: Current timestamp (default: max in data)
            
        Returns:
            RegimeSignal with state and metadata
        """
        if timestamp is None:
            timestamp = data["ts"].max()
        
        cfg = self.config
        triggers = []
        
        # Calculate aggregate equity basket performance
        basket_data = self._get_basket_data(data, equity_symbols)
        
        if basket_data.empty or len(basket_data) < cfg.ma_window:
            logger.warning(f"Insufficient data for regime detection (< {cfg.ma_window} days)")
            return RegimeSignal(
                state=RiskState.RISK_ON,  # Default to risk-on if uncertain
                timestamp=timestamp,
                price_vs_ma=1.0,
 momentum_6m=0.0,
                triggers=["insufficient_data"]
            )
        
        current_price = basket_data["close"].iloc[-1]
        
        # Check 1: Price vs Moving Average
        ma_value = basket_data["close"].iloc[-cfg.ma_window:].mean()
        price_vs_ma = current_price / ma_value if ma_value > 0 else 1.0
        
        price_below_ma = price_vs_ma < 1.0
        if price_below_ma and cfg.price_below_ma:
            triggers.append(f"price_below_{cfg.ma_window}ma")
        
        # Check 2: Momentum (6-month vs 1-month)
        if len(basket_data) >= cfg.momentum_lookback:
            price_6m_ago = basket_data["close"].iloc[-cfg.momentum_lookback]
            price_1m_ago = basket_data["close"].iloc[-cfg.momentum_short]
            
            momentum_6m = (current_price / price_6m_ago) - 1
            momentum_1m = (current_price / price_1m_ago) - 1
            
            # Negative momentum if 6m return < 0 or 6m < 1m (decelerating)
            negative_momentum = momentum_6m < 0
            
            if negative_momentum and cfg.negative_momentum:
                triggers.append(f"negative_{cfg.momentum_lookback}d_momentum")
        else:
            momentum_6m = 0.0
        
        # Determine regime
        if len(triggers) > 0:
            state = RiskState.RISK_OFF
            if cfg.verbose:
                logger.info(f"[RISK REGIME] RISK_OFF triggered by: {triggers}")
        else:
            state = RiskState.RISK_ON
            if cfg.verbose and self.history and self.history[-1].state == RiskState.RISK_OFF:
                logger.info(f"[RISK REGIME] RISK_ON resumed")
        
        signal = RegimeSignal(
            state=state,
            timestamp=timestamp,
            price_vs_ma=price_vs_ma,
            momentum_6m=momentum_6m if 'momentum_6m' in locals() else 0.0,
            triggers=triggers
        )
        
        self.history.append(signal)
        return signal
    
    def _get_basket_data(
        self,
        data: pd.DataFrame,
        symbols: List[str]
    ) -> pd.DataFrame:
        """Get aggregate basket price data."""
        if not symbols:
            return pd.DataFrame()
        
        # Filter to basket symbols
        basket_df = data[data["symbol"].isin(symbols)].copy()
        
        if basket_df.empty:
            return pd.DataFrame()
        
        # Create equal-weight basket
        pivot = basket_df.pivot(index="ts", columns="symbol", values="close")
        
        # Calculate equal-weight index
        basket_price = pivot.mean(axis=1)
        
        return pd.DataFrame({
            "ts": basket_price.index,
            "close": basket_price.values
        })
    
    def get_defensive_allocation(self) -> Dict[str, float]:
        """Get defensive asset allocation for RISK_OFF state."""
        cfg = self.config
        return {
            "equity": cfg.risk_off_equity_pct,
            "bond": cfg.risk_off_bond_pct,
            "gold": cfg.risk_off_gold_pct,
        }
    
    def get_regime_history(self) -> pd.DataFrame:
        """Get history of regime changes."""
        if not self.history:
            return pd.DataFrame()
        
        return pd.DataFrame([
            {
                "timestamp": s.timestamp,
                "state": s.state.name,
                "price_vs_ma": s.price_vs_ma,
                "momentum_6m": s.momentum_6m,
                "triggers": ",".join(s.triggers)
            }
            for s in self.history
        ])


class RiskRegimeStrategy:
    """Two-layer strategy with risk regime + rotation.
    
    Layer 1: Risk Regime (this class)
        - Determine RISK_ON or RISK_OFF
        - Adjust overall equity/bond/gold allocation
    
    Layer 2: Rotation Strategy (within equity allocation)
        - Select specific ETFs within equity bucket
        - Momentum-based rotation
    """
    
    def __init__(
        self,
        regime_detector: RiskRegimeDetector,
        equity_symbols: List[str],
        bond_symbol: str = "511010",  # 国债ETF
        gold_symbol: str = "518880",  # 黄金ETF
        top_k: int = 3,
        momentum_window: int = 60
    ):
        self.regime_detector = regime_detector
        self.equity_symbols = equity_symbols
        self.bond_symbol = bond_symbol
        self.gold_symbol = gold_symbol
        self.top_k = top_k
        self.momentum_window = momentum_window
    
    def generate_weights(
        self,
        data: pd.DataFrame,
        timestamp: datetime
    ) -> Dict[str, float]:
        """Generate target weights based on risk regime and rotation.
        
        Returns:
            Dict of symbol -> weight
        """
        # Layer 1: Determine risk regime
        regime = self.regime_detector.detect(data, self.equity_symbols, timestamp)
        
        if regime.state == RiskState.RISK_OFF:
            # Defensive allocation
            return self._generate_risk_off_weights(data)
        else:
            # Full risk allocation with rotation
            return self._generate_risk_on_weights(data)
    
    def _generate_risk_off_weights(self, data: pd.DataFrame) -> Dict[str, float]:
        """Generate defensive weights for RISK_OFF state."""
        cfg = self.regime_detector.config
        
        weights = {}
        
        # Reduce equity to minimum
        if self.equity_symbols:
            # Still do rotation, but with reduced allocation
            equity_weights = self._calculate_momentum_weights(data, self.equity_symbols)
            for symbol, w in equity_weights.items():
                weights[symbol] = w * cfg.risk_off_equity_pct
        
        # Increase bonds and gold
        weights[self.bond_symbol] = cfg.risk_off_bond_pct
        weights[self.gold_symbol] = cfg.risk_off_gold_pct
        
        return weights
    
    def _generate_risk_on_weights(self, data: pd.DataFrame) -> Dict[str, float]:
        """Generate full risk weights with momentum rotation."""
        # Full equity allocation with momentum rotation
        return self._calculate_momentum_weights(data, self.equity_symbols)
    
    def _calculate_momentum_weights(
        self,
        data: pd.DataFrame,
        symbols: List[str]
    ) -> Dict[str, float]:
        """Calculate momentum-based weights for top K symbols."""
        if not symbols:
            return {}
        
        # Calculate momentum for each symbol
        momentum_scores = {}
        
        for symbol in symbols:
            symbol_data = data[data["symbol"] == symbol].sort_values("ts")
            
            if len(symbol_data) < self.momentum_window:
                continue
            
            current = symbol_data["close"].iloc[-1]
            past = symbol_data["close"].iloc[-self.momentum_window]
            
            momentum = (current / past) - 1 if past > 0 else 0
            momentum_scores[symbol] = momentum
        
        if not momentum_scores:
            # Equal weight if no data
            return {s: 1.0 / len(symbols) for s in symbols}
        
        # Select top K by momentum
        sorted_symbols = sorted(momentum_scores.items(), key=lambda x: x[1], reverse=True)
        top_symbols = [s for s, _ in sorted_symbols[:self.top_k]]
        
        # Equal weight among top K
        weight_per_symbol = 1.0 / len(top_symbols) if top_symbols else 0
        
        return {s: weight_per_symbol for s in top_symbols}
