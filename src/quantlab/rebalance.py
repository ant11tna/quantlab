"""Rebalance rule abstractions for portfolio reallocation decisions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict


def _norm_frequency(frequency: str) -> str:
    f = str(frequency or "monthly").strip().lower()
    if f not in {"monthly", "quarterly", "yearly"}:
        raise ValueError(f"Unsupported rebalance frequency: {frequency}")
    return f


def _period_bucket(dt: datetime, frequency: str) -> tuple[int, int]:
    if frequency == "monthly":
        return (dt.year, dt.month)
    if frequency == "quarterly":
        return (dt.year, (dt.month - 1) // 3 + 1)
    return (dt.year, 1)


class RebalanceRule(ABC):
    """Abstract rebalancing decision rule."""

    @abstractmethod
    def should_rebalance(
        self,
        dt: datetime,
        current_weights: Dict[str, float],
        target_weights: Dict[str, float],
    ) -> bool:
        """Return True if rebalance should be executed at dt."""


class PeriodicRebalance(RebalanceRule):
    """Rebalance when a new period starts (monthly / quarterly / yearly)."""

    def __init__(self, frequency: str = "monthly") -> None:
        self.frequency = _norm_frequency(frequency)
        self._last_bucket: tuple[int, int] | None = None

    def should_rebalance(
        self,
        dt: datetime,
        current_weights: Dict[str, float],
        target_weights: Dict[str, float],
    ) -> bool:
        bucket = _period_bucket(dt, self.frequency)
        if self._last_bucket is None or bucket != self._last_bucket:
            self._last_bucket = bucket
            return True
        return False


class ThresholdRebalance(RebalanceRule):
    """Rebalance when any symbol deviates above threshold from target."""

    def __init__(self, threshold: float) -> None:
        self.threshold = float(threshold)

    def should_rebalance(
        self,
        dt: datetime,
        current_weights: Dict[str, float],
        target_weights: Dict[str, float],
    ) -> bool:
        symbols = set(current_weights.keys()) | set(target_weights.keys())
        for symbol in symbols:
            current = float(current_weights.get(symbol, 0.0))
            target = float(target_weights.get(symbol, 0.0))
            if abs(current - target) > self.threshold:
                return True
        return False


class HybridRebalance(RebalanceRule):
    """Rebalance when either periodic or threshold condition is met."""

    def __init__(self, frequency: str = "monthly", threshold: float = 0.05) -> None:
        self.periodic = PeriodicRebalance(frequency)
        self.threshold = ThresholdRebalance(threshold)

    def should_rebalance(
        self,
        dt: datetime,
        current_weights: Dict[str, float],
        target_weights: Dict[str, float],
    ) -> bool:
        return self.periodic.should_rebalance(dt, current_weights, target_weights) or self.threshold.should_rebalance(
            dt, current_weights, target_weights
        )


def build_rebalance_rule(config: dict | None) -> RebalanceRule:
    """Build rebalance rule from config block.

    Supported config:
    rebalance:
      type: periodic / threshold / hybrid
      frequency: monthly
      threshold: 0.05
    """
    cfg = config or {}
    rtype = str(cfg.get("type", "periodic")).strip().lower()
    frequency = str(cfg.get("frequency", "monthly")).strip().lower()
    raw_threshold = cfg.get("threshold", 0.05)
    threshold = 0.05 if raw_threshold is None else float(raw_threshold)

    if rtype == "periodic":
        return PeriodicRebalance(frequency=frequency)
    if rtype == "threshold":
        return ThresholdRebalance(threshold=threshold)
    if rtype == "hybrid":
        return HybridRebalance(frequency=frequency, threshold=threshold)

    # Fallback default for robustness
    return PeriodicRebalance(frequency="monthly")
