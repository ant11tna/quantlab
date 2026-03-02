"""Research module for quantlab.

Contains strategies, portfolio construction, risk analysis, and reporting.
"""

from quantlab.research.strategies.base import (
    Strategy,
    EqualWeightStrategy,
    BuyAndHoldStrategy,
    ThresholdRebalanceStrategy,
)
from quantlab.research.portfolio import (
    WeightConstraints,
    WeightOptimizer,
    LotSizeRounder,
    PortfolioBuilder,
)
from quantlab.research.risk import (
    RiskMetrics,
    RiskDecomposition,
    RiskAnalyzer,
    CorrelationAnalyzer,
)
from quantlab.research.reports import ReportGenerator

# Phase 1-3: Risk and Regime
from quantlab.research.risk_constraints import (
    RiskConstraintConfig,
    RiskConstraintChecker,
    ConstraintViolation,
    RiskConstraintSet,
)
from quantlab.research.risk_regime import (
    RiskState,
    RiskRegimeConfig,
    RiskRegimeDetector,
    RegimeSignal,
    RiskRegimeStrategy,
)

__all__ = [
    # Strategies
    "Strategy",
    "EqualWeightStrategy",
    "BuyAndHoldStrategy",
    "ThresholdRebalanceStrategy",
    # Portfolio
    "WeightConstraints",
    "WeightOptimizer",
    "LotSizeRounder",
    "PortfolioBuilder",
    # Risk Analysis
    "RiskMetrics",
    "RiskDecomposition",
    "RiskAnalyzer",
    "CorrelationAnalyzer",
    # Risk Constraints (Phase 3)
    "RiskConstraintConfig",
    "RiskConstraintChecker",
    "ConstraintViolation",
    "RiskConstraintSet",
    # Risk Regime (Phase 2)
    "RiskState",
    "RiskRegimeConfig",
    "RiskRegimeDetector",
    "RegimeSignal",
    "RiskRegimeStrategy",
    # Reports
    "ReportGenerator",
]
