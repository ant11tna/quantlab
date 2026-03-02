"""Backtest metrics calculation.

Performance and risk metrics from backtest results.
"""

from __future__ import annotations

from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from quantlab.research.risk import RiskMetrics, RiskAnalyzer


class MetricsCalculator:
    """Calculate backtest metrics."""
    
    def __init__(self, risk_free_rate: float = 0.02) -> None:
        """Initialize calculator.
        
        Args:
            risk_free_rate: Annual risk-free rate
        """
        self.risk_analyzer = RiskAnalyzer(risk_free_rate)
    
    def calculate(
        self,
        equity_df: pd.DataFrame,
        trades_df: Optional[pd.DataFrame] = None,
        benchmark_df: Optional[pd.DataFrame] = None
    ) -> Dict[str, any]:
        """Calculate all metrics.
        
        Args:
            equity_df: Equity curve DataFrame (ts, nav, ...)
            trades_df: Trades DataFrame
            benchmark_df: Optional benchmark equity curve
            
        Returns:
            Dictionary of metrics
        """
        if equity_df.empty:
            return {"error": "Empty equity curve"}
        
        # Calculate returns
        equity_df = equity_df.copy()
        equity_df["returns"] = equity_df["nav"].pct_change()
        
        # Risk metrics
        returns = equity_df["returns"].dropna()
        benchmark_returns = None
        if benchmark_df is not None:
            benchmark_returns = benchmark_df["nav"].pct_change().dropna()
        
        risk_metrics = self.risk_analyzer.calculate_metrics(
            returns, benchmark_returns
        )
        
        # Trading metrics
        trading_metrics = self._calculate_trading_metrics(trades_df)
        
        # Combine
        metrics = {
            "risk": risk_metrics,
            "trading": trading_metrics,
            "summary": self._summary_metrics(risk_metrics, trading_metrics),
        }
        
        return metrics
    
    def _calculate_trading_metrics(
        self,
        trades_df: Optional[pd.DataFrame]
    ) -> Dict:
        """Calculate trading-specific metrics."""
        if trades_df is None or trades_df.empty:
            return {
                "total_trades": 0,
                "total_volume": 0,
                "total_fees": 0,
                "total_impact_cost": 0,
                "impact_cost_ratio": 0,
                "avg_trade_size": 0,
            }
        
        metrics = {
            "total_trades": len(trades_df),
            "total_volume": float(trades_df["filled_qty"].abs().sum()),
            "total_fees": float(trades_df["fee"].sum()),
            "avg_trade_size": float(trades_df["filled_qty"].abs().mean()),
        }
        
        # Buy/Sell split
        if "side" in trades_df.columns:
            metrics["buy_trades"] = len(trades_df[trades_df["side"] == "BUY"])
            metrics["sell_trades"] = len(trades_df[trades_df["side"] == "SELL"])
        
        # Impact cost metrics (P1.5 capacity sensitivity)
        if "impact_cost" in trades_df.columns:
            total_impact = float(trades_df["impact_cost"].sum())
            metrics["total_impact_cost"] = total_impact
            
            # Calculate gross traded value for ratio
            gross_value = float((trades_df["filled_qty"].abs() * trades_df["price"]).sum())
            metrics["impact_cost_ratio"] = total_impact / gross_value if gross_value > 0 else 0
            
            # Average impact bps
            if "impact_bps" in trades_df.columns:
                metrics["avg_impact_bps"] = float(trades_df["impact_bps"].mean())
        else:
            metrics["total_impact_cost"] = 0
            metrics["impact_cost_ratio"] = 0
        
        # Turnover estimate
        if "nav" in trades_df.columns:
            avg_nav = trades_df["nav"].mean()
            metrics["turnover"] = metrics["total_volume"] / avg_nav if avg_nav > 0 else 0
        
        return metrics
    
    def _summary_metrics(
        self,
        risk: RiskMetrics,
        trading: Dict
    ) -> Dict:
        """Create summary metrics."""
        return {
            "total_return": risk.total_return,
            "annualized_return": risk.annualized_return,
            "volatility": risk.annualized_volatility,
            "sharpe_ratio": risk.sharpe_ratio,
            "max_drawdown": risk.max_drawdown,
            "calmar_ratio": risk.calmar_ratio,
            "total_trades": trading["total_trades"],
            "total_fees": trading["total_fees"],
            "total_impact_cost": trading.get("total_impact_cost", 0),
            "impact_cost_ratio": trading.get("impact_cost_ratio", 0),
        }
    
    def generate_report_text(self, metrics: Dict) -> str:
        """Generate text report."""
        summary = metrics.get("summary", {})
        risk = metrics.get("risk", RiskMetrics())
        trading = metrics.get("trading", {})
        
        lines = [
            "=" * 50,
            "BACKTEST RESULTS",
            "=" * 50,
            "",
            "Performance Metrics:",
            f"  Total Return:       {summary.get('total_return', 0):.2%}",
            f"  Annualized Return:  {summary.get('annualized_return', 0):.2%}",
            f"  Volatility:         {summary.get('volatility', 0):.2%}",
            f"  Sharpe Ratio:       {summary.get('sharpe_ratio', 0):.2f}",
            f"  Max Drawdown:       {summary.get('max_drawdown', 0):.2%}",
            f"  Calmar Ratio:       {summary.get('calmar_ratio', 0):.2f}",
            "",
            "Risk Metrics:",
            f"  VaR (95%):          {risk.var_95:.2%}",
            f"  CVaR (95%):         {risk.cvar_95:.2%}",
            f"  Skewness:           {risk.skewness:.2f}",
            f"  Kurtosis:           {risk.kurtosis:.2f}",
            "",
            "Trading Metrics:",
            f"  Total Trades:       {trading.get('total_trades', 0)}",
            f"  Total Volume:       {trading.get('total_volume', 0):,.0f}",
            f"  Total Fees:         ${trading.get('total_fees', 0):,.2f}",
            "=" * 50,
        ]
        
        return "\n".join(lines)
