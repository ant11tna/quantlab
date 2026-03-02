"""Research report generation.

Generate markdown reports from backtest results.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

from quantlab.core.types import RunConfig
from quantlab.research.risk import RiskMetrics


class ReportGenerator:
    """Generate research reports."""
    
    def __init__(self, output_dir: str | Path) -> None:
        """Initialize report generator.
        
        Args:
            output_dir: Directory to save reports
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_report(
        self,
        run_id: str,
        config: RunConfig,
        metrics: RiskMetrics,
        trades_df: Optional[pd.DataFrame] = None,
        weights_df: Optional[pd.DataFrame] = None,
        equity_df: Optional[pd.DataFrame] = None,
        insights: Optional[List[str]] = None
    ) -> Path:
        """Generate full markdown report.
        
        Args:
            run_id: Experiment run ID
            config: Run configuration
            metrics: Risk metrics
            trades_df: Trades DataFrame
            weights_df: Weights DataFrame
            equity_df: Equity curve DataFrame
            insights: List of insights/recommendations
            
        Returns:
            Path to generated report
        """
        report_path = self.output_dir / f"{run_id}_report.md"
        
        sections = [
            self._header(run_id, config),
            self._executive_summary(metrics),
            self._metrics_table(metrics),
            self._trades_summary(trades_df),
            self._insights(insights),
            self._next_steps(),
        ]
        
        content = "\n\n".join(sections)
        
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(content)
        
        logger.info(f"Generated report: {report_path}")
        return report_path
    
    def _header(self, run_id: str, config: RunConfig) -> str:
        """Generate report header."""
        return f"""# Backtest Report: {run_id}

**Generated**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Configuration

| Parameter | Value |
|-----------|-------|
| Universe | {config.universe} |
| Start Date | {config.start_date.strftime("%Y-%m-%d")} |
| End Date | {config.end_date.strftime("%Y-%m-%d")} |
| Rebalance Freq | {config.rebalance_freq or "N/A"} |
| Threshold | {config.threshold or "N/A"} |
| Fee Model | {config.fee_model} |
"""
    
    def _executive_summary(self, metrics: RiskMetrics) -> str:
        """Generate executive summary."""
        return f"""## Executive Summary

### Key Metrics

- **Total Return**: {metrics.total_return:.2%}
- **Annualized Return**: {metrics.annualized_return:.2%}
- **Volatility**: {metrics.annualized_volatility:.2%}
- **Sharpe Ratio**: {metrics.sharpe_ratio:.2f}
- **Max Drawdown**: {metrics.max_drawdown:.2%}
- **Calmar Ratio**: {metrics.calmar_ratio:.2f}

### Risk Assessment

- **VaR (95%)**: {metrics.var_95:.2%}
- **CVaR (95%)**: {metrics.cvar_95:.2%}
- **Skewness**: {metrics.skewness:.2f}
- **Kurtosis**: {metrics.kurtosis:.2f}
"""
    
    def _metrics_table(self, metrics: RiskMetrics) -> str:
        """Generate detailed metrics table."""
        return f"""## Detailed Metrics

| Metric | Value |
|--------|-------|
| Total Return | {metrics.total_return:.4%} |
| Annualized Return | {metrics.annualized_return:.4%} |
| Volatility (Ann.) | {metrics.annualized_volatility:.4%} |
| Sharpe Ratio | {metrics.sharpe_ratio:.4f} |
| Sortino Ratio | {metrics.sortino_ratio:.4f} |
| Max Drawdown | {metrics.max_drawdown:.4%} |
| Max DD Duration | {metrics.max_drawdown_duration} days |
| VaR (95%) | {metrics.var_95:.4%} |
| VaR (99%) | {metrics.var_99:.4%} |
| CVaR (95%) | {metrics.cvar_95:.4%} |
| Beta | {metrics.beta:.4f if metrics.beta else "N/A"} |
| Alpha | {metrics.alpha:.4f if metrics.alpha else "N/A"} |
| Information Ratio | {metrics.information_ratio:.4f if metrics.information_ratio else "N/A"} |
"""
    
    def _trades_summary(self, trades_df: Optional[pd.DataFrame]) -> str:
        """Generate trades summary."""
        if trades_df is None or trades_df.empty:
            return "## Trades Summary\n\nNo trades recorded."
        
        n_trades = len(trades_df)
        total_volume = trades_df["qty"].abs().sum() if "qty" in trades_df.columns else 0
        total_fees = trades_df["fee"].sum() if "fee" in trades_df.columns else 0
        
        # Buy/Sell split
        if "side" in trades_df.columns:
            n_buys = len(trades_df[trades_df["side"] == "BUY"])
            n_sells = len(trades_df[trades_df["side"] == "SELL"])
        else:
            n_buys = n_sells = "N/A"
        
        return f"""## Trades Summary

| Metric | Value |
|--------|-------|
| Total Trades | {n_trades} |
| Buy Orders | {n_buys} |
| Sell Orders | {n_sells} |
| Total Volume | {total_volume:,.0f} |
| Total Fees | ${total_fees:,.2f} |

### Trade Distribution

(TODO: Add trade analysis charts)
"""
    
    def _insights(self, insights: Optional[List[str]]) -> str:
        """Generate insights section."""
        if not insights:
            return "## Insights\n\n*No insights recorded*"
        
        items = "\n".join(f"- {insight}" for insight in insights)
        return f"""## Insights

{items}
"""
    
    def _next_steps(self) -> str:
        """Generate next steps section."""
        return """## Next Steps / Recommendations

1. **Sensitivity Analysis**: Test parameter sensitivity
2. **Stress Test**: Evaluate under market stress scenarios
3. **Transaction Cost Analysis**: Detailed cost breakdown
4. **Correlation Regime Analysis**: Performance in different correlation environments
5. **Walk-Forward Validation**: Out-of-sample testing

---

*This report was auto-generated by QuantLab.*
"""
    
    def save_metrics_json(
        self,
        run_id: str,
        metrics: RiskMetrics,
        extra: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Save metrics as JSON.
        
        Args:
            run_id: Run ID
            metrics: Risk metrics
            extra: Additional data to include
            
        Returns:
            Path to saved file
        """
        path = self.output_dir / f"{run_id}_metrics.json"
        
        data = {
            "run_id": run_id,
            "generated_at": datetime.now().isoformat(),
            "metrics": {
                k: v for k, v in metrics.__dict__.items()
                if v is not None
            }
        }
        
        if extra:
            data.update(extra)
        
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
        
        return path
    
    def generate_comparison_table(
        self,
        runs: List[tuple[str, RiskMetrics]],
        output_path: Optional[Path] = None
    ) -> str:
        """Generate comparison table for multiple runs.
        
        Args:
            runs: List of (run_id, metrics) tuples
            output_path: Optional path to save
            
        Returns:
            Markdown table string
        """
        lines = [
            "| Run | Total Return | Ann. Return | Volatility | Sharpe | Max DD |",
            "|-----|-------------|-------------|------------|--------|--------|"
        ]
        
        for run_id, m in runs:
            lines.append(
                f"| {run_id} | {m.total_return:.2%} | {m.annualized_return:.2%} | "
                f"{m.annualized_volatility:.2%} | {m.sharpe_ratio:.2f} | {m.max_drawdown:.2%} |"
            )
        
        table = "\n".join(lines)
        
        if output_path:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write("# Run Comparison\n\n")
                f.write(table)
                f.write("\n")
        
        return table
