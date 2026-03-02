"""Three-way reconciliation: targets vs orders vs fills.

Provides reconciliation between:
1. Strategy targets (TargetWeight)
2. Generated orders (symbol/side/order_qty)
3. Actual fills (trades with filled_qty, reject_reason)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger


@dataclass
class ReconcileSummary:
    """Summary of reconciliation results."""
    
    # Counts
    total_targets: int = 0
    total_orders: int = 0
    total_fills: int = 0
    
    # Order status breakdown
    fully_filled: int = 0
    partial_filled: int = 0
    rejected: int = 0
    
    # Rejection breakdown by reason
    reject_by_reason: Dict[str, int] = field(default_factory=dict)
    reject_by_category: Dict[str, int] = field(default_factory=dict)
    
    # Fill quality
    avg_fill_ratio: float = 0.0
    min_fill_ratio: float = 0.0
    max_fill_ratio: float = 0.0
    
    # Slippage
    avg_slippage_bps: float = 0.0
    avg_impact_bps: float = 0.0
    
    # Coverage
    targets_without_orders: List[str] = field(default_factory=list)
    orders_without_fills: List[str] = field(default_factory=list)


def load_reconcile_data(run_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load targets, orders, trades, and rejected orders DataFrames from run directory.
    
    Args:
        run_dir: Path to run directory (e.g., runs/<run_id>/)
        
    Returns:
        Tuple of (targets_df, orders_df, trades_df, rejected_df)
        
    Note:
        rejected_orders.parquet contains orders that were rejected with zero fills.
        This is separate from trades.parquet which only contains filled orders.
    """
    results_dir = run_dir / "results"
    
    targets = pd.read_parquet(results_dir / "targets.parquet") if (results_dir / "targets.parquet").exists() else pd.DataFrame()
    orders = pd.read_parquet(results_dir / "orders.parquet") if (results_dir / "orders.parquet").exists() else pd.DataFrame()
    trades = pd.read_parquet(results_dir / "trades.parquet") if (results_dir / "trades.parquet").exists() else pd.DataFrame()
    rejected = pd.read_parquet(results_dir / "rejected_orders.parquet") if (results_dir / "rejected_orders.parquet").exists() else pd.DataFrame()
    
    return targets, orders, trades, rejected


def reconcile(
    targets: pd.DataFrame,
    orders: pd.DataFrame,
    trades: pd.DataFrame,
    rejected: pd.DataFrame = None,
    run_id: str = ""
) -> ReconcileSummary:
    """Perform three-way reconciliation.
    
    Args:
        targets: TargetWeight records (ts, symbol, target_weight, source)
        orders: Order records (ts, symbol, side, order_qty, strategy_id)
        trades: Trade/fill records (with filled_qty, filled_ratio)
        rejected: Rejected order records (ts, symbol, side, qty, reject_reason)
                These are zero-fill rejections, separate from partial fills.
        run_id: Optional run identifier for logging
        
    Returns:
        ReconcileSummary with reconciliation results
    """
    if rejected is None:
        rejected = pd.DataFrame()
    summary = ReconcileSummary()
    
    # Basic counts
    summary.total_targets = len(targets)
    summary.total_orders = len(orders)
    summary.total_fills = len(trades)
    
    if trades.empty:
        logger.warning(f"[{run_id}] No trades to reconcile")
        return summary
    
    # Fill status breakdown
    if "filled_ratio" in trades.columns:
        summary.fully_filled = int((trades["filled_ratio"] >= 0.99).sum())
        summary.partial_filled = int(((trades["filled_ratio"] > 0) & (trades["filled_ratio"] < 0.99)).sum())
        summary.avg_fill_ratio = float(trades["filled_ratio"].mean())
        summary.min_fill_ratio = float(trades["filled_ratio"].min())
        summary.max_fill_ratio = float(trades["filled_ratio"].max())
    
    # Rejection analysis
    # Priority: use rejected_orders.parquet (zero-fill rejections)
    # Fallback: trades with reject_reason (partial fill rejections)
    if not rejected.empty and "reject_reason" in rejected.columns:
        # Use dedicated rejected orders file (captures all zero-fill rejections)
        summary.rejected = len(rejected)
        summary.reject_by_reason = rejected["reject_reason"].value_counts().to_dict()
        
        # By category
        from quantlab.execution.constraints import categorize_rejection
        categories = rejected["reject_reason"].apply(categorize_rejection)
        summary.reject_by_category = categories.value_counts().to_dict()
        
        logger.info(f"[{run_id}] Using rejected_orders.parquet for rejection stats: {summary.rejected} rejections")
    elif "reject_reason" in trades.columns:
        # Fallback: trades with reject_reason (legacy, only captures partial fill info)
        rejects = trades[trades["reject_reason"] != ""]
        summary.rejected = len(rejects)
        summary.reject_by_reason = rejects["reject_reason"].value_counts().to_dict()
        
        from quantlab.execution.constraints import categorize_rejection
        categories = rejects["reject_reason"].apply(categorize_rejection)
        summary.reject_by_category = categories.value_counts().to_dict()
        
        logger.warning(f"[{run_id}] Using trades.reject_reason for rejection stats (may miss zero-fill rejections)")
    
    # Slippage/impact stats
    if "impact_bps" in trades.columns:
        summary.avg_impact_bps = float(trades["impact_bps"].mean())
    if "slippage" in trades.columns:
        # Convert slippage per share to bps approximation
        trades_copy = trades.copy()
        trades_copy["slippage_bps"] = trades_copy["slippage"] / trades_copy["price"] * 10000
        summary.avg_slippage_bps = float(trades_copy["slippage_bps"].mean())
    
    # Coverage analysis: targets vs orders
    if not targets.empty and not orders.empty:
        target_symbols = set(targets["symbol"].unique())
        order_symbols = set(orders["symbol"].unique())
        missing = target_symbols - order_symbols
        if missing:
            summary.targets_without_orders = list(missing)
            logger.warning(f"[{run_id}] Targets without orders: {missing}")
    
    # Coverage analysis: orders vs fills (by symbol+ts)
    if not orders.empty and not trades.empty:
        # Group trades by symbol
        trade_symbols = set(trades["symbol"].unique())
        order_symbols = set(orders["symbol"].unique())
        missing = order_symbols - trade_symbols
        if missing:
            summary.orders_without_fills = list(missing)
            logger.warning(f"[{run_id}] Orders without fills: {missing}")
    
    return summary


def reconcile_to_dict(summary: ReconcileSummary) -> Dict:
    """Convert ReconcileSummary to dictionary for serialization."""
    return {
        "counts": {
            "total_targets": summary.total_targets,
            "total_orders": summary.total_orders,
            "total_fills": summary.total_fills,
            "fully_filled": summary.fully_filled,
            "partial_filled": summary.partial_filled,
            "rejected": summary.rejected,
        },
        "fill_quality": {
            "avg_fill_ratio": round(summary.avg_fill_ratio, 4),
            "min_fill_ratio": round(summary.min_fill_ratio, 4),
            "max_fill_ratio": round(summary.max_fill_ratio, 4),
        },
        "costs": {
            "avg_slippage_bps": round(summary.avg_slippage_bps, 2),
            "avg_impact_bps": round(summary.avg_impact_bps, 2),
        },
        "rejections": {
            "by_reason": summary.reject_by_reason,
            "by_category": summary.reject_by_category,
        },
        "coverage_gaps": {
            "targets_without_orders": summary.targets_without_orders,
            "orders_without_fills": summary.orders_without_fills,
        },
    }


def render_md(summary: ReconcileSummary, run_id: str = "") -> str:
    """Render reconciliation report as Markdown.
    
    Args:
        summary: ReconcileSummary object
        run_id: Run identifier
        
    Returns:
        Markdown formatted report string
    """
    lines = []
    lines.append(f"# Reconciliation Report: {run_id or 'Unknown Run'}")
    lines.append("")
    lines.append("## Summary Counts")
    lines.append("")
    lines.append(f"| Metric | Count |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Total Targets | {summary.total_targets} |")
    lines.append(f"| Total Orders | {summary.total_orders} |")
    lines.append(f"| Total Fills | {summary.total_fills} |")
    lines.append(f"| Fully Filled | {summary.fully_filled} |")
    lines.append(f"| Partial Filled | {summary.partial_filled} |")
    lines.append(f"| Rejected | {summary.rejected} |")
    lines.append("")
    
    lines.append("## Fill Quality")
    lines.append("")
    lines.append(f"- **Average Fill Ratio**: {summary.avg_fill_ratio:.2%}")
    lines.append(f"- **Min Fill Ratio**: {summary.min_fill_ratio:.2%}")
    lines.append(f"- **Max Fill Ratio**: {summary.max_fill_ratio:.2%}")
    lines.append("")
    
    lines.append("## Execution Costs")
    lines.append("")
    lines.append(f"- **Average Slippage**: {summary.avg_slippage_bps:.2f} bps")
    lines.append(f"- **Average Impact**: {summary.avg_impact_bps:.2f} bps")
    lines.append("")
    
    if summary.reject_by_reason:
        lines.append("## Rejection Breakdown")
        lines.append("")
        lines.append("### By Reason")
        lines.append("")
        lines.append("| Reason | Count |")
        lines.append("|--------|-------|")
        for reason, count in sorted(summary.reject_by_reason.items(), key=lambda x: -x[1]):
            lines.append(f"| {reason} | {count} |")
        lines.append("")
    
    if summary.reject_by_category:
        lines.append("### By Category")
        lines.append("")
        lines.append("| Category | Count |")
        lines.append("|----------|-------|")
        for cat, count in sorted(summary.reject_by_category.items(), key=lambda x: -x[1]):
            lines.append(f"| {cat} | {count} |")
        lines.append("")
    
    if summary.targets_without_orders:
        lines.append("## Coverage Gaps")
        lines.append("")
        if summary.targets_without_orders:
            lines.append(f"**Targets without Orders**: {', '.join(summary.targets_without_orders)}")
        if summary.orders_without_fills:
            lines.append(f"**Orders without Fills**: {', '.join(summary.orders_without_fills)}")
        lines.append("")
    
    return "\n".join(lines)


def save_reconcile_report(
    summary: ReconcileSummary,
    run_dir: Path,
    run_id: str = ""
) -> Path:
    """Save reconciliation report to run directory.
    
    Args:
        summary: ReconcileSummary object
        run_dir: Path to run directory
        run_id: Run identifier
        
    Returns:
        Path to saved report
    """
    results_dir = run_dir / "results"
    results_dir.mkdir(exist_ok=True)
    
    # Save JSON
    json_path = results_dir / "reconcile.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(reconcile_to_dict(summary), f, indent=2)
    
    # Save Markdown
    md_path = results_dir / "reconcile.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(render_md(summary, run_id))
    
    logger.info(f"Saved reconciliation report to {md_path}")
    return md_path


def quick_reconcile(run_dir: Path) -> ReconcileSummary:
    """Quick reconciliation from run directory.
    
    Args:
        run_dir: Path to run directory
        
    Returns:
        ReconcileSummary
    """
    run_id = run_dir.name
    targets, orders, trades, rejected = load_reconcile_data(run_dir)
    summary = reconcile(targets, orders, trades, rejected, run_id)
    save_reconcile_report(summary, run_dir, run_id)
    return summary
