"""Reconciliation Demo - Three-way reconciliation of targets/orders/fills.

Demonstrates the reconciliation workflow:
1. Load targets, orders, and trades from a run
2. Perform three-way reconciliation
3. Generate and view reconciliation report
"""

from __future__ import annotations

import sys
sys.path.insert(0, "src")
sys.path.insert(0, ".")

from pathlib import Path
from loguru import logger

from quantlab.research.reconcile import (
    load_reconcile_data,
    reconcile,
    reconcile_to_dict,
    render_md,
    save_reconcile_report,
    quick_reconcile,
)


def main():
    """Main demo function."""
    logger.info("=" * 60)
    logger.info("Reconciliation Demo - Three-way targets/orders/fills")
    logger.info("=" * 60)
    
    # Find latest run
    runs_dir = Path("runs")
    if not runs_dir.exists():
        logger.error("No runs directory found. Run a backtest first.")
        return
    
    run_dirs = [d for d in runs_dir.iterdir() if d.is_dir() and d.name != ".gitkeep"]
    if not run_dirs:
        logger.error("No runs found. Run a backtest first.")
        return
    
    # Use risk_constraints_demo or latest
    target_run = None
    for d in run_dirs:
        if "risk_constraints" in d.name:
            target_run = d
            break
    
    if not target_run:
        target_run = max(run_dirs, key=lambda d: d.stat().st_mtime)
    
    run_id = target_run.name
    logger.info(f"\nAnalyzing run: {run_id}")
    logger.info(f"Path: {target_run}")
    
    # Method 1: Step-by-step reconciliation
    logger.info("\n" + "-" * 40)
    logger.info("Method 1: Step-by-step reconciliation")
    logger.info("-" * 40)
    
    targets, orders, trades = load_reconcile_data(target_run)
    
    logger.info(f"\nData loaded:")
    logger.info(f"  Targets: {len(targets)} records")
    logger.info(f"  Orders: {len(orders)} records")
    logger.info(f"  Trades: {len(trades)} records")
    
    if not trades.empty:
        logger.info(f"\nTrades columns: {list(trades.columns)}")
        logger.info(f"\nSample trades:\n{trades.head(3).to_string()}")
    
    # Perform reconciliation
    summary = reconcile(targets, orders, trades, run_id)
    
    logger.info(f"\nReconciliation Summary:")
    logger.info(f"  Total targets: {summary.total_targets}")
    logger.info(f"  Total orders: {summary.total_orders}")
    logger.info(f"  Total fills: {summary.total_fills}")
    logger.info(f"  Fully filled: {summary.fully_filled}")
    logger.info(f"  Partial filled: {summary.partial_filled}")
    logger.info(f"  Rejected: {summary.rejected}")
    
    if summary.avg_fill_ratio > 0:
        logger.info(f"\nFill Quality:")
        logger.info(f"  Avg fill ratio: {summary.avg_fill_ratio:.2%}")
        logger.info(f"  Min fill ratio: {summary.min_fill_ratio:.2%}")
        logger.info(f"  Max fill ratio: {summary.max_fill_ratio:.2%}")
    
    if summary.reject_by_reason:
        logger.info(f"\nRejections by reason:")
        for reason, count in sorted(summary.reject_by_reason.items(), key=lambda x: -x[1]):
            logger.info(f"  {reason}: {count}")
    
    # Convert to dict for JSON serialization
    report_dict = reconcile_to_dict(summary)
    logger.info(f"\nReport dict:\n{report_dict}")
    
    # Generate markdown report
    md_report = render_md(summary, run_id)
    logger.info(f"\nMarkdown report preview (first 1000 chars):\n{md_report[:1000]}...")
    
    # Method 2: Quick reconcile (one-liner)
    logger.info("\n" + "-" * 40)
    logger.info("Method 2: Quick reconcile (one-liner)")
    logger.info("-" * 40)
    
    summary2 = quick_reconcile(target_run)
    logger.info(f"Quick reconcile complete!")
    logger.info(f"  Report saved to: {target_run / 'results' / 'reconcile.md'}")
    logger.info(f"  JSON saved to: {target_run / 'results' / 'reconcile.json'}")
    
    # Display the generated report
    logger.info("\n" + "=" * 60)
    logger.info("Generated Reconciliation Report")
    logger.info("=" * 60)
    
    report_path = target_run / "results" / "reconcile.md"
    if report_path.exists():
        with open(report_path) as f:
            content = f.read()
            print(content)
    
    logger.info("\n" + "=" * 60)
    logger.info("Reconciliation Demo Complete!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
