"""Command line interface for QuantLab.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import yaml
from loguru import logger


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = "DEBUG" if verbose else "INFO"
    logger.remove()
    logger.add(sys.stderr, level=level, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")


def cmd_backtest(args: argparse.Namespace) -> int:
    """Run backtest command with full run tracking."""
    from quantlab.backtest.engine import BacktestEngine
    from quantlab.core.runlog import (
        create_run_dir,
        write_run_metadata,
        finalize_run,
        load_run_metrics,
    )
    from quantlab.data.sources.local_csv import MockDataSource
    from quantlab.data.ingest import DataIngestor
    from quantlab.research.strategies.base import EqualWeightStrategy
    
    logger.info(f"Running backtest with config: {args.config}")
    
    # Load config (keep original text for hash)
    config_path = Path(args.config)
    with open(config_path, 'r', encoding='utf-8') as f:
        config_text = f.read()
    config = yaml.safe_load(config_text)
    
    # Create run directory with full metadata
    run_dir = create_run_dir(
        base="runs",
        name="backtest",
        config_text=config_text
    )
    
    # Write metadata before running
    write_run_metadata(
        run_dir=run_dir,
        config_path=config_path,
        config_text=config_text,
        data_manifest=None,  # Will be generated
        git_rev=None,        # Will be auto-detected
        env_info=None        # Will be auto-detected
    )
    
    # Create mock data source for demo
    symbols = ["SPY", "TLT", "GLD"]
    data_source = MockDataSource(symbols=symbols)
    
    # Generate mock data
    ingestor = DataIngestor(data_source, "data/curated")
    
    start = datetime(2020, 1, 1)
    end = datetime(2024, 12, 31)
    
    data = ingestor.ingest(symbols, start, end)
    
    # Create strategy
    strategy = EqualWeightStrategy(symbols=symbols)
    
    # Extract execution config from loaded config
    exec_config = config.get("default", {}).get("execution", {}) if config else {}
    
    # Run backtest
    engine = BacktestEngine(
        strategy=strategy,
        initial_cash=1_000_000.0,
        fee_model="us_etfs",
        exec_config=exec_config
    )
    
    results = engine.run(
        data=data,
        rebalance_freq="M",
        progress=True
    )
    
    # Prepare artifacts for results/ subdirectory
    artifacts = {}
    
    # Equity curve
    equity_path = run_dir / "equity_curve.parquet"
    results["equity_curve"].to_parquet(equity_path, index=False)
    artifacts["equity_curve"] = equity_path
    
    # Weights
    weights_path = run_dir / "weights.parquet"
    results["weights"].to_parquet(weights_path, index=False)
    artifacts["weights"] = weights_path
    
    # Fills/Trades
    if not results["trades"].empty:
        fills_path = run_dir / "fills.parquet"
        results["trades"].to_parquet(fills_path, index=False)
        artifacts["fills"] = fills_path
    
    # Positions (generate from equity curve)
    positions_df = results.get("positions")
    if positions_df is not None and not positions_df.empty:
        positions_path = run_dir / "positions.parquet"
        positions_df.to_parquet(positions_path, index=False)
        artifacts["positions"] = positions_path
    
    # Finalize run with artifacts and metrics
    finalize_run(
        run_dir=run_dir,
        artifacts=artifacts,
        metrics=results["metrics"]
    )
    
    # Also persist to DuckDB for querying
    try:
        from quantlab.db.persist import persist_run
        persist_run(
            run_id=run_dir.name,
            run_dir=run_dir,
            results=results,
            config=config,
            snapshot=None,
        )
        logger.info(f"Run persisted to DuckDB")
    except Exception as e:
        logger.warning(f"Failed to persist to DuckDB: {e}")
    
    # Print summary
    metrics = results["metrics"]
    summary = metrics.get("summary", {})
    
    print("\n" + "=" * 60)
    print("BACKTEST RESULTS")
    print("=" * 60)
    print(f"Run ID: {run_dir.name}")
    print(f"Total Return: {summary.get('total_return', 0):.2%}")
    print(f"Sharpe Ratio: {summary.get('sharpe_ratio', 0):.2f}")
    print(f"Max Drawdown: {summary.get('max_drawdown', 0):.2%}")
    print(f"Total Trades: {metrics.get('trading', {}).get('total_trades', 0)}")
    print(f"\nResults saved to: {run_dir}")
    print("=" * 60)
    
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    """Compare multiple runs."""
    from quantlab.core.runlog import load_run_metrics
    
    run_dirs = [Path(p) for p in args.run_dirs]
    
    # Validate paths
    for run_dir in run_dirs:
        if not run_dir.exists():
            logger.error(f"Run directory not found: {run_dir}")
            return 1
    
    # Load metrics from all runs
    runs_data = {}
    for run_dir in run_dirs:
        metrics = load_run_metrics(run_dir)
        if metrics and "summary" in metrics:
            runs_data[run_dir.name] = metrics["summary"]
        else:
            logger.warning(f"No metrics found for {run_dir.name}")
    
    if not runs_data:
        logger.error("No valid metrics found for comparison")
        return 1
    
    # Get all metric keys
    all_keys = set()
    for summary in runs_data.values():
        all_keys.update(summary.keys())
    
    # Print comparison table
    print("\n" + "=" * 80)
    print("RUN COMPARISON")
    print("=" * 80)
    
    # Header
    run_names = list(runs_data.keys())
    header = f"{'Metric':<25}"
    for name in run_names:
        header += f" {name:<15}"
    if len(run_names) == 2:
        header += f" {'Diff':<15}"
    print(header)
    print("-" * 80)
    
    # Rows
    for key in sorted(all_keys):
        values = [runs_data[name].get(key) for name in run_names]
        
        # Format values
        formatted = []
        for v in values:
            if v is None:
                formatted.append("N/A")
            elif isinstance(v, float):
                if abs(v) < 1:  # Probably a ratio/percentage
                    formatted.append(f"{v:.4f}")
                else:
                    formatted.append(f"{v:.2f}")
            else:
                formatted.append(str(v))
        
        row = f"{key:<25}"
        for fv in formatted:
            row += f" {fv:<15}"
        
        # Add diff for exactly 2 runs
        if len(run_names) == 2 and all(v is not None for v in values):
            try:
                diff = float(values[1]) - float(values[0])
                if abs(diff) < 1:
                    row += f" {diff:+.4f}"
                else:
                    row += f" {diff:+.2f}"
            except (ValueError, TypeError):
                row += f" {'N/A':<15}"
        
        print(row)
    
    print("=" * 80)
    
    # If JSON output requested
    if args.json:
        output = {
            "runs": run_names,
            "metrics": {k: {name: runs_data[name].get(k) for name in run_names} for k in all_keys}
        }
        print("\nJSON Output:")
        print(json.dumps(output, indent=2, default=str))
    
    return 0


def cmd_ui(args: argparse.Namespace) -> int:
    """Launch UI command."""
    import subprocess
    
    app_path = Path(__file__).parent / "ui" / "app.py"
    cmd = ["streamlit", "run", str(app_path)]
    
    if args.port:
        cmd.extend(["--server.port", str(args.port)])
    
    logger.info(f"Starting UI on port {args.port or 8501}")
    subprocess.run(cmd)
    return 0


def cmd_init(args: argparse.Namespace) -> int:
    """Initialize database command."""
    import duckdb
    
    db_path = Path("db/quant.duckdb")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    schema_path = Path("db/schema.sql")
    
    if not schema_path.exists():
        logger.error(f"Schema file not found: {schema_path}")
        return 1
    
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()
    
    con = duckdb.connect(str(db_path))
    con.execute(schema_sql)
    con.close()
    
    logger.info(f"Initialized database: {db_path}")
    return 0


def main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="quantlab",
        description="QuantLab - Quantitative research system"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Backtest command
    backtest_parser = subparsers.add_parser(
        "backtest",
        help="Run backtest"
    )
    backtest_parser.add_argument(
        "--config",
        default="config/backtest.yaml",
        help="Backtest configuration file"
    )
    backtest_parser.set_defaults(func=cmd_backtest)
    
    # Compare command
    compare_parser = subparsers.add_parser(
        "compare",
        help="Compare multiple runs"
    )
    compare_parser.add_argument(
        "run_dirs",
        nargs="+",
        help="Run directories to compare"
    )
    compare_parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON"
    )
    compare_parser.set_defaults(func=cmd_compare)
    
    # UI command
    ui_parser = subparsers.add_parser(
        "ui",
        help="Launch Streamlit UI"
    )
    ui_parser.add_argument(
        "--port",
        type=int,
        help="Port to run UI on"
    )
    ui_parser.set_defaults(func=cmd_ui)
    
    # Init command
    init_parser = subparsers.add_parser(
        "init",
        help="Initialize database"
    )
    init_parser.set_defaults(func=cmd_init)
    
    args = parser.parse_args()
    
    setup_logging(args.verbose)
    
    if not args.command:
        parser.print_help()
        return 1
    
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
