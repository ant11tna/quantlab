"""Command line interface for QuantLab.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from copy import deepcopy
from datetime import datetime
from itertools import product
from pathlib import Path
from typing import Any, Dict, Iterable

import yaml
from loguru import logger


def _iter_grid_combinations(grid_config: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """Yield cartesian product of config.grid."""
    if not isinstance(grid_config, dict) or not grid_config:
        yield {}
        return

    keys = []
    values = []
    for key, options in grid_config.items():
        if isinstance(options, list) and options:
            keys.append(key)
            values.append(options)

    if not keys:
        yield {}
        return

    for combo in product(*values):
        yield dict(zip(keys, combo))


def _slug(value: Any) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", str(value).strip()).strip("_").lower()


def _summarize_grid_params(grid_params: Dict[str, Any]) -> str:
    if not grid_params:
        return ""

    tokens = []
    for key, value in sorted(grid_params.items(), key=lambda item: item[0]):
        if key == "momentum_window":
            tokens.append(f"w{value}")
        elif key == "rebalance":
            tokens.append(_slug(value))
        else:
            tokens.append(f"{_slug(key)}{_slug(value)}")
    return "_".join(tokens)


def _map_rebalance(value: Any) -> str:
    mapping = {
        "monthly": "M",
        "weekly": "W",
        "quarterly": "Q",
        "daily": "D",
    }
    return mapping.get(str(value).strip().lower(), "M")


def _resolve_rebalance_config(config: Dict[str, Any], grid_params: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve rebalance rule config with backward compatibility.

    Preferred:
      rebalance:
        type: periodic|threshold|hybrid
        frequency: monthly|quarterly|yearly
        threshold: 0.05
    """
    rebalance_cfg = config.get("rebalance") if isinstance(config.get("rebalance"), dict) else {}

    # Backward compatible fallbacks
    if not rebalance_cfg and isinstance(config.get("default"), dict):
        default_cfg = config.get("default", {})
        if isinstance(default_cfg.get("rebalance"), dict):
            rebalance_cfg = default_cfg.get("rebalance", {})

    if not rebalance_cfg and isinstance(config.get("default"), dict):
        rebalancing = config.get("default", {}).get("rebalancing", {})
        if isinstance(rebalancing, dict):
            mode = str(rebalancing.get("mode", "periodic")).lower()
            rebalance_cfg = {
                "type": "hybrid" if mode == "both" else mode,
                "frequency": {"M": "monthly", "Q": "quarterly", "Y": "yearly"}.get(
                    str(rebalancing.get("frequency", "M")).upper(), "monthly"
                ),
                "threshold": rebalancing.get("threshold", 0.05),
            }

    # Grid parameter can still override periodic frequency for experiments
    if "rebalance" in grid_params:
        rebalance_cfg = {**rebalance_cfg, "frequency": str(grid_params["rebalance"]).lower()}

    if not rebalance_cfg:
        rebalance_cfg = {"type": "periodic", "frequency": "monthly", "threshold": 0.05}

    return rebalance_cfg


def _run_single_backtest(config: Dict[str, Any], config_path: Path, config_text: str, grid_params: Dict[str, Any]) -> Path:
    """Run one backtest and return run directory."""
    from quantlab.backtest.engine import BacktestEngine
    from quantlab.core.runlog import (
        create_run_dir,
        write_run_metadata,
        finalize_run,
    )
    from quantlab.data.sources.local_csv import MockDataSource
    from quantlab.data.ingest import DataIngestor
    from quantlab.rebalance import build_rebalance_rule
    from quantlab.research.strategies.base import EqualWeightStrategy

    strategy_name = "momentum" if "momentum_window" in grid_params else "backtest"
    run_name = strategy_name
    grid_summary = _summarize_grid_params(grid_params)
    if grid_summary:
        run_name = f"{strategy_name}_{grid_summary}"

    run_dir = create_run_dir(
        base="runs",
        name=run_name,
        config_text=config_text,
    )

    write_run_metadata(
        run_dir=run_dir,
        config_path=config_path,
        config_text=config_text,
        data_manifest=None,
        git_rev=None,
        env_info=None,
    )

    symbols = ["SPY", "TLT", "GLD"]
    data_source = MockDataSource(symbols=symbols)
    ingestor = DataIngestor(data_source, "data/curated")

    start = datetime(2020, 1, 1)
    end = datetime(2024, 12, 31)
    data = ingestor.ingest(symbols, start, end)

    strategy = EqualWeightStrategy(symbols=symbols)
    exec_config = config.get("default", {}).get("execution", {}) if config else {}
    rebalance_cfg = _resolve_rebalance_config(config, grid_params)
    rebalance_rule = build_rebalance_rule(rebalance_cfg)

    engine = BacktestEngine(
        strategy=strategy,
        initial_cash=1_000_000.0,
        fee_model="us_etfs",
        exec_config=exec_config,
    )

    results = engine.run(
        data=data,
        rebalance_rule=rebalance_rule,
        progress=True,
    )

    artifacts = {}
    equity_path = run_dir / "equity_curve.parquet"
    results["equity_curve"].to_parquet(equity_path, index=False)
    artifacts["equity_curve"] = equity_path

    weights_path = run_dir / "weights.parquet"
    results["weights"].to_parquet(weights_path, index=False)
    artifacts["weights"] = weights_path

    if not results["trades"].empty:
        fills_path = run_dir / "fills.parquet"
        results["trades"].to_parquet(fills_path, index=False)
        artifacts["fills"] = fills_path

    positions_df = results.get("positions")
    if positions_df is not None and not positions_df.empty:
        positions_path = run_dir / "positions.parquet"
        positions_df.to_parquet(positions_path, index=False)
        artifacts["positions"] = positions_path

    runtime_config = {
        "symbols": symbols,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "initial_cash": 1_000_000.0,
        "rebalance": rebalance_cfg,
        "fee": exec_config.get("fee_bps") if isinstance(exec_config, dict) else None,
        "slippage": exec_config.get("slippage_bps") if isinstance(exec_config, dict) else None,
        "strategy": strategy.__class__.__name__,
        "strategy_params": {
            "symbols": symbols,
            **grid_params,
        },
    }

    finalize_run(
        run_dir=run_dir,
        artifacts=artifacts,
        metrics=results["metrics"],
        runtime_config=runtime_config,
    )

    try:
        from quantlab.db.persist import persist_run

        persist_run(
            run_id=run_dir.name,
            run_dir=run_dir,
            results=results,
            config=config,
            snapshot=None,
        )
        logger.info("Run persisted to DuckDB")
    except Exception as e:
        logger.warning(f"Failed to persist to DuckDB: {e}")

    return run_dir


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = "DEBUG" if verbose else "INFO"
    logger.remove()
    logger.add(sys.stderr, level=level, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")


def cmd_backtest(args: argparse.Namespace) -> int:
    """Run backtest command with full run tracking."""
    logger.info(f"Running backtest with config: {args.config}")
    
    # Load config (keep original text for hash)
    config_path = Path(args.config)
    with open(config_path, 'r', encoding='utf-8') as f:
        config_text = f.read()
    config = yaml.safe_load(config_text) or {}

    run_params = [dict()]
    if args.grid and isinstance(config.get("grid"), dict) and config.get("grid"):
        run_params = list(_iter_grid_combinations(config.get("grid", {})))

    for grid_params in run_params:
        run_config = deepcopy(config)
        if grid_params:
            run_config.setdefault("grid_selection", {}).update(grid_params)

        run_dir = _run_single_backtest(
            config=run_config,
            config_path=config_path,
            config_text=config_text,
            grid_params=grid_params,
        )
        print(f"Run ID: {run_dir.name}")
        print(f"Results saved to: {run_dir}")
    
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
    backtest_parser.add_argument(
        "--profile",
        default=None,
        help="Execution profile name (currently reserved, optional)"
    )
    backtest_parser.add_argument(
        "--grid",
        action="store_true",
        help="Expand config.grid and run all parameter combinations",
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
