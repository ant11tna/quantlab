"""DuckDB persistence for backtest results.

Persists run data to DuckDB for unified querying and comparison.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import duckdb
import pandas as pd
from loguru import logger


def persist_run(
    run_id: str,
    run_dir: Path,
    results: Dict[str, Any],
    config: Optional[Dict[str, Any]] = None,
    snapshot: Optional[Dict[str, Any]] = None,
    db_path: Path = Path("db/quant.duckdb"),
) -> None:
    """Persist backtest run to DuckDB.
    
    Args:
        run_id: Unique run identifier
        run_dir: Run output directory
        results: Backtest results dict (equity_curve, trades, weights, metrics)
        config: Run configuration dict
        snapshot: Data snapshot info
        db_path: Path to DuckDB database
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    
    try:
        # 1) runs 表
        created_at = pd.Timestamp.now()
        start_date = pd.to_datetime(results["equity_curve"]["ts"].min()).date()
        end_date = pd.to_datetime(results["equity_curve"]["ts"].max()).date()
        
        # Extract universe from weights columns (excluding ts)
        weight_cols = [c for c in results["weights"].columns if c != "ts"]
        universe = ",".join(weight_cols)
        
        # Get rebalance freq from config if available
        rebalance_freq = None
        threshold = None
        fee_model = None
        if config:
            if "rebalancing" in config:
                rebalance_freq = config["rebalancing"].get("frequency")
                threshold = config["rebalancing"].get("threshold")
            if "execution" in config:
                fee_model = config["execution"].get("slippage_model")
        
        run_row = pd.DataFrame([{
            "run_id": run_id,
            "created_at": created_at,
            "universe": universe,
            "start_date": start_date,
            "end_date": end_date,
            "rebalance_freq": rebalance_freq,
            "threshold": threshold,
            "fee_model": fee_model,
            "config_json": json.dumps(config or {}, ensure_ascii=False),
            "data_snapshot_id": (snapshot or {}).get("snapshot_id"),
        }])
        
        con.register("run_row", run_row)
        con.execute("INSERT OR REPLACE INTO runs SELECT * FROM run_row")
        
        # 2) portfolio_daily 表：equity_curve
        eq = results["equity_curve"].copy()
        eq["run_id"] = run_id
        
        # Calculate drawdown
        cummax = eq["nav"].cummax()
        eq["drawdown"] = (eq["nav"] - cummax) / cummax
        
        # Ensure all required columns exist
        if "cash" not in eq.columns:
            eq["cash"] = 0.0
        if "positions_value" not in eq.columns:
            eq["positions_value"] = eq["nav"] - eq["cash"]
        
        pdaily = eq[["run_id", "ts", "nav", "cash", "positions_value", "drawdown"]]
        con.register("pdaily", pdaily)
        con.execute("INSERT OR REPLACE INTO portfolio_daily SELECT * FROM pdaily")
        
        # 3) trades 表
        tr = results["trades"].copy()
        if not tr.empty:
            tr["run_id"] = run_id
            # Ensure required columns
            if "order_id" not in tr.columns:
                tr["order_id"] = ""
            if "side" not in tr.columns:
                tr["side"] = ""
            if "fee" not in tr.columns:
                tr["fee"] = 0.0
            if "slippage" not in tr.columns:
                tr["slippage"] = 0.0
            
            trade_cols = ["run_id", "ts", "order_id", "symbol", "side", "qty", "price", "fee", "slippage"]
            tr_filtered = tr[[c for c in trade_cols if c in tr.columns]]
            con.register("tr", tr_filtered)
            con.execute("INSERT OR REPLACE INTO trades SELECT * FROM tr")
        
        # 4) weights_target 表（实际权重轨迹，source=realized）
        w = results["weights"].copy().fillna(0.0)
        w["run_id"] = run_id
        melted = w.melt(id_vars=["run_id", "ts"], var_name="symbol", value_name="target_weight")
        melted["source"] = "realized"
        con.register("wt", melted)
        con.execute("INSERT OR REPLACE INTO weights_target SELECT * FROM wt")
        
        logger.info(f"Run {run_id} persisted to DuckDB")
        
    except Exception as e:
        logger.error(f"Failed to persist run to DuckDB: {e}")
        raise
    finally:
        con.close()


def load_run_summary(db_path: Path = Path("db/quant.duckdb")) -> pd.DataFrame:
    """Load run summary from DuckDB view.
    
    Args:
        db_path: Path to DuckDB database
        
    Returns:
        DataFrame with run summaries
    """
    con = duckdb.connect(str(db_path))
    try:
        df = con.execute("SELECT * FROM v_run_summary ORDER BY created_at DESC").fetchdf()
        return df
    finally:
        con.close()


def load_run_details(
    run_id: str,
    db_path: Path = Path("db/quant.duckdb")
) -> Dict[str, pd.DataFrame]:
    """Load all details for a run.
    
    Args:
        run_id: Run identifier
        db_path: Path to DuckDB database
        
    Returns:
        Dict of table_name -> DataFrame
    """
    con = duckdb.connect(str(db_path))
    try:
        return {
            "run": con.execute(
                "SELECT * FROM runs WHERE run_id = ?", [run_id]
            ).fetchdf(),
            "portfolio_daily": con.execute(
                "SELECT * FROM portfolio_daily WHERE run_id = ? ORDER BY ts", [run_id]
            ).fetchdf(),
            "trades": con.execute(
                "SELECT * FROM trades WHERE run_id = ? ORDER BY ts", [run_id]
            ).fetchdf(),
            "weights_target": con.execute(
                "SELECT * FROM weights_target WHERE run_id = ? ORDER BY ts", [run_id]
            ).fetchdf(),
        }
    finally:
        con.close()
