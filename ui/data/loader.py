"""Data loader for UI - Abstracts all file system operations.

Conventions:
- runs/<run_id>/config.yaml
- runs/<run_id>/results/metrics.json
- runs/<run_id>/results/equity_curve.parquet (or .csv)
- runs/<run_id>/results/positions.parquet
- runs/<run_id>/results/fills.parquet

Time formats:
- Internal DataFrames: datetime or date string
- Lightweight Charts output: epoch seconds (int)
- ECharts output: YYYY-MM-DD string
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yaml
from loguru import logger


# Base path for all runs
RUNS_DIR = Path("runs")


def _get_results_path(run_id: str, filename: str) -> Optional[Path]:
    """Get path to a result file, checking multiple locations."""
    # Try results subdirectory first
    path = RUNS_DIR / run_id / "results" / filename
    if path.exists():
        return path
    
    # Try root of run directory
    path = RUNS_DIR / run_id / filename
    if path.exists():
        return path
    
    return None


def _parse_run_id(run_id: str) -> Tuple[datetime, str, str]:
    """Parse run_id into components: started_at, name, hash.
    
    Expected format: YYYYMMDD_HHMMSS__<name>__<hash>
    Falls back to defaults if parsing fails.
    """
    try:
        parts = run_id.split("__")
        if len(parts) >= 1:
            dt_str = parts[0]
            started_at = datetime.strptime(dt_str, "%Y%m%d_%H%M%S")
        else:
            started_at = datetime.fromtimestamp(0)
        
        name = parts[1] if len(parts) > 1 else "unknown"
        hash_str = parts[2] if len(parts) > 2 else ""
        
        return started_at, name, hash_str
    except Exception:
        return datetime.fromtimestamp(0), run_id[:20], ""


def list_runs() -> pd.DataFrame:
    """Scan runs directory and return DataFrame of all runs.
    
    Returns columns:
        - run_id (str)
        - strategy (str)
        - created_at (datetime)
        - total_return (float|None)
        - max_drawdown (float|None)
        - sharpe (float|None)
        - (compat) started_at/name/hash/status/sharpe_ratio/turnover/fees
    """
    if not RUNS_DIR.exists():
        logger.warning(f"Runs directory not found: {RUNS_DIR}")
        return pd.DataFrame(columns=[
            "run_id", "strategy", "created_at", "total_return", "max_drawdown", "sharpe"
        ])
    
    runs = []
    
    for run_dir in sorted(RUNS_DIR.iterdir(), key=lambda x: x.name, reverse=True):
        if not run_dir.is_dir():
            continue
        
        run_id = run_dir.name
        started_at, name, hash_str = _parse_run_id(run_id)
        
        # Week1 contract: strictly read runs/<run_id>/results/metrics.json, missing => skip
        metrics_path = run_dir / "results" / "metrics.json"
        if not metrics_path.exists():
            continue

        try:
            with open(metrics_path, 'r', encoding='utf-8') as f:
                metrics = json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load metrics for {run_id}: {e}")
            continue

        config_path = run_dir / "config.yaml"
        strategy = name
        created_at = started_at
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f) or {}
                strategy = (
                    config.get("strategy")
                    or config.get("name")
                    or config.get("default", {}).get("strategy")
                    or strategy
                )
                config_created_at = config.get("created_at")
                if config_created_at:
                    created_at = pd.to_datetime(config_created_at)
            except Exception as e:
                logger.warning(f"Failed to parse config for {run_id}: {e}")
        
        # Extract metrics safely
        risk_metrics = metrics.get("risk", {})
        trading_metrics = metrics.get("trading", {})
        summary = metrics.get("summary", {})
        
        # Handle both nested and flat metric structures
        def get_metric(*keys, default=None):
            for key in keys:
                if key in summary:
                    return summary[key]
                if key in metrics:
                    return metrics[key]
            for key in keys:
                if key in risk_metrics:
                    return risk_metrics[key]
                if key in trading_metrics:
                    return trading_metrics[key]
            return default
        
        runs.append({
            "run_id": run_id,
            "strategy": strategy,
            "created_at": created_at,
            "started_at": started_at,
            "name": name,
            "hash": hash_str,
            "status": "complete",
            "total_return": get_metric("total_return"),
            "max_drawdown": get_metric("max_drawdown"),
            "sharpe": get_metric("sharpe", "sharpe_ratio"),
            "sharpe_ratio": get_metric("sharpe", "sharpe_ratio"),
            "turnover": get_metric("turnover"),
            "total_fees": get_metric("total_fees"),
            "total_impact_cost": get_metric("total_impact_cost"),
        })
    
    df = pd.DataFrame(runs)
    
    # Ensure all columns exist even if empty
    required_cols = [
        "run_id", "strategy", "created_at", "started_at", "name", "hash", "status",
        "total_return", "max_drawdown", "sharpe", "sharpe_ratio", "turnover",
        "total_fees", "total_impact_cost"
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None
    
    return df


def load_run(run_id: str) -> Dict:
    """Load all data for a specific run.
    
    Returns dict with:
        - config_text: raw YAML text
        - config_dict: parsed YAML
        - metrics_dict: parsed metrics.json
        - paths_dict: mapping of file types to paths
        - status: 'complete', 'incomplete', 'error'
    """
    result = {
        "config_text": None,
        "config_dict": None,
        "metrics_dict": None,
        "paths_dict": {},
        "status": "incomplete"
    }
    
    # Load config.yaml
    config_path = RUNS_DIR / run_id / "config.yaml"
    if config_path.exists():
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                result["config_text"] = f.read()
            with open(config_path, 'r', encoding='utf-8') as f:
                result["config_dict"] = yaml.safe_load(f)
        except Exception as e:
            logger.warning(f"Failed to load config for {run_id}: {e}")
    
    # Load metrics.json
    metrics_path = _get_results_path(run_id, "metrics.json")
    if metrics_path:
        try:
            with open(metrics_path, 'r') as f:
                result["metrics_dict"] = json.load(f)
            result["status"] = "complete"
        except Exception as e:
            logger.warning(f"Failed to load metrics for {run_id}: {e}")
            result["status"] = "error"
    
    # Build paths dict
    for file_type in ["equity_curve", "positions", "fills", "weights", "trades"]:
        for ext in [".parquet", ".csv"]:
            path = _get_results_path(run_id, f"{file_type}{ext}")
            if path:
                result["paths_dict"][file_type] = path
                break
    
    return result


def load_equity_curve(run_id: str) -> pd.DataFrame:
    """Load equity curve for a run.
    
    Required output columns:
        - ts: datetime
        - nav: float (portfolio NAV)
        - cash: float (optional)
        - exposure: float (optional, positions value)
    
    Returns empty DataFrame with required columns if file not found.
    """
    path = _get_results_path(run_id, "equity_curve.parquet")
    if path is None:
        path = _get_results_path(run_id, "equity_curve.csv")
    
    if path is None:
        logger.warning(f"No equity curve found for {run_id}")
        return pd.DataFrame(columns=["ts", "nav", "cash", "exposure"])
    
    try:
        if path.suffix == ".parquet":
            df = pd.read_parquet(path)
        else:
            df = pd.read_csv(path)
        
        # Normalize column names
        df = df.rename(columns={
            "timestamp": "ts",
            "date": "ts",
            "portfolio_value": "nav",
            "value": "nav",
        })
        
        # Ensure ts is datetime
        if "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"])
        
        # Ensure required columns exist
        for col in ["nav", "cash", "exposure"]:
            if col not in df.columns:
                df[col] = None
        
        return df
    
    except Exception as e:
        logger.error(f"Failed to load equity curve for {run_id}: {e}")
        return pd.DataFrame(columns=["ts", "nav", "cash", "exposure"])


def load_positions(run_id: str) -> pd.DataFrame:
    """Load positions history for a run.
    
    Required output columns:
        - ts: datetime
        - symbol: str
        - weight: float (position weight in portfolio)
        - value: float (optional, position value)
        - qty: float (optional, quantity)
    
    If weight is not in file, will compute as value / total_value if possible.
    Returns empty DataFrame if file not found.
    """
    path = _get_results_path(run_id, "positions.parquet")
    if path is None:
        path = _get_results_path(run_id, "positions.csv")
    
    if path is None:
        # Try to reconstruct from weights.parquet
        path = _get_results_path(run_id, "weights.parquet")
        if path is None:
            path = _get_results_path(run_id, "weights.csv")
    
    if path is None:
        logger.warning(f"No positions/weights found for {run_id}")
        return pd.DataFrame(columns=["ts", "symbol", "weight", "value", "qty"])
    
    try:
        if path.suffix == ".parquet":
            df = pd.read_parquet(path)
        else:
            df = pd.read_csv(path)
        
        # Handle weights file format (wide format with symbol columns)
        if "ts" in df.columns and any(col not in ["ts", "symbol", "weight", "value", "qty"] for col in df.columns):
            # Likely wide format: ts, AAPL, MSFT, ...
            id_vars = ["ts"] if "ts" in df.columns else []
            if id_vars:
                df = df.melt(id_vars=id_vars, var_name="symbol", value_name="weight")
                df = df[df["symbol"] != "ts"]
        
        # Normalize column names
        df = df.rename(columns={
            "timestamp": "ts",
            "date": "ts",
            "ticker": "symbol",
        })
        
        # Ensure ts is datetime
        if "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"])
        
        # Ensure required columns exist
        for col in ["weight", "value", "qty"]:
            if col not in df.columns:
                df[col] = None
        
        return df
    
    except Exception as e:
        logger.error(f"Failed to load positions for {run_id}: {e}")
        return pd.DataFrame(columns=["ts", "symbol", "weight", "value", "qty"])


def load_fills(run_id: str, symbol: Optional[str] = None) -> pd.DataFrame:
    """Load fill/trade history for a run.
    
    Required output columns:
        - ts: datetime
        - symbol: str
        - side: str ('BUY', 'SELL', or 'buy', 'sell')
        - qty: float
        - price: float
        - fee: float (optional)
        - impact_bps: float (optional)
    
    Args:
        run_id: Run identifier
        symbol: Optional symbol filter
    
    Returns empty DataFrame if file not found.
    """
    # Try fills.parquet first, then trades.parquet
    for file_name in ["fills.parquet", "trades.parquet", "fills.csv", "trades.csv"]:
        path = _get_results_path(run_id, file_name)
        if path:
            break
    else:
        logger.warning(f"No fills/trades found for {run_id}")
        return pd.DataFrame(columns=["ts", "symbol", "side", "qty", "price", "fee", "impact_bps"])
    
    try:
        if path.suffix == ".parquet":
            df = pd.read_parquet(path)
        else:
            df = pd.read_csv(path)
        
        # Normalize column names
        df = df.rename(columns={
            "timestamp": "ts",
            "date": "ts",
            "ticker": "symbol",
            "quantity": "qty",
            "fill_price": "price",
            "cost": "fee",
        })
        
        # Normalize side values
        if "side" in df.columns:
            df["side"] = df["side"].str.upper()
        
        # Filter by symbol if specified
        if symbol is not None and "symbol" in df.columns:
            df = df[df["symbol"] == symbol]
        
        # Ensure ts is datetime
        if "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"])
        
        # Ensure required columns exist
        for col in ["fee", "impact_bps"]:
            if col not in df.columns:
                df[col] = None
        
        return df
    
    except Exception as e:
        logger.error(f"Failed to load fills for {run_id}: {e}")
        return pd.DataFrame(columns=["ts", "symbol", "side", "qty", "price", "fee", "impact_bps"])


# Data transformation helpers for charts

def to_echarts_date(ts: pd.Timestamp) -> str:
    """Convert timestamp to ECharts date string format (YYYY-MM-DD)."""
    return ts.strftime("%Y-%m-%d")


def to_lightweight_time(ts: pd.Timestamp) -> int:
    """Convert timestamp to Lightweight Charts epoch seconds format."""
    return int(ts.timestamp())


def equity_curve_to_echarts(df: pd.DataFrame, last_n: int = 60) -> Dict:
    """Convert equity curve DataFrame to ECharts format.
    
    Returns:
        {
            "dates": ["2026-01-01", ...],  # YYYY-MM-DD strings
            "values": [1000000, ...]        # NAV values
        }
    """
    if df.empty:
        return {"dates": [], "values": []}
    
    df = df.tail(last_n) if last_n and len(df) > last_n else df
    
    return {
        "dates": [to_echarts_date(ts) for ts in df["ts"]],
        "values": df["nav"].tolist()
    }


def cost_breakdown_to_echarts(metrics: Dict) -> Dict:
    """Convert metrics to cost breakdown bar chart for ECharts.
    
    Returns:
        {
            "categories": ["Fees", "Impact", "Slippage"],
            "values": [100, 50, 30]
        }
    """
    trading = metrics.get("trading", {})
    summary = metrics.get("summary", metrics)  # fallback to root
    
    categories = []
    values = []
    
    # Try various cost metric names
    cost_fields = [
        ("Fees", ["total_fees", "fees", "commission"]),
        ("Impact", ["total_impact_cost", "impact_cost", "market_impact"]),
        ("Slippage", ["total_slippage", "slippage"]),
    ]
    
    for label, fields in cost_fields:
        for field in fields:
            if field in trading:
                value = trading[field]
                if value is not None and value > 0:
                    categories.append(label)
                    values.append(float(value))
                break
            elif field in summary:
                value = summary[field]
                if value is not None and value > 0:
                    categories.append(label)
                    values.append(float(value))
                break
    
    return {
        "categories": categories,
        "values": values
    }


# ============================================================================
# Run Detail Page - Additional Loaders and Transformers
# ============================================================================

# Data source base paths
DATA_DIR = Path("data/curated/bars")

# Curated index path
CURATED_INDEX_FILE = DATA_DIR / "curated_index.json"


def _load_curated_index() -> Dict:
    """Load curated_index.json if exists."""
    if CURATED_INDEX_FILE.exists():
        try:
            with open(CURATED_INDEX_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load curated index: {e}")
    return {}


def _get_curated_parquet_path(symbol: str) -> Optional[Path]:
    """Get parquet path for symbol from curated index or convention.
    
    Search order:
        1. curated_index.json
        2. data/curated/bars/{etf,index,stock}/{sanitized_symbol}.parquet
    """
    # Try index first
    index = _load_curated_index()
    symbol_info = index.get("symbols", {}).get(symbol)
    
    if symbol_info and "file" in symbol_info:
        file_path = symbol_info["file"]
        # Handle relative paths
        if file_path.startswith("bars/"):
            path = Path("data/curated") / file_path
        else:
            path = DATA_DIR / file_path
        
        if path.exists():
            return path
    
    # Fallback to convention
    safe_name = symbol.replace(":", "_")
    for subdir in ["etf", "index", "stock"]:
        path = DATA_DIR / subdir / f"{safe_name}.parquet"
        if path.exists():
            return path
    
    return None


def sanitize_symbol(symbol: str) -> str:
    """Sanitize symbol for safe filename usage.
    
    Replaces any character not in [A-Za-z0-9_-] with _
    to ensure cross-platform filename safety.
    
    Examples:
        >>> sanitize_symbol("ETF:510300")
        'ETF_510300'
        >>> sanitize_symbol("sh000300")
        'sh000300'
        >>> sanitize_symbol("A/B:C.d")
        'A_B_C_d'
    """
    import re
    return re.sub(r'[^A-Za-z0-9_-]', '_', str(symbol))


def load_symbol_bars(symbol: str, run_id: Optional[str] = None, source: str = "etf") -> pd.DataFrame:
    """Load OHLCV bars for a symbol.
    
    Search order:
        1. runs/<run_id>/bars/<sanitized_symbol>.parquet (if run_id provided)
        2. data/curated/bars/{etf,index,stock}/{sanitized_symbol}.parquet (curated_v1 - DEFAULT)
        3. data/curated/bars/<source>/<symbol>.csv (legacy raw - fallback)
    
    Args:
        symbol: Trading symbol (e.g., "ETF:510300", "AAPL")
        run_id: Optional run_id to load from run-specific bars directory
        source: "etf" or "index" or "stock" (fallback only)
    
    Returns DataFrame with columns (curated_v1 schema):
        - ts: datetime
        - symbol: str
        - open, high, low, close: float
        - volume: int/float
        - amount: float (optional)
        # Regime fields (curated_v1)
        - prev_close: float
        - is_limit_up, is_limit_down: bool
        - is_suspended: bool
        - can_buy, can_sell: bool
        - adj_factor: float
    """
    sanitized = sanitize_symbol(symbol)
    
    # 1. Try run-specific bars first (if run_id provided)
    if run_id:
        bars_dir = RUNS_DIR / run_id / "bars"
        index_file = bars_dir / "bars_index.json"
        
        # Check if there's a mapping file
        if index_file.exists():
            try:
                with open(index_file, 'r') as f:
                    index = json.load(f)
                # Find filename by symbol
                filename = index.get("symbol_to_file", {}).get(symbol)
                if filename:
                    path = bars_dir / filename
                    if path.exists():
                        df = pd.read_parquet(path)
                        # Ensure required columns
                        df = _normalize_bars_schema(df)
                        logger.debug(f"Loaded bars for {symbol} from {path}")
                        return df
            except Exception as e:
                logger.warning(f"Failed to read bars_index.json: {e}")
        
        # Fallback: try sanitized name directly
        path = bars_dir / f"{sanitized}.parquet"
        if path.exists():
            try:
                df = pd.read_parquet(path)
                df = _normalize_bars_schema(df)
                logger.debug(f"Loaded bars for {symbol} from {path}")
                return df
            except Exception as e:
                logger.warning(f"Failed to load bars from {path}: {e}")
    
    # 2. Try curated parquet (curated_v1 - DEFAULT data source)
    curated_path = _get_curated_parquet_path(symbol)
    if curated_path and curated_path.exists():
        try:
            df = pd.read_parquet(curated_path)
            # Ensure required columns
            df = _normalize_bars_schema(df)
            logger.debug(f"Loaded curated bars for {symbol} from {curated_path}")
            return df
        except Exception as e:
            logger.warning(f"Failed to load curated bars from {curated_path}: {e}")
    
    # 3. Fallback to raw CSV (data/raw/bars) if curated not built yet
    RAW_DATA_DIR = Path("data/raw/bars")
    for src in [source, "etf", "index", "stock"]:
        path = RAW_DATA_DIR / src / f"{symbol}.csv"
        if path.exists():
            try:
                df = pd.read_csv(path)
                df = _normalize_bars_schema(df)
                logger.debug(f"Loaded raw bars for {symbol} from {path}")
                return df
            except Exception as e:
                logger.error(f"Failed to load bars from {path}: {e}")
                continue
    
    logger.warning(f"No bars found for {symbol}")
    return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])


def _normalize_bars_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize bars DataFrame to standard schema.
    
    Ensures columns: ts, open, high, low, close, volume
    Preserves curated_v1 regime fields if present.
    """
    # Rename common variations
    df = df.rename(columns={
        "timestamp": "ts",
        "date": "ts",
        "datetime": "ts",
        "open_": "open",
        "Open": "open",
        "high": "high",
        "High": "high",
        "low": "low",
        "Low": "low",
        "close": "close",
        "Close": "close",
        "volume": "volume",
        "Volume": "volume",
        "vol": "volume",
        "amount": "amount",
        "Amount": "amount",
    })
    
    # Ensure ts is datetime
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"])
    
    # Ensure required columns exist
    required = ["open", "high", "low", "close"]
    for col in required:
        if col not in df.columns:
            logger.warning(f"Missing required column: {col}")
            df[col] = 0.0
    
    # Ensure volume exists (default 0)
    if "volume" not in df.columns:
        df["volume"] = 0
    
    # Optional: amount
    if "amount" not in df.columns:
        df["amount"] = None
    
    # Curated_v1 regime fields - preserve if present
    regime_cols = [
        "symbol", "prev_close", "is_limit_up", "is_limit_down",
        "is_suspended", "can_buy", "can_sell", "adj_factor"
    ]
    
    # Select columns to return: base + existing regime cols
    base_cols = ["ts", "open", "high", "low", "close", "volume", "amount"]
    existing_regime = [c for c in regime_cols if c in df.columns]
    
    return df[base_cols + existing_regime]


def compute_drawdown(nav: pd.Series) -> pd.Series:
    """Compute drawdown series from NAV.
    
    Formula:
        peak = cummax(nav_norm)
        dd = nav_norm / peak - 1
    
    Returns:
        Drawdown series (negative values, 0 = no drawdown)
    """
    nav_norm = nav / nav.iloc[0] if nav.iloc[0] != 0 else nav
    peak = nav_norm.cummax()
    dd = nav_norm / peak - 1
    return dd


def compute_turnover_from_positions(pos_df: pd.DataFrame) -> pd.DataFrame:
    """Compute turnover from positions time series.
    
    Input: pos_df with columns [ts, symbol, weight]
    Output: DataFrame with columns [ts, turnover]
    
    Formula per timestamp:
        turnover = 0.5 * sum(|w_t - w_{t-1}|)
    """
    if pos_df.empty or "weight" not in pos_df.columns:
        return pd.DataFrame(columns=["ts", "turnover"])
    
    # Pivot to wide format: index=ts, columns=symbol, values=weight
    # Fill NaN with 0 (assuming no position = 0 weight)
    wide = pos_df.pivot(index="ts", columns="symbol", values="weight").fillna(0)
    
    # Compute weight changes
    diff = wide.diff().abs().sum(axis=1)
    
    # Turnover = 0.5 * sum of absolute changes
    turnover = diff * 0.5
    
    return pd.DataFrame({
        "ts": wide.index,
        "turnover": turnover.values
    })


def aggregate_fills_by_ts(fills_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate fills by timestamp for cost analysis.
    
    Input: fills_df with columns [ts, fee, impact_cost/slippage]
    Output: DataFrame with columns [ts, fees, impact, total_cost]
    """
    if fills_df.empty:
        return pd.DataFrame(columns=["ts", "fees", "impact", "total_cost"])
    
    # Determine impact column name
    impact_col = None
    for col in ["impact_cost", "slippage", "impact"]:
        if col in fills_df.columns:
            impact_col = col
            break
    
    agg = {"fee": "sum"}
    if impact_col:
        agg[impact_col] = "sum"
    
    grouped = fills_df.groupby("ts").agg(agg).reset_index()
    
    # Rename columns
    grouped = grouped.rename(columns={"fee": "fees"})
    if impact_col:
        grouped = grouped.rename(columns={impact_col: "impact"})
    else:
        grouped["impact"] = 0.0
    
    grouped["total_cost"] = grouped["fees"] + grouped["impact"]
    
    return grouped[["ts", "fees", "impact", "total_cost"]]


def positions_to_allocation_series(
    pos_df: pd.DataFrame, 
    top_n: int = 8
) -> Dict[str, any]:
    """Convert positions to allocation time series with TopN + OTHER grouping.
    
    Steps:
        1. Stat each symbol's avg/max weight
        2. Take TopN symbols
        3. Group rest as "OTHER"
        4. Pivot to wide format
    
    Returns:
        {
            "dates": ["2026-01-01", ...],
            "series": [
                {"name": "AAPL", "data": [0.5, 0.5, ...]},
                {"name": "OTHER", "data": [0.1, 0.1, ...]},
            ],
            "symbols": ["AAPL", "MSFT", ...]  # TopN symbol list
        }
    """
    if pos_df.empty or "weight" not in pos_df.columns:
        return {"dates": [], "series": [], "symbols": []}
    
    # Drop NaN weights
    pos_df = pos_df.dropna(subset=["weight"]).copy()
    
    if pos_df.empty:
        return {"dates": [], "series": [], "symbols": []}
    
    # 1. Stat: use max weight to rank symbols
    symbol_stats = pos_df.groupby("symbol")["weight"].max().sort_values(ascending=False)
    
    # 2. Take TopN
    top_symbols = symbol_stats.head(top_n).index.tolist()
    
    # 3. Add "OTHER" group
    pos_df["group"] = pos_df["symbol"].apply(
        lambda x: x if x in top_symbols else "OTHER"
    )
    
    # Aggregate weights by group per timestamp
    grouped = pos_df.groupby(["ts", "group"])["weight"].sum().reset_index()
    
    # 4. Pivot to wide
    wide = grouped.pivot(index="ts", columns="group", values="weight").fillna(0)
    
    # Ensure OTHER exists
    if "OTHER" not in wide.columns:
        wide["OTHER"] = 0.0
    
    # Sort columns: TopN first, OTHER last
    col_order = [s for s in top_symbols if s in wide.columns] + ["OTHER"]
    wide = wide[[c for c in col_order if c in wide.columns]]
    
    # Convert to output format
    dates = [to_echarts_date(ts) for ts in wide.index]
    
    series = []
    for symbol in wide.columns:
        series.append({
            "name": symbol,
            "data": wide[symbol].tolist()
        })
    
    return {
        "dates": dates,
        "series": series,
        "symbols": top_symbols
    }


def fills_to_lightweight_markers(fills_df: pd.DataFrame) -> List[Dict]:
    """Convert fills DataFrame to Lightweight Charts markers format.
    
    Returns:
        [
            {"time": 1704067200, "position": "belowBar", "shape": "arrowUp", "text": "B 100"},
            {"time": 1704326400, "position": "aboveBar", "shape": "arrowDown", "text": "S 50"},
        ]
    """
    if fills_df.empty:
        return []
    
    markers = []
    
    for _, row in fills_df.iterrows():
        ts = pd.Timestamp(row["ts"])
        time = int(ts.timestamp())
        
        side = str(row.get("side", "")).upper()
        qty = float(row.get("qty", 0))
        
        # Format qty
        if qty >= 1000:
            qty_str = f"{qty/1000:.1f}k"
        elif qty >= 1:
            qty_str = f"{qty:.0f}"
        else:
            qty_str = f"{qty:.2f}"
        
        if side == "BUY":
            marker = {
                "time": time,
                "position": "belowBar",
                "shape": "arrowUp",
                "color": "#26a69a",  # green
                "text": f"B {qty_str}",
                "size": 2,
            }
        elif side == "SELL":
            marker = {
                "time": time,
                "position": "aboveBar",
                "shape": "arrowDown",
                "color": "#ef5350",  # red
                "text": f"S {qty_str}",
                "size": 2,
            }
        else:
            continue
        
        markers.append(marker)
    
    return markers


def bars_to_lightweight_ohlcv(bars_df: pd.DataFrame) -> Dict:
    """Convert bars DataFrame to Lightweight Charts format.
    
    Returns:
        {
            "ohlc": [
                {"time": 1704067200, "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05},
                ...
            ],
            "volume": [
                {"time": 1704067200, "value": 123456},
                ...
            ]
        }
    """
    if bars_df.empty:
        return {"ohlc": [], "volume": []}
    
    ohlc = []
    volume = []
    
    for _, row in bars_df.iterrows():
        ts = pd.Timestamp(row["ts"])
        time = int(ts.timestamp())
        
        ohlc.append({
            "time": time,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
        })
        
        vol_val = float(row.get("volume", 0))
        if vol_val > 0:
            volume.append({
                "time": time,
                "value": vol_val,
            })
    
    return {"ohlc": ohlc, "volume": volume}


def get_default_symbol(run_id: str) -> str:
    """Get default symbol for a run based on fills and positions.
    
    Priority:
        1. Most traded symbol (by fill count)
        2. Highest average weight symbol
        3. First available symbol
    """
    fills = load_fills(run_id)
    if not fills.empty and "symbol" in fills.columns:
        most_traded = fills["symbol"].value_counts().index[0]
        return most_traded
    
    positions = load_positions(run_id)
    if not positions.empty and "symbol" in positions.columns:
        avg_weights = positions.groupby("symbol")["weight"].mean().sort_values(ascending=False)
        return avg_weights.index[0]
    
    return ""


# ECharts option builders for Run Detail page

def equity_and_drawdown_to_echarts(equity_df: pd.DataFrame) -> Dict:
    """Generate ECharts option for Equity + Drawdown combo chart.
    
    Returns option dict with two series:
        - nav_norm (line)
        - drawdown (area below zero)
    """
    if equity_df.empty or "nav" not in equity_df.columns:
        return {"dates": [], "nav_norm": [], "drawdown": []}
    
    dates = [to_echarts_date(ts) for ts in equity_df["ts"]]
    nav = equity_df["nav"]
    nav_norm = (nav / nav.iloc[0]).tolist()
    dd = compute_drawdown(nav).tolist()
    
    return {
        "dates": dates,
        "nav_norm": nav_norm,
        "drawdown": dd,
    }


def turnover_and_cost_to_echarts(
    turnover_df: pd.DataFrame,
    cost_df: pd.DataFrame
) -> Dict:
    """Generate data for Turnover & Cost dual-axis chart.
    
    Returns:
        {
            "dates": [...],
            "turnover": [...],
            "fees": [...],
            "impact": [...],
            "total_cost": [...],
        }
    """
    if turnover_df.empty:
        return {"dates": [], "turnover": [], "fees": [], "impact": [], "total_cost": []}
    
    # Merge on ts
    merged = turnover_df.merge(cost_df, on="ts", how="left").fillna(0)
    
    dates = [to_echarts_date(ts) for ts in merged["ts"]]
    
    return {
        "dates": dates,
        "turnover": merged["turnover"].tolist(),
        "fees": merged["fees"].tolist(),
        "impact": merged["impact"].tolist(),
        "total_cost": merged["total_cost"].tolist(),
    }
