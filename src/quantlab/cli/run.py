"""Standard research run entrypoint.

Usage:
    python -m quantlab.cli.run --config configs/xxx.yaml
"""

from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml
from loguru import logger

from quantlab.backtest.engine import BacktestEngine
from quantlab.data.ingest import DataIngestor
from quantlab.data.sources.local_csv import MockDataSource
from quantlab.research.strategies.base import EqualWeightStrategy


def _load_config(config_path: Path) -> Dict[str, Any]:
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _to_serializable(value: Any) -> Any:
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _to_serializable(value.to_dict())
    if is_dataclass(value):
        return _to_serializable(asdict(value))
    if isinstance(value, dict):
        return {k: _to_serializable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_serializable(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _generate_run_id(prefix: str = "run") -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{ts}__{prefix}__{uuid.uuid4().hex[:8]}"


def run_from_config(config: Dict[str, Any], config_path: Path) -> Path:
    data_cfg = config.get("data", {})
    strategy_cfg = config.get("strategy", {})
    backtest_cfg = config.get("backtest", {})
    execution_cfg = config.get("execution", config.get("default", {}).get("execution", {}))

    symbols = data_cfg.get("symbols", strategy_cfg.get("symbols", ["SPY", "TLT", "GLD"]))
    start_date = datetime.fromisoformat(data_cfg.get("start_date", "2020-01-01"))
    end_date = datetime.fromisoformat(data_cfg.get("end_date", "2024-12-31"))
    data_dir = data_cfg.get("data_dir", "data/curated")

    strategy = EqualWeightStrategy(symbols=symbols)
    engine = BacktestEngine(
        strategy=strategy,
        initial_cash=float(backtest_cfg.get("initial_cash", 1_000_000.0)),
        fee_model=backtest_cfg.get("fee_model", "us_etfs"),
        exec_config=execution_cfg,
        calendar=backtest_cfg.get("calendar", "XNYS"),
    )

    data_source = MockDataSource(symbols=symbols)
    ingestor = DataIngestor(data_source, data_dir)
    bars = ingestor.ingest(symbols=symbols, start=start_date, end=end_date)

    results = engine.run(
        data=bars,
        rebalance_freq=backtest_cfg.get("rebalance_freq", "M"),
        rebalance_threshold=backtest_cfg.get("rebalance_threshold"),
        progress=bool(backtest_cfg.get("progress", True)),
    )

    run_id = _generate_run_id(strategy.name)
    run_dir = Path("runs") / run_id
    run_dir.mkdir(parents=True, exist_ok=False)

    results["equity_curve"].to_parquet(run_dir / "equity_curve.parquet", index=False)
    results["weights"].to_parquet(run_dir / "weights.parquet", index=False)
    results["trades"].to_parquet(run_dir / "trades.parquet", index=False)

    metrics_payload = _to_serializable(results.get("metrics", {}))
    with (run_dir / "metrics.json").open("w", encoding="utf-8") as f:
        json.dump(metrics_payload, f, indent=2, ensure_ascii=False)

    with (run_dir / "config.yaml").open("w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, sort_keys=False, allow_unicode=True)

    logger.info(f"Run finished: {run_id}")
    logger.info(f"Artifacts saved under: {run_dir}")
    return run_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="QuantLab standard research runner")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)
    if not config_path.exists():
        logger.error(f"Config not found: {config_path}")
        return 1

    config = _load_config(config_path)
    run_dir = run_from_config(config, config_path)
    print(f"run_dir={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
