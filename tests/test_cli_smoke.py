from __future__ import annotations

import json
import subprocess
from pathlib import Path


def test_cli_backtest_smoke() -> None:
    runs_dir = Path("runs")
    runs_dir.mkdir(exist_ok=True)

    before = {p.name for p in runs_dir.iterdir() if p.is_dir()}

    cmd = [
        "uv",
        "run",
        "quantlab",
        "backtest",
        "--config",
        "config/backtest.yaml",
        "--profile",
        "china_ashare",
    ]
    subprocess.run(cmd, check=True)

    after = {p.name for p in runs_dir.iterdir() if p.is_dir()}
    new_runs = sorted(after - before)
    assert new_runs, "expected a newly created run directory under runs/"

    run_dir = runs_dir / new_runs[-1]
    results_dir = run_dir / "results"

    metrics_path = results_dir / "metrics.json"
    equity_path = results_dir / "equity_curve.parquet"
    weights_path = results_dir / "weights.parquet"

    assert metrics_path.exists(), f"missing {metrics_path}"
    assert equity_path.exists(), f"missing {equity_path}"
    assert weights_path.exists(), f"missing {weights_path}"

    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

    # Metrics schema v1.0
    for section in ["meta", "config", "performance", "risk", "trade", "benchmark"]:
        assert section in metrics, f"missing metrics section: {section}"

    assert isinstance(metrics["meta"].get("created_at"), str)
    assert "T" in metrics["meta"]["created_at"], "created_at should be ISO format"
    assert metrics["meta"].get("schema_version") == "1.0"
    assert "git_commit" in metrics["meta"]

    for key in ["symbols", "start", "end", "initial_cash", "rebalance", "fee", "slippage", "strategy", "strategy_params"]:
        assert key in metrics["config"], f"missing config key: {key}"

    for key in ["total_return", "max_drawdown", "volatility"]:
        value = metrics["performance"].get(key)
        assert value is None or isinstance(value, float), f"{key} must be decimal float or null"
